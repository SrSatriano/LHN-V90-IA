# AIMixin auto-extracted
import asyncio
import gc
import logging
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime

import numpy as np
import pandas as pd
from bybit_helpers import (fetch_klines_historical_bulk, fetch_klines_since_ms,
                           get_kline_throttled, get_mark_price_and_funding,
                           get_open_interest_now, map_interval,
                           normalize_klines_result)
from config import *
from order_execution_gate import validate_linear_open_order
from services.risk_limits import MAX_OPERACOES_SIMULTANEAS, obter_limites_risco

try:
    from services.binance_oracle import get_binance_lead_signal, get_binance_oracle_top
except ImportError:

    def get_binance_lead_signal():  # type: ignore
        return 0

    def get_binance_oracle_top():  # type: ignore
        return (0.0, 0.0, 0.0)


try:
    from services.onchain_tracker import get_onchain_whale_alert_level
except ImportError:

    def get_onchain_whale_alert_level():  # type: ignore
        return 0


try:
    from pybit.exceptions import FailedRequestError
except ImportError:
    FailedRequestError = Exception  # type: ignore

try:
    import tensorflow.keras.backend as K
    import tensorflow.keras.saving as saving
    from tensorflow.keras import layers, models

    TENSORFLOW_ERROR = None
except Exception as e:
    import traceback

    TENSORFLOW_ERROR = f"{type(e).__name__}: {str(e)}"
    K = None  # type: ignore
    models = None
    layers = None
    saving = None

if saving:

    @saving.register_keras_serializable(package="Custom", name="reduce_sum_layer")
    def reduce_sum_layer(x):
        return K.sum(x, axis=1)

    @saving.register_keras_serializable(package="Custom", name="custom_pain_loss")
    def custom_pain_loss(y_true, y_pred):
        """Aprendizado pela dor (V90.1): BCE ponderado — penalização forte em erros grandes (SL / direção errada)."""
        import tensorflow as tf

        bce = K.binary_crossentropy(y_true, y_pred)
        pain_multiplier = 1.0 + tf.abs(y_true - y_pred) * 28.0
        return K.mean(bce * pain_multiplier)

    from tensorflow.keras.losses import Loss
    @saving.register_keras_serializable(package="Custom", name="InstitutionalAsymmetricLoss")
    class InstitutionalAsymmetricLoss(Loss):
        def __init__(self, threshold=0.05, penalty_factor=5.0, name="institutional_asymmetric_loss", **kwargs):
            super().__init__(name=name, **kwargs)
            self.threshold = threshold
            self.penalty_factor = penalty_factor

        def call(self, y_true, y_pred):
            import tensorflow as tf
            y_true = tf.cast(y_true, tf.float32)
            y_pred = tf.cast(y_pred, tf.float32)
            error = tf.abs(y_true - y_pred)
            
            loss = tf.where(
                error > self.threshold,
                error * self.penalty_factor,
                error
            )
            return tf.reduce_mean(loss)
            
        def get_config(self):
            config = super().get_config()
            config.update({"threshold": self.threshold, "penalty_factor": self.penalty_factor})
            return config

else:

    def reduce_sum_layer(x):
        return x

    def custom_pain_loss(y_true, y_pred):
        return 0.0


def keras_custom_objects():
    """Objetos exigidos ao carregar .keras (Lambda reduce_sum + loss customizada)."""
    return {
        "reduce_sum_layer": reduce_sum_layer, 
        "custom_pain_loss": custom_pain_loss,
        "InstitutionalAsymmetricLoss": InstitutionalAsymmetricLoss if K is not None else None
    }


logger = logging.getLogger(__name__)
_AI_BYBIT_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,15}USDT$")
_AI_BYBIT_SYMBOL_DENY = frozenset({"4USDT", "1USDT"})


def _is_valid_ai_market_symbol(symbol: str) -> bool:
    s = str(symbol or "").strip().upper()
    if not s or not _AI_BYBIT_SYMBOL_RE.match(s):
        return False
    if s in _AI_BYBIT_SYMBOL_DENY:
        return False
    return True


def _sanitize_ai_market_symbols(symbols) -> list[str]:
    out: list[str] = []
    seen = set()
    for symbol in symbols or []:
        s = str(symbol or "").strip().upper()
        if not _is_valid_ai_market_symbol(s):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _is_expected_market_data_error(exc: BaseException) -> bool:
    low = str(exc).lower()
    return any(
        token in low
        for token in (
            "getaddrinfo failed",
            "failed to resolve",
            "name resolution",
            "too many visits",
            "rate limit",
            "read timed out",
            "timeout",
            "connectionerror",
            "502",
            "503",
            "504",
        )
    )


def _apply_certeza_stretch(prob_pct: float, stretch: float) -> float:
    """Afasta a probabilidade de 50% (stretch>1 = certeza mais 'decisiva' na UI e nos filtros)."""
    try:
        s = float(stretch)
    except (TypeError, ValueError):
        s = 1.0
    if s <= 0 or abs(s - 1.0) < 1e-9:
        return max(0.0, min(100.0, float(prob_pct)))
    out = 50.0 + (float(prob_pct) - 50.0) * s
    return max(0.0, min(100.0, out))


def _bollinger_bands_last(close_series, period: int = 20, num_std: float = 2.0):
    """Última média, banda superior e inferior (Bollinger) para TP dinâmico."""
    try:
        mid = close_series.rolling(period).mean().iloc[-1]
        sd = close_series.rolling(period).std().iloc[-1]
        upper = float(mid + num_std * sd)
        lower = float(mid - num_std * sd)
        return float(mid), upper, lower
    except Exception:
        return None, None, None


if TENSORFLOW_ERROR is None:
    try:
        import tensorflow as tf

        # Modo implacável (Xeon / muitos núcleos): threads agressivos; override via env.
        _cpu = max(1, int(os.cpu_count() or 18))
        _tf_intra = int(os.environ.get("LHN_TF_INTRA_OP_THREADS", "18"))
        _tf_inter = int(os.environ.get("LHN_TF_INTER_OP_THREADS", "18"))
        _tf_intra = max(1, min(_tf_intra, _cpu))
        _tf_inter = max(1, min(_tf_inter, _cpu))
        tf.config.threading.set_intra_op_parallelism_threads(_tf_intra)
        tf.config.threading.set_inter_op_parallelism_threads(_tf_inter)

        for _gpu in tf.config.experimental.list_physical_devices("GPU"):
            try:
                tf.config.experimental.set_memory_growth(_gpu, True)
            except Exception:
                pass
    except Exception:
        pass


def _lhn_early_stopping_callbacks(patience: int, has_validation: bool):
    if TENSORFLOW_ERROR:
        return []
    try:
        from tensorflow.keras.callbacks import EarlyStopping
    except Exception:
        return []
    p = max(2, int(patience))
    if has_validation:
        return [
            EarlyStopping(
                monitor="val_loss",
                patience=p,
                restore_best_weights=True,
                verbose=0,
                min_delta=1e-6,
            )
        ]
    return [
        EarlyStopping(
            monitor="loss",
            patience=p,
            restore_best_weights=True,
            verbose=0,
            min_delta=1e-7,
        )
    ]

if TENSORFLOW_ERROR is None:
    from tensorflow.keras.utils import Sequence as _KerasSequence

    class _LHNNeuralBatchSequence(_KerasSequence):
        """Gera lotes (X, y) on-the-fly para reduzir pico de RAM no fit."""

        def __init__(self, x, y, batch_size=1024):
            _w = max(4, min(18, int(os.cpu_count() or 8)))
            super().__init__(workers=_w, use_multiprocessing=False)
            self.x = np.asarray(x, dtype=np.float32)
            self.y = np.asarray(y, dtype=np.float32)
            self.batch_size = max(1, int(batch_size))

        def __len__(self):
            return max(1, (len(self.x) + self.batch_size - 1) // self.batch_size)

        def __getitem__(self, idx):
            a = idx * self.batch_size
            b = min(a + self.batch_size, len(self.x))
            return self.x[a:b], self.y[a:b]

else:
    _LHNNeuralBatchSequence = None  # type: ignore[misc, assignment]


class AIMixin:
    _datalake_write_lock = threading.Lock()  # [M4] Lock para Data Lake

    def _neural_fit_epochs(self) -> int:
        return max(50, int(self.cfg.get("neural_fit_epochs", 50) or 50))

    def _neural_batch_size(self, n_samples: int) -> int:
        cap = int(self.cfg.get("neural_fit_batch_size_cap", 512) or 512)
        floor = int(self.cfg.get("neural_fit_batch_size_floor", 64) or 64)
        n = max(1, int(n_samples))
        return max(floor, min(cap, n))

    def _neural_fit_callbacks(self, has_validation: bool):
        return _lhn_early_stopping_callbacks(
            int(self.cfg.get("neural_early_stop_patience", 8) or 8),
            has_validation,
        )

    def _coerce_replay_feature_vector(self, values, dim_exp: int):
        """Normaliza um vetor legado para a dimensionalidade neural atual."""
        try:
            vec = np.asarray(values, dtype=np.float32).reshape(-1)
        except (TypeError, ValueError):
            return None
        if vec.size <= 0:
            return None
        vec = np.nan_to_num(vec, nan=0.0, posinf=0.0, neginf=0.0)
        dim_exp = int(dim_exp)
        if vec.size == dim_exp:
            return vec
        if vec.size > dim_exp:
            return vec[:dim_exp]
        padded = np.zeros(dim_exp, dtype=np.float32)
        padded[: vec.size] = vec
        return padded

    def _rehydrate_replay_state(self, state_raw, dim_exp: int, seq_len: int = 10):
        """
        Reidrata estados legados do replay/lake.

        O lake antigo salvava vetores 10D; o modelo atual exige janelas
        (seq_len x dim_exp). Preservamos o que existe e preenchemos o restante
        com neutros para não amputar o continuous learn.
        """
        if isinstance(state_raw, bytes):
            state_raw = state_raw.decode("utf-8", errors="ignore")
        if isinstance(state_raw, str):
            try:
                import json

                state_raw = json.loads(state_raw)
            except Exception:
                return None
        try:
            arr = np.asarray(state_raw, dtype=np.float32)
        except (TypeError, ValueError):
            return None
        if arr.ndim == 0 or arr.size <= 0:
            return None
        if arr.ndim == 1:
            vec = self._coerce_replay_feature_vector(arr, dim_exp)
            if vec is None:
                return None
            return np.repeat(vec.reshape(1, -1), int(seq_len), axis=0).tolist()

        arr = arr.reshape((-1, arr.shape[-1]))
        rows = []
        for row in arr:
            vec = self._coerce_replay_feature_vector(row, dim_exp)
            if vec is not None:
                rows.append(vec)
        if not rows:
            return None
        seq_len = int(seq_len)
        if len(rows) >= seq_len:
            rows = rows[-seq_len:]
        else:
            pad_row = rows[0]
            rows = [pad_row.copy() for _ in range(seq_len - len(rows))] + rows
        return np.stack(rows, axis=0).astype(np.float32).tolist()

    def __init__(self):
        self.ultima_vela_t = {}
        self.ia_cache_probs = {}
        self.temporal_windows = {}
        self.prob_ia = 50.0
        self._feature_executor = ThreadPoolExecutor(max_workers=15)
        self._train_executor = ThreadPoolExecutor(max_workers=15)
        self._market_filter_cache = {}
        self._open_interest_prev = {}
        self._replay_maintenance_ts = 0.0
        self._last_incremental_replay_ts = 0.0
        self._market_filter_rest_backoff_until = 0.0
        self._market_filter_last_warn_ts = 0.0
        self._training_task_running = False
        self._training_task_lock = threading.Lock()
        self._ativo_em_transicao = set()
        self._ativo_transicao_lock = threading.Lock()
        self._ultima_varredura_ts = None
        self._ultima_pulse_neural_ts = None
        self._analise_generation = 0
        self._analise_restart_lock = threading.Lock()
        self._last_watchdog_neural_restart_ts = 0.0
        self._silent_neural_runner_active = False
        self._silent_neural_refresh = False
        # Treino SNIPER só após primeiro tick de preço no WS (olhos abertos)
        self._ws_price_operational_ev = threading.Event()

    def _aguardar_feed_precos_para_treino(self):
        """Bloqueia o disparo do treino até o túnel de preços (tickers) confirmar operação."""
        if not self.cfg.get("use_ws", True):
            return
        ev = getattr(self, "_ws_price_operational_ev", None)
        if ev is None or ev.is_set():
            return
        self.log_msg(
            "⏳ Treinamento SNIPER aguardando túnel de preços (WebSocket) operante..."
        )
        ok = ev.wait(timeout=180.0)
        if not ok:
            self.log_msg(
                "⚠️ Timeout (180s) aguardando WS de preços — iniciando treino mesmo assim."
            )

    def _compute_dynamic_sl_tp_rr(
        self,
        preco: float,
        atr: float,
        sinal: str,
        df,
        cfg=None,
    ):
        """
        Alvo dinâmico (ATR + Bollinger): distância SL = max(% duro, ATR×mult).
        TP garante R:R mínimo rr_ratio_min (default 5) até rr_ratio_max (10).
        """
        cfg = cfg if cfg is not None else getattr(self, "cfg", {})
        hard_sl_pct = float(cfg.get("hard_sl_pct", 0.015) or 0.015)
        atr_mult_sl = float(cfg.get("atr_sl_distance_mult", 1.35) or 1.35)
        rr_min = float(cfg.get("rr_ratio_min", 5.0) or 5.0)
        rr_max = float(cfg.get("rr_ratio_max", 10.0) or 10.0)
        if rr_max < rr_min:
            rr_max = rr_min
        atr_f = max(float(atr) if atr is not None else 0.0, preco * 0.0008)
        sl_dist = max(preco * hard_sl_pct, atr_f * atr_mult_sl)
        atr_pct = atr_f / preco if preco > 0 else 0.01
        t = min(1.0, math.sqrt(max(0.0, atr_pct / 0.025)))
        rr = rr_min + (rr_max - rr_min) * t
        tp_dist = sl_dist * rr
        mid_u = upper = lower = None
        if df is not None and len(getattr(df, "index", [])) >= 22:
            mid, upper, lower = _bollinger_bands_last(df["c"])
        if sinal == "LONG":
            sl = preco - sl_dist
            if upper is not None and upper > preco:
                tp_dist = max(tp_dist, min(upper - preco, (upper - (mid or preco)) * 1.15))
            tp = preco + tp_dist
        else:
            sl = preco + sl_dist
            if lower is not None and lower < preco:
                tp_dist = max(tp_dist, min(preco - lower, ((mid or preco) - lower) * 1.15))
            tp = preco - tp_dist
        return {
            "sl": sl,
            "tp": tp,
            "sl_dist": sl_dist,
            "rr": rr,
            "tp_dist": tp_dist,
        }

    def _sniper_volatility_gate_accept(
        self, res: dict, sinal: str, p: float, sl_dist: float, rr_target: float
    ) -> bool:
        """
        Filtro Sniper: mercados mornos/laterais abortam o sinal sem log.
        Exige ADX alto, volume relativo e ATR% mínimo; o alvo projetado (1:RR) deve caber no range recente.
        """
        cfg = getattr(self, "cfg", {})
        if getattr(self, "modo_lateral_macro_liberado", False) and cfg.get(
            "sniper_relax_gate_in_lateral_macro", True
        ):
            return True
        adx = float(res.get("adx_val", 0.0) or 0.0)
        vol_ratio = float(res.get("vol_ratio", 100.0) or 100.0)
        atr = float(res.get("atr_val", p * 0.01) or p * 0.01)
        # Fronteira unificada com adx_regime_minimo / REGIME_ADX_MIN (config)
        adx_min = float(cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN)
        vol_min = float(cfg.get("sniper_vol_ratio_min", 112.0) or 112.0)
        atr_pct_min = float(cfg.get("sniper_atr_pct_min", 0.0042) or 0.0042)
        if adx < adx_min:
            return False
        if vol_ratio < vol_min:
            return False
        if p > 0 and (atr / p) < atr_pct_min:
            return False
        if not cfg.get("sniper_require_chart_room", True):
            return True
        df = res.get("df")
        if df is None or len(df) < 20:
            return False
        recent = df.iloc[-20:]
        hi = float(recent["h"].max())
        lo = float(recent["l"].min())
        need = (sl_dist * rr_target) / p if p > 0 else 1.0
        if sinal == "LONG":
            room = (hi - p) / p if p > 0 else 0.0
            if room < need * 0.82:
                return False
        else:
            room = (p - lo) / p if p > 0 else 0.0
            if room < need * 0.82:
                return False
        return True

    def _binance_futures_klines_safe(self, client, **kwargs):
        """Compat: cliente Bybit (pybit) — intervalos 15m, 1h etc. como string Bybit."""
        try:
            import requests

            symbol = kwargs.get("symbol")
            interval = kwargs.get("interval") or "15m"
            limit = int(kwargs.get("limit") or 200)
            iv = map_interval(interval) if isinstance(interval, str) else "15"
            res = get_kline_throttled(
                client,
                category="linear",
                symbol=symbol,
                interval=iv,
                limit=min(limit, 1000),
            )
            rows = normalize_klines_result(res)
            return rows if rows else None
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
            FailedRequestError,
            Exception,
        ):
            return None

    def ligar_cerebro_ia(self):
        if TENSORFLOW_ERROR:
            self.erro_msg(f"⛔ FALHA AO CARREGAR TENSORFLOW: {TENSORFLOW_ERROR}")
            self.log_msg("⚠️ O TensorFlow exige Python 3.11 ou 3.12.")
            self.log_msg(
                "💡 SOLUÇÃO: Delete a pasta 'venv' e execute SETUP_E_INICIAR.bat para instalar Python 3.11 automaticamente."
            )
            self.log_msg(
                "⚡ O bot continuará operando sem IA (apenas WebSocket e OrderBook)."
            )
            return

        if K is not None:
            try:
                K.clear_session()
            except Exception:
                pass

        wr = (
            getattr(self, "workspace_raiz", None)
            or (getattr(self, "cfg", None) or {}).get("workspace_root")
            or (getattr(self, "cfg", None) or {}).get("workspace_raiz")
        )
        if wr:
            wr = str(wr).strip()
            if wr and os.path.isdir(wr):
                self.workspace_raiz = os.path.abspath(wr)
                if hasattr(self, "inject_cfg_workspace_paths"):
                    self.inject_cfg_workspace_paths()
        if not getattr(self, "workspace_raiz", None):
            self.erro_msg("Atenção: Selecione o Workspace primeiro.")
            return

        n_hydr_replay = 0
        n_punit_tbl = 0
        try:
            if hasattr(self, "bootstrap_memoria_persistida_sqlite"):
                _, n_punit_tbl, n_hydr_replay = self.bootstrap_memoria_persistida_sqlite()
        except Exception:
            logger.exception("bootstrap_memoria_persistida_sqlite")
            n_punit_tbl = n_hydr_replay = 0

        modelos_dir = os.path.join(self.workspace_raiz, "modelos")
        os.makedirs(modelos_dir, exist_ok=True)
        import shutil

        self.arq_sniper_titular = os.path.join(modelos_dir, "SNIPER_TITULAR.keras")
        self.arq_sniper_reserva = os.path.join(modelos_dir, "SNIPER_RESERVA.keras")
        self.arq_lateral_titular = os.path.join(modelos_dir, "LATERAL_TITULAR.keras")
        self.arq_lateral_reserva = os.path.join(modelos_dir, "LATERAL_RESERVA.keras")
        self.arquivo_cerebro_sniper = self.arq_sniper_titular
        self.arquivo_cerebro_lateral = self.arq_lateral_titular
        self.arquivo_cerebro = self.arq_sniper_titular

        expected_shape = self._neural_feature_dim()
        _leg_sn = os.path.join(modelos_dir, "LHN_IA_DEEP_QUANT_V90 FINAL.keras")
        _leg_lat = os.path.join(modelos_dir, "cerebro_lateral_v90.keras")
        if os.path.isfile(_leg_sn) and not os.path.isfile(self.arq_sniper_titular):
            try:
                shutil.copy2(_leg_sn, self.arq_sniper_titular)
            except OSError:
                pass
        if os.path.isfile(_leg_lat) and not os.path.isfile(self.arq_lateral_titular):
            try:
                shutil.copy2(_leg_lat, self.arq_lateral_titular)
            except OSError:
                pass

        def _carregar_ou_forjar(caminho, mode="sigmoid"):
            if os.path.exists(caminho):
                try:
                    m = models.load_model(
                        caminho,
                        compile=False,
                        custom_objects=keras_custom_objects(),
                    )
                    if m.input_shape[-1] != expected_shape:
                        try:
                            os.remove(caminho)
                        except OSError:
                            pass
                        raise ValueError("arch_mismatch")
                    self._recompile_loaded_gladiador(m, mode)
                    return m
                except Exception:
                    self.log_msg(f"⚠️ Forjando novo modelo: {os.path.basename(caminho)}")
            m_novo = self.construir_rede_neural(mode)
            try:
                m_novo.save(caminho)
            except Exception:
                pass
            return m_novo

        if not (os.path.isfile(self.arq_sniper_titular) or os.path.isfile(_leg_sn)):
            self.log_msg(
                "⚠️ Cérebro não encontrado. Iniciando forja automática da IA (Treinamento)..."
            )
            modo_lat0 = self._lateral_neural_mode()
            self.model_sniper_titular = self.construir_rede_neural()
            self.model_sniper_reserva = self.construir_rede_neural()
            self.model_lateral_titular = self.construir_rede_neural(modo_lat0)
            self.model_lateral_reserva = self.construir_rede_neural(modo_lat0)
            self.model_sniper = self.model_sniper_titular
            self.model_lateral = self.model_lateral_titular
            self.model = self.model_sniper
            self.ia_treinada = False
            self.treinamento_concluido = False
            _nh = len(getattr(self, "historico_operacoes", []) or [])
            self.log_msg(
                f"[🧠 MEMÓRIA] Carregando {_nh} operações passadas; modelos .keras ausentes — forja neural pesada em background."
            )
            if n_hydr_replay or n_punit_tbl:
                self.log_msg(
                    f"[🧠 MEMÓRIA] Persistência SQLite: punit_memory={n_punit_tbl} registros, +{n_hydr_replay} reidratações no replay_buffer."
                )
            self.forjar_ia_sniper_nexus()
            return

        if os.path.isfile(self.arq_sniper_titular) and not os.path.isfile(
            self.arq_sniper_reserva
        ):
            try:
                shutil.copy2(self.arq_sniper_titular, self.arq_sniper_reserva)
            except OSError:
                pass
        if os.path.isfile(self.arq_lateral_titular) and not os.path.isfile(
            self.arq_lateral_reserva
        ):
            try:
                shutil.copy2(self.arq_lateral_titular, self.arq_lateral_reserva)
            except OSError:
                pass

        modo_lat = self._lateral_neural_mode()
        self.model_sniper_titular = _carregar_ou_forjar(
            self.arq_sniper_titular, "sigmoid"
        )
        self.model_sniper_reserva = _carregar_ou_forjar(
            self.arq_sniper_reserva, "sigmoid"
        )
        self.model_lateral_titular = _carregar_ou_forjar(
            self.arq_lateral_titular, modo_lat
        )
        self.model_lateral_reserva = _carregar_ou_forjar(
            self.arq_lateral_reserva, modo_lat
        )
        self.model_sniper = self.model_sniper_titular
        self.model_lateral = self.model_lateral_titular
        self.model = self.model_sniper
        self.ia_treinada = True
        self.treinamento_concluido = True
        if hasattr(self, "_limpar_acumuladores_virtuais_pos_forja"):
            self._limpar_acumuladores_virtuais_pos_forja()
        self.log_msg(
            f"🧠 Arena: 4 gladiadores carregados ({expected_shape}D). Titular+Reserva Sniper/Lateral."
        )
        _nh = len(getattr(self, "historico_operacoes", []) or [])
        self.log_msg(
            f"[🧠 MEMÓRIA] Carregando {_nh} operações passadas e modelos neurais persistidos. Bot pronto para execução."
        )
        if n_hydr_replay or n_punit_tbl:
            self.log_msg(
                f"[🧠 MEMÓRIA] Persistência: punit_memory={n_punit_tbl} registros, +{n_hydr_replay} reidratações no replay_buffer."
            )
        self._schedule_silent_neural_refresh()

        if not getattr(self, "_loop_analise_started", False):
            self._loop_analise_started = True
            self.submit_background_task(self.loop_analise_neural)

    def _schedule_silent_neural_refresh(self):
        """Replay + memória recente + (opcional) treinar_ia após atraso — não bloqueia o boot nem o uso dos .keras carregados."""
        if getattr(self, "_silent_neural_runner_active", False):
            return
        self._silent_neural_runner_active = True

        def _runner():
            try:
                self._silent_neural_refresh = True
                time.sleep(2.0)
                if hasattr(self, "_replay_buffer_experience_reservas"):
                    self._replay_buffer_experience_reservas()
                    if hasattr(self, "_limpar_acumuladores_virtuais_pos_forja"):
                        self._limpar_acumuladores_virtuais_pos_forja()
                if hasattr(self, "forjar_memoria_recente"):
                    self.forjar_memoria_recente()
                    if hasattr(self, "_limpar_acumuladores_virtuais_pos_forja"):
                        self._limpar_acumuladores_virtuais_pos_forja()
                _delay = float(self.cfg.get("silent_retrain_delay_sec", 90.0) or 90.0)
                _delay = max(0.0, _delay - 2.0)
                if self.cfg.get("silent_background_treinar_ia", True) and _delay > 0:
                    time.sleep(_delay)
                if self.cfg.get("silent_background_treinar_ia", True) and getattr(
                    self, "ia_treinada", False
                ):
                    self.forjar_ia_sniper_nexus()
            except Exception:
                logger.exception("silent_neural_refresh_runner_failed")
            finally:
                self._silent_neural_refresh = False
                self._silent_neural_runner_active = False

        self.submit_background_task(_runner)

    def forjar_ia_sniper_nexus(self):
        """Agenda a forja da IA em background sem bloquear o loop de preços."""
        now = time.time()
        if now - float(getattr(self, "_last_auto_train_ts", 0.0) or 0.0) < 1800.0:
            return
        with self._ops_lock:
            abertas_ativas = sum(
                1
                for op in getattr(self, "operacoes_abertas", {}).values()
                if isinstance(op, dict) and not op.get("_pending")
            )
        if abertas_ativas > 0:
            return
        self._aguardar_feed_precos_para_treino()
        with self._training_task_lock:
            if self._training_task_running:
                self.log_msg("⚠️ Forja Neural já em execução. Ignorando novo disparo.")
                return
            self._training_task_running = True
            self._last_auto_train_ts = now
            self.treinamento_concluido = False

        def _runner():
            try:
                self.treinar_ia()
            finally:
                with self._training_task_lock:
                    self._training_task_running = False

        try:
            if hasattr(self, "loop") and self.loop and self.loop.is_running():

                async def _async_train():
                    try:
                        if hasattr(asyncio, "to_thread"):
                            await asyncio.to_thread(self.treinar_ia)
                        else:
                            await asyncio.get_running_loop().run_in_executor(
                                self._train_executor, self.treinar_ia
                            )
                    finally:
                        with self._training_task_lock:
                            self._training_task_running = False

                asyncio.run_coroutine_threadsafe(_async_train(), self.loop)
            else:
                self.submit_background_task(_runner)
        except Exception:
            logger.exception(
                "async_training_schedule_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                "GLOBAL",
                {},
            )
            with self._training_task_lock:
                self._training_task_running = False

    def _inferencia_bayesiana(self, model, X_batch, n_iter=20, threshold_incerteza=0.15):
        """Fase 3: Calibração Bayesiana de Incerteza (MC Dropout dinâmico)."""
        import numpy as np
        import tensorflow as tf

        n_iter = max(1, int(n_iter or 1))
        batch_size = int(len(X_batch))
        X_clonado = np.repeat(X_batch, n_iter, axis=0)
        preds = model(X_clonado, training=True)
        preds_reshape = tf.reshape(preds, (batch_size, n_iter, -1)).numpy()

        media_predicoes = np.mean(preds_reshape, axis=1)
        desvio_padrao = np.std(preds_reshape, axis=1)

        vetado = bool(desvio_padrao[0][0] > threshold_incerteza)
        return {
            "predicao": float(media_predicoes[0][0]),
            "incerteza": float(desvio_padrao[0][0]),
            "vetado_por_varianca": vetado
        }

    def _neural_feature_dim(self) -> int:
        """28 base | +6 microestrutura (sem MTF) | 60 MTF | 67 MTF+lhn_indicators (V90.65D+2)."""
        if self.cfg.get("use_mtf_neural", False) and self.cfg.get(
            "use_65d_layout", False
        ):
            return 67
        if self.cfg.get("use_mtf_neural", False):
            return 60
        if self.cfg.get("use_institutional_microstructure", False):
            return 34
        return 28

    def _wants_lhn_65d_pack(self) -> bool:
        """Pacote Tabajara/AlphaTrend/MACD4/pin/SR/MFI só com MTF + layout 65D."""
        return bool(
            self.cfg.get("use_mtf_neural", False)
            and self.cfg.get("use_65d_layout", False)
        )

    def get_neural_lake_path(self):
        """Parquet do Experience Replay (últimos ticks gravados para treino contínuo)."""
        wr = getattr(self, "workspace_raiz", None)
        if not wr:
            return ""
        return os.path.join(wr, "lhn_datalake", "neural_training_lake.parquet")

    def construir_rede_neural(self, output_mode: str = "sigmoid"):
        """output_mode: sigmoid+BCE (legado) | tanh+hinge (rotulo reforço lateral, -1/+1)."""
        shape_dim = self._neural_feature_dim()
        out_act = "tanh" if output_mode == "tanh" else "sigmoid"
        # --- FASE 6: ARQUITETURA LSTM + ATTENTION (PRODUTO INTERNO TENSORIAL) ---
        inputs = layers.Input(shape=(10, shape_dim))

        # Bloco LSTM e Transformer
        if self.cfg.get("use_transformer", False):
            # --- [M3] TRANSFORMER ARCHITECTURE ---
            x = layers.Dense(128)(inputs)
            for _ in range(2):
                attn_out = layers.MultiHeadAttention(
                    num_heads=4, key_dim=32, dropout=0.2
                )(x, x)
                attn_out = layers.Dropout(0.2)(attn_out)
                x = layers.LayerNormalization()(x + attn_out)
                ff = layers.Dense(256, activation="relu")(x)
                ff = layers.Dense(128)(ff)
                ff = layers.Dropout(0.2)(ff)
                x = layers.LayerNormalization()(x + ff)
            x = layers.GlobalAveragePooling1D()(x)
            x = layers.Dense(64, activation="relu")(x)
            x = layers.Dropout(0.3)(x)
            output = layers.Dense(1, activation=out_act)(x)
        else:
            # Bloco LSTM
            lstm_out = layers.LSTM(256, return_sequences=True)(inputs)
            lstm_out = layers.Dropout(0.3)(lstm_out)
            lstm_out2 = layers.LSTM(128, return_sequences=True)(lstm_out)

            # Camada de Atenção Simples (Global Context Attention)
            attention = layers.Dense(1, activation="tanh")(lstm_out2)
            attention = layers.Flatten()(attention)
            attention = layers.Activation("softmax")(attention)
            attention = layers.RepeatVector(128)(attention)
            attention = layers.Permute([2, 1])(attention)

            sentinel_context = layers.Multiply()([lstm_out2, attention])

            # Chamada da função global decorada
            sentinel_context = layers.Lambda(reduce_sum_layer, output_shape=(128,))(
                sentinel_context
            )

            # Cabeça de Decisão
            x = layers.BatchNormalization()(sentinel_context)
            x = layers.Dense(64, activation="relu")(x)
            x = layers.Dropout(0.3)(x)
            output = layers.Dense(1, activation=out_act)(x)

        model = models.Model(inputs=inputs, outputs=output)

        from tensorflow.keras.optimizers import Adam

        opt = Adam(learning_rate=0.001)
        if output_mode == "tanh":
            model.compile(optimizer=opt, loss="hinge", metrics=["accuracy"])
        else:
            model.compile(optimizer=opt, loss=custom_pain_loss, metrics=["accuracy"])
        return model

    def _lateral_neural_mode(self) -> str:
        """sigmoid (legado) | tanh (reforço ±1 + hinge no treino lateral)."""
        return (
            "tanh" if self.cfg.get("use_reinforcement_lateral_training") else "sigmoid"
        )

    def _recompile_loaded_gladiador(self, m, output_mode: str = "sigmoid"):
        """Após load_model(..., compile=False): Adam limpo + mesma loss do treino (evita erro de otimizador serializado)."""
        if m is None or models is None:
            return
        from tensorflow.keras.optimizers import Adam

        opt = Adam(learning_rate=0.001)
        if output_mode == "tanh":
            m.compile(optimizer=opt, loss="hinge", metrics=["accuracy"])
        else:
            m.compile(optimizer=opt, loss=InstitutionalAsymmetricLoss(), metrics=["accuracy"])

    def analisar_tendencia_neural(self, ativo, dados_ohlc):
        """Validação mínima (10 timesteps) antes de qualquer predição sobre série OHLC."""
        if dados_ohlc is None:
            return None
        try:
            if isinstance(dados_ohlc, np.ndarray):
                if dados_ohlc.shape[0] < 10:
                    return None
            else:
                if len(dados_ohlc) < 10:
                    return None
        except (TypeError, ValueError):
            return None
        return None

    def _lateral_smart_money_adjust(self, ativo: str, sinal: str, client):
        """Regime macro lateral: bloqueia se OI não sustenta; bônus de certeza se funding alinha ao viés."""
        if not getattr(self, "modo_lateral_macro_liberado", False):
            return True, 0.0
        if not self.cfg.get("use_lateral_smart_money_filters", True):
            return True, 0.0
        try:
            fm = self._coletar_filtros_mercado(ativo, client)
        except Exception:
            return True, 0.0
        oi_d = float(fm.get("oi_delta_pct", 0.0))
        min_oi = float(self.cfg.get("lateral_oi_min_delta_pct", 0.0))
        if oi_d <= min_oi:
            self.log_msg(
                f"[FILTRO OI] Rejeitado {ativo}: movimento sem combustível (ΔOI={oi_d:.4f}% vs mín {min_oi})."
            )
            return False, 0.0
        bonus = 0.0
        eps = float(self.cfg.get("lateral_funding_bias_eps", 0.0001))
        fr = float(fm.get("funding_rate", 0.0))
        bpts = float(self.cfg.get("lateral_funding_bias_certeza_pts", 3.0))
        if sinal == "SHORT" and fr > eps:
            bonus += bpts
        elif sinal == "LONG" and fr < -eps:
            bonus += bpts
        return True, bonus

    def _replay_loss_weight_mult(self) -> float:
        return float(self.cfg.get("replay_loss_weight_multiplier", 15.0))

    def _sample_weight_replay_reward(self, reward_val: float) -> float:
        """Peso de amostra: perdas pesam mais; ganhos reforçam um pouco (rentabilidade relativa ao sinal)."""
        lm = self._replay_loss_weight_mult()
        wm = float(self.cfg.get("replay_win_weight_boost", 1.35))
        rw = float(reward_val)
        if rw < 0:
            return lm * max(0.05, abs(rw))
        return wm * (1.0 + 0.4 * min(1.0, max(0.0, rw)))

    def _schedule_incremental_replay_fit(self) -> None:
        """Após fecho de trade com reward gravado: micro-fit nas reservas (debounced)."""
        if not self.cfg.get("incremental_replay_on_trade_close", True):
            return
        deb = float(self.cfg.get("incremental_replay_debounce_sec", 90))
        now = time.time()
        if now - float(getattr(self, "_last_incremental_replay_ts", 0) or 0) < deb:
            return
        self._last_incremental_replay_ts = now

        def _run():
            try:
                self._incremental_fit_reservas_from_replay_microbatch()
            except Exception:
                logger.exception(
                    "incremental_replay_fit_failed | ts=%s",
                    int(time.time() * 1000),
                )

        if hasattr(self, "submit_background_task"):
            self.submit_background_task(_run)
        else:
            _run()

    def _incremental_fit_reservas_from_replay_microbatch(self) -> None:
        """Últimas experiências com reward → fit curto só em SNIPER_RESERVA / LATERAL_RESERVA."""
        if TENSORFLOW_ERROR or not getattr(self, "ia_treinada", False):
            return
        min_n = int(self.cfg.get("incremental_replay_min_samples", 8))
        epochs = int(self.cfg.get("incremental_replay_epochs", 4))
        bs = int(self.cfg.get("incremental_replay_batch_size", 256))
        _sql_lim = max(32, min(100_000, int(self.cfg.get("incremental_replay_sql_limit", 12000) or 12000)))
        db_path = getattr(self, "arquivo_db_memoria", None)
        if not db_path or not os.path.isfile(db_path):
            return
        import json

        dim_exp = int(self._neural_feature_dim())
        modo_lat = self._lateral_neural_mode()

        def _read_micro(conn):
            return conn.execute(
                """
                SELECT agent_id, state, action, reward FROM replay_buffer
                WHERE state IS NOT NULL AND action IS NOT NULL AND reward IS NOT NULL
                  AND agent_id IN ('SNIPER_V90 FINAL', 'ARENA_SNIPER_RESERVA', 'ARENA_LATERAL_RESERVA')
                ORDER BY id DESC LIMIT ?
                """,
                (_sql_lim,),
            ).fetchall()

        try:
            rows = self._run_sqlite_with_retry(
                db_path,
                _read_micro,
                max_attempts=4,
                base_delay=0.1,
                timeout=8.0,
            )
        except Exception:
            return

        xs_sn, ys_sn, ws_sn = [], [], []
        xs_lat, ys_lat, ws_lat = [], [], []

        for agent_id, state_raw, action, reward in rows:
            try:
                state_arr = self._rehydrate_replay_state(state_raw, dim_exp)
                if state_arr is None:
                    continue
                rw = float(reward)
                w = self._sample_weight_replay_reward(rw)
                act_i = int(action) if action is not None else 0
                if agent_id in ("SNIPER_V90 FINAL", "ARENA_SNIPER_RESERVA"):
                    xs_sn.append(state_arr)
                    ys_sn.append(float(act_i))
                    ws_sn.append(w)
                elif agent_id == "ARENA_LATERAL_RESERVA":
                    xs_lat.append(state_arr)
                    if modo_lat == "tanh":
                        ys_lat.append(1.0 if act_i >= 1 else -1.0)
                    else:
                        ys_lat.append(float(act_i))
                    ws_lat.append(w)
            except Exception:
                continue

        m_sn_r = getattr(self, "model_sniper_reserva", None)
        m_lat_r = getattr(self, "model_lateral_reserva", None)

        if m_sn_r is not None and len(xs_sn) >= min_n:
            import numpy as np

            X = np.asarray(xs_sn, dtype=np.float32)
            y = np.asarray(ys_sn, dtype=np.float32)
            w_arr = np.asarray(ws_sn, dtype=np.float32)
            self._keras_fit(
                m_sn_r,
                X,
                y,
                sample_weight=w_arr,
                epochs=epochs,
                batch_size=min(max(1, bs), len(xs_sn)),
                callbacks=self._neural_fit_callbacks(False),
                verbose=0,
            )
            if getattr(self, "arq_sniper_reserva", None):
                m_sn_r.save(self.arq_sniper_reserva)
            self.log_msg(
                f"⚡ [REPLAY+] SNIPER_RESERVA micro-fit n={len(xs_sn)} ép={epochs} (pós-fecho)"
            )

        if m_lat_r is not None and len(xs_lat) >= min_n:
            import numpy as np

            X = np.asarray(xs_lat, dtype=np.float32)
            y = np.asarray(ys_lat, dtype=np.float32)
            w_arr = np.asarray(ws_lat, dtype=np.float32)
            self._keras_fit(
                m_lat_r,
                X,
                y,
                sample_weight=w_arr,
                epochs=epochs,
                batch_size=min(max(1, bs), len(xs_lat)),
                callbacks=self._neural_fit_callbacks(False),
                verbose=0,
            )
            if getattr(self, "arq_lateral_reserva", None):
                m_lat_r.save(self.arq_lateral_reserva)
            self.log_msg(
                f"⚡ [REPLAY+] LATERAL_RESERVA micro-fit n={len(xs_lat)} ép={epochs} (pós-fecho)"
            )
        gc.collect()

    def punir_e_retreinar_reserva(self, regime):
        """Retreino agressivo do modelo RESERVA com sample_weight nas experiências negativas (replay_buffer)."""
        if TENSORFLOW_ERROR:
            return
        modelo_alvo = (
            self.model_sniper_reserva
            if regime == "SNIPER"
            else self.model_lateral_reserva
        )
        if modelo_alvo is None:
            return
        modo = "sigmoid" if regime == "SNIPER" else self._lateral_neural_mode()
        agent_needle = (
            "ARENA_SNIPER_RESERVA" if regime == "SNIPER" else "ARENA_LATERAL_RESERVA"
        )
        self.log_msg(
            f"⚡ [PUNIÇÃO] Eletrochoque sináptico em {regime}_RESERVA ({agent_needle})..."
        )
        try:
            import json

            X_list, y_list, w_list = [], [], []
            db_path = getattr(self, "arquivo_db_memoria", None) or getattr(
                self, "db_path", None
            )
            if not db_path or not os.path.isfile(db_path):
                return
            _pun_lim = max(
                1,
                min(
                    500_000,
                    int(
                        self.cfg.get("punish_samples_limit", PUNISH_SAMPLES_LIMIT)
                        or PUNISH_SAMPLES_LIMIT
                    ),
                ),
            )

            def _read_punish(conn):
                return conn.execute(
                    """
                    SELECT ativo, state, action, reward FROM replay_buffer
                    WHERE reward IS NOT NULL AND reward < 0 AND agent_id = ?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (agent_needle, _pun_lim),
                ).fetchall()

            rows = self._run_sqlite_with_retry(
                db_path, _read_punish, max_attempts=6, base_delay=0.1, timeout=10.0
            )
            if rows:

                def _archive_punit(conn):
                    for ativo_pm, state_raw_pm, action_pm, reward_pm in rows:
                        st_pm = state_raw_pm
                        if isinstance(st_pm, bytes):
                            st_pm = st_pm.decode("utf-8", errors="ignore")
                        conn.execute(
                            """
                            INSERT INTO punit_memory (ativo, agent_id, state, action, reward, source)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(ativo_pm or ""),
                                agent_needle,
                                st_pm,
                                int(action_pm) if action_pm is not None else 0,
                                float(reward_pm),
                                "replay_negative",
                            ),
                        )
                    conn.commit()

                self.enqueue_deep_memory_write(
                    _archive_punit, max_attempts=6, base_delay=0.12, timeout=12.0
                )

            dim_exp = int(self._neural_feature_dim())
            for _ativo_row, state_raw, action, reward in rows:
                try:
                    state_arr = self._rehydrate_replay_state(state_raw, dim_exp)
                    if state_arr is None:
                        continue
                    X_list.append(np.array(state_arr, dtype=np.float32))
                    act_i = int(action) if action is not None else 0
                    if modo == "tanh":
                        y_list.append(1.0 if act_i >= 1 else -1.0)
                    else:
                        y_list.append(float(act_i))
                    w_list.append(abs(float(reward)) * self._replay_loss_weight_mult())
                except Exception:
                    continue
            if len(X_list) < 3:
                self.log_msg(
                    f"⚠️ [PUNIÇÃO] Poucas amostras negativas para {regime}_RESERVA — pulando fit."
                )
                return
            Xb = np.stack(X_list, axis=0)
            yb = np.array(y_list, dtype=np.float32)
            wb = np.array(w_list, dtype=np.float32)
            self._keras_fit(
                modelo_alvo,
                Xb,
                yb,
                sample_weight=wb,
                epochs=max(8, int(self.cfg.get("punish_reserva_epochs", 12) or 12)),
                batch_size=max(1, min(self._neural_batch_size(len(Xb)), len(Xb))),
                callbacks=self._neural_fit_callbacks(False),
                verbose=0,
            )
            try:
                from tensorflow.keras.layers import Dense

                _decay = float(self.cfg.get("punish_weight_decay_factor", 0.86) or 0.86)
                for layer in modelo_alvo.layers:
                    if isinstance(layer, Dense):
                        w = layer.get_weights()
                        if w:
                            layer.set_weights([wi * np.float32(_decay) for wi in w])
            except Exception:
                pass
            caminho = (
                self.arq_sniper_reserva
                if regime == "SNIPER"
                else self.arq_lateral_reserva
            )
            if caminho:
                modelo_alvo.save(caminho)
            gc.collect()
            self.log_msg(
                f"💀 [MUTAÇÃO] {regime}_RESERVA retreinado sob stress e salvo."
            )
        except Exception as e:
            self.erro_msg(f"Falha punição neural ({regime}): {e}")

    def _replay_buffer_experience_reservas(self):
        """Replay massivo do SQLite (arena reserva): limite em replay_buffer_sql_limit; épocas/batch via config + EarlyStopping."""
        if TENSORFLOW_ERROR or not getattr(self, "ia_treinada", False):
            return
        db_path = getattr(self, "arquivo_db_memoria", None)
        if not db_path or not os.path.isfile(db_path):
            return
        m_sn_r = getattr(self, "model_sniper_reserva", None)
        m_lat_r = getattr(self, "model_lateral_reserva", None)
        if m_sn_r is None and m_lat_r is None:
            return
        import json

        dim_exp = int(self._neural_feature_dim())
        modo_lat = self._lateral_neural_mode()

        _rb_lim = max(500, min(200_000, int(self.cfg.get("replay_buffer_sql_limit", 50000) or 50000)))

        def _read(conn):
            cols = [
                c[1]
                for c in conn.execute("PRAGMA table_info(replay_buffer)").fetchall()
            ]
            if "agent_id" not in cols:
                conn.execute(
                    "ALTER TABLE replay_buffer ADD COLUMN agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'"
                )
                conn.commit()
            rows = conn.execute(
                """
                SELECT agent_id, state, action, reward FROM replay_buffer
                WHERE state IS NOT NULL AND action IS NOT NULL
                  AND reward IS NOT NULL
                  AND agent_id IN ('ARENA_SNIPER_RESERVA', 'ARENA_LATERAL_RESERVA')
                ORDER BY id DESC LIMIT ?
                """,
                (_rb_lim,),
            ).fetchall()
            return rows

        try:
            rows = self._run_sqlite_with_retry(
                db_path,
                _read,
                max_attempts=6,
                base_delay=0.12,
                timeout=5.0,
            )
        except Exception as e:
            self.erro_msg(f"[XP_REPLAY] leitura SQLite: {e}")
            return

        xs_sn, ys_sn, ws_sn = [], [], []
        xs_lat, ys_lat, ws_lat = [], [], []

        for agent_id, state_raw, action, reward in rows:
            try:
                state_arr = self._rehydrate_replay_state(state_raw, dim_exp)
                if state_arr is None:
                    continue
                rw = float(reward)
                w = self._sample_weight_replay_reward(rw)
                act_i = int(action) if action is not None else 0
                if agent_id == "ARENA_SNIPER_RESERVA":
                    xs_sn.append(state_arr)
                    ys_sn.append(float(act_i))
                    ws_sn.append(w)
                elif agent_id == "ARENA_LATERAL_RESERVA":
                    xs_lat.append(state_arr)
                    if modo_lat == "tanh":
                        ys_lat.append(1.0 if act_i >= 1 else -1.0)
                    else:
                        ys_lat.append(float(act_i))
                    ws_lat.append(w)
            except Exception:
                continue

        _xpe = max(2, int(self.cfg.get("replay_buffer_fit_epochs", 8) or 8))
        try:
            if m_sn_r is not None and len(xs_sn) >= 8:
                X = np.asarray(xs_sn, dtype=np.float32)
                y = np.asarray(ys_sn, dtype=np.float32)
                w_arr = np.asarray(ws_sn, dtype=np.float32)
                _bs = self._neural_batch_size(len(xs_sn))

                if not getattr(self, "_silent_neural_refresh", False):
                    self.log_msg(f"🧬 [XP_REPLAY] Treinamento Contínuo: Ingerindo matriz de formato {X.shape} - 65D Ativo")
                    
                self._keras_fit(
                    m_sn_r,
                    X,
                    y,
                    sample_weight=w_arr,
                    epochs=_xpe,
                    batch_size=min(_bs, len(xs_sn)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                if getattr(self, "arq_sniper_reserva", None):
                    m_sn_r.save(self.arq_sniper_reserva)
                if not getattr(self, "_silent_neural_refresh", False):
                    self.log_msg(
                        f"🧬 [XP_REPLAY] SNIPER_RESERVA SQLite n={len(xs_sn)} ({_xpe}ép, b{_bs})"
                    )
            if m_lat_r is not None and len(xs_lat) >= 8:
                X = np.asarray(xs_lat, dtype=np.float32)
                y = np.asarray(ys_lat, dtype=np.float32)
                w_arr = np.asarray(ws_lat, dtype=np.float32)
                _bs = self._neural_batch_size(len(xs_lat))
                self._keras_fit(
                    m_lat_r,
                    X,
                    y,
                    sample_weight=w_arr,
                    epochs=_xpe,
                    batch_size=min(_bs, len(xs_lat)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                if getattr(self, "arq_lateral_reserva", None):
                    m_lat_r.save(self.arq_lateral_reserva)
                if not getattr(self, "_silent_neural_refresh", False):
                    self.log_msg(
                        f"🧬 [XP_REPLAY] LATERAL_RESERVA SQLite n={len(xs_lat)} ({_xpe}ép, b{_bs})"
                    )
        except Exception as e:
            self.erro_msg(f"[XP_REPLAY] fit/save: {e}")

    def _calcular_16d(self, df, include_lhn_pack: bool = False):
        df_feat = df.copy()
        delta = df_feat["c"].diff()
        gain = delta.where(delta > 0, 0).rolling(window=self.cfg["rsi_p"]).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=self.cfg["rsi_p"]).mean()
        rs = gain / loss
        df_feat["rsi"] = 100 - (100 / (1 + rs))
        df_feat["rsi"] = df_feat["rsi"].fillna(50)

        qtd = self.cfg["ema_count"]
        for j in range(qtd):
            df_feat[f"ema_{j}"] = (
                df_feat["c"].ewm(span=self.cfg["emas"][j], adjust=False).mean()
            )

        df_feat["macd"] = (
            df_feat["c"].ewm(span=self.cfg["macd_f"], adjust=False).mean()
            - df_feat["c"].ewm(span=self.cfg["macd_s"], adjust=False).mean()
        )

        tr = pd.concat(
            [
                df_feat["h"] - df_feat["l"],
                abs(df_feat["h"] - df_feat["c"].shift()),
                abs(df_feat["l"] - df_feat["c"].shift()),
            ],
            axis=1,
        ).max(axis=1)
        df_feat["atr"] = tr.rolling(14).mean()

        # [M1] ADX Real (Welles Wilder)
        _adx_p = self.cfg["adx_p"]
        _high_diff = df_feat["h"].diff()
        _low_diff = -df_feat["l"].diff()  # PrevLow - Low
        _plus_dm = _high_diff.where((_high_diff > _low_diff) & (_high_diff > 0), 0.0)
        _minus_dm = _low_diff.where((_low_diff > _high_diff) & (_low_diff > 0), 0.0)
        _atr_wilder = tr.ewm(alpha=1 / _adx_p, adjust=False).mean()
        _plus_di = (
            100
            * _plus_dm.ewm(alpha=1 / _adx_p, adjust=False).mean()
            / _atr_wilder.replace(0, 1)
        )
        _minus_di = (
            100
            * _minus_dm.ewm(alpha=1 / _adx_p, adjust=False).mean()
            / _atr_wilder.replace(0, 1)
        )
        _dx = 100 * abs(_plus_di - _minus_di) / (_plus_di + _minus_di).replace(0, 1)
        df_feat["adx"] = _dx.ewm(alpha=1 / _adx_p, adjust=False).mean()
        df_feat["adx"] = df_feat["adx"].fillna(20)

        df_feat["ema_36"] = df_feat["c"].ewm(span=36).mean()
        df_feat["ema_84"] = df_feat["c"].ewm(span=84).mean()
        df_feat["ema_144"] = df_feat["c"].ewm(span=144).mean()
        df_feat["ema_336"] = df_feat["c"].ewm(span=336).mean()

        low_min = df_feat["l"].rolling(self.cfg["stoch_k"]).min()
        high_max = df_feat["h"].rolling(self.cfg["stoch_k"]).max()
        df_feat["stoch"] = 100 * (
            (df_feat["c"] - low_min) / (high_max - low_min).replace(0, 1)
        )
        df_feat["stoch"] = df_feat["stoch"].fillna(50)

        vol_sma = df_feat["v"].rolling(self.cfg["vol_sma"]).mean()
        df_feat["vol_ratio"] = (df_feat["v"] / vol_sma.replace(0, 1)) * 100
        df_feat["vol_ratio"] = df_feat["vol_ratio"].fillna(100)

        df_feat["sell_vol"] = df_feat["v"] - df_feat["tb"]
        df_feat["order_flow_delta"] = (
            (df_feat["tb"] - df_feat["sell_vol"]) / df_feat["v"].replace(0, 1)
        ) * 100

        df_feat["financial_aggression"] = (
            df_feat["tq"] / df_feat["qv"].replace(0, 1)
        ) * 100

        df_feat["cvd"] = (df_feat["tb"] - df_feat["sell_vol"]).rolling(window=20).sum()
        df_feat["cvd"] = df_feat["cvd"].fillna(0)
        vol_sum_20 = df_feat["v"].rolling(window=20).sum()
        df_feat["cvd_norm"] = (df_feat["cvd"] / vol_sum_20.replace(0, 1)) * 100

        v_tp = df_feat["v"] * (df_feat["h"] + df_feat["l"] + df_feat["c"]) / 3
        # VWAP Rolante de 144 períodos para consistência entre Trenamento e Live
        vwap = v_tp.rolling(144).sum() / df_feat["v"].rolling(144).sum().replace(0, 1)
        df_feat["vwap_dist"] = ((df_feat["c"] - vwap) / vwap.replace(0, 1)) * 100

        df_feat["hour_norm"] = pd.to_datetime(df_feat["t"], unit="ms").dt.hour / 24.0
        df_feat["regime"] = (
            df_feat["atr"] / df_feat["c"].rolling(100).mean().replace(0, 1)
        ) * 100
        df_feat["regime"] = df_feat["regime"].fillna(0)

        # [INSTITUCIONAL V90] POC + compressão (Markov)
        df_feat["preco_tipico"] = (df_feat["h"] + df_feat["l"] + df_feat["c"]) / 3
        df_feat["vol_price"] = df_feat["v"] * df_feat["preco_tipico"]
        df_feat["poc_rolling"] = df_feat["vol_price"].rolling(50).sum() / df_feat[
            "v"
        ].rolling(50).sum().replace(0, 1)
        df_feat["dist_poc"] = (
            (df_feat["c"] - df_feat["poc_rolling"])
            / df_feat["poc_rolling"].replace(0, 1)
        ) * 100
        df_feat["dist_poc"] = df_feat["dist_poc"].fillna(0)
        df_feat["atr_variance"] = df_feat["atr"].rolling(20).var()
        df_feat["atr_variance"] = df_feat["atr_variance"].fillna(0)
        df_feat["markov_compression"] = (
            df_feat["atr"] / (df_feat["atr_variance"] + 1e-9)
        ) * 100
        # --- Microestrutura institucional (reserva / shadow HFT) — DNA 6D ---
        if self.cfg.get("use_institutional_microstructure", False):
            buy_pct = (
                (df_feat["tb"] / df_feat["v"].replace(0, np.nan)).clip(0, 1).fillna(0.5)
            )
            # VPIN multi-janela (velas 15m: m1 = barra atual; m5 ≈ blend 2/3 atual + 1/3 anterior)
            df_feat["vpin_m1"] = buy_pct
            df_feat["vpin_m5"] = 0.67 * buy_pct + 0.33 * buy_pct.shift(1).fillna(
                buy_pct
            )
            # CVD por polaridade: sign(close-open) * volume, acumulado 10 velas, normalizado
            sign_co = np.sign(df_feat["c"].values - df_feat["o"].values)
            vol_signed = pd.Series(sign_co, index=df_feat.index) * df_feat["v"]
            roll_sum = vol_signed.rolling(10, min_periods=1).sum()
            den10 = df_feat["v"].rolling(10, min_periods=1).sum().replace(0, np.nan)
            df_feat["cvd_polar10_norm"] = (roll_sum / den10).fillna(0).clip(-1, 1)
            # Desequilíbrio tipo L2: (agressão compra - venda) / volume ≈ (Bids - Asks) / (2*metade)
            df_feat["book_imb_approx"] = (
                df_feat["tb"] - df_feat["sell_vol"]
            ) / df_feat["v"].replace(0, 1)
            # Aceleração de fluxo (proxy OI quando não há série OI na kline)
            df_feat["oi_accel_proxy"] = (
                df_feat["v"].pct_change().rolling(5, min_periods=1).mean().fillna(0)
                * 100.0
            )
            df_feat["oi_funding_cross_hist"] = (
                df_feat["oi_accel_proxy"] * np.sign(df_feat["c"].diff().fillna(0.0))
            ).clip(-5, 5)
        if include_lhn_pack and self._wants_lhn_65d_pack():
            try:
                from lhn_indicators import (
                    alphatrend_features,
                    cm_macd_histogram_4color,
                    detect_pin_bar,
                    mfi_wilder,
                    sr_channel_position,
                    tabajara_categorical,
                )

                ohlc = pd.DataFrame(
                    {
                        "open": pd.to_numeric(df_feat["o"], errors="coerce"),
                        "high": pd.to_numeric(df_feat["h"], errors="coerce"),
                        "low": pd.to_numeric(df_feat["l"], errors="coerce"),
                        "close": pd.to_numeric(df_feat["c"], errors="coerce"),
                        "volume": pd.to_numeric(df_feat["v"], errors="coerce"),
                    },
                    index=df_feat.index,
                )
                df_feat["feat_tabajara"] = tabajara_categorical(ohlc).astype(float)
                _at = alphatrend_features(ohlc)
                df_feat["feat_alphatrend_dist_pct"] = _at["distance_pct"].astype(float)
                df_feat["feat_alphatrend_cross"] = _at["cross_signal"].astype(float)
                df_feat["feat_macd_4color"] = cm_macd_histogram_4color(ohlc).astype(
                    float
                )
                df_feat["feat_pin_bar"] = detect_pin_bar(ohlc).astype(float)
                df_feat["feat_sr_pos"] = sr_channel_position(ohlc).astype(float).fillna(
                    0.5
                )
                df_feat["feat_mfi_raw"] = mfi_wilder(ohlc, 14).astype(float)
            except Exception:
                logger.exception("lhn_indicators_pack_failed | ts=%s", int(time.time() * 1000))
                for _col in (
                    "feat_tabajara",
                    "feat_alphatrend_dist_pct",
                    "feat_alphatrend_cross",
                    "feat_macd_4color",
                    "feat_pin_bar",
                    "feat_sr_pos",
                    "feat_mfi_raw",
                ):
                    if _col not in df_feat.columns:
                        df_feat[_col] = 0.0
        return df_feat

    def _extrair_features_linha(
        self, df_row, l2_imbalance=0.0, realtime_data=None, ativo=None
    ):
        """Extrai vetor de features por vela. Layout 65D (MTF+lhn): exatamente 65 floats quando ``use_65d_layout``."""
        p = df_row["c"]
        rsi = df_row["rsi"]
        bias_ema = 0
        qtd = self.cfg["ema_count"]
        if self.cfg["use_ema"]:
            val_emas = [df_row[f"ema_{j}"] for j in range(qtd)]
            is_bull = (
                all(val_emas[j] > val_emas[j + 1] for j in range(qtd - 1))
                if qtd > 1
                else (p > val_emas[0])
            )
            is_bear = (
                all(val_emas[j] < val_emas[j + 1] for j in range(qtd - 1))
                if qtd > 1
                else (p < val_emas[0])
            )
            bias_ema = 1 if is_bull else -1 if is_bear else 0

        macd_norm = (df_row["macd"] / p) * 100
        atr_val = df_row["atr"]
        atr_norm = (atr_val / p) * 100
        adx_val = df_row["adx"]
        bias_1h = 1 if df_row["ema_36"] > df_row["ema_84"] else 0
        bias_4h = 1 if df_row["ema_144"] > df_row["ema_336"] else 0
        mom_5m_norm = ((df_row["c"] - df_row.get("c_shift", p)) / p) * 100
        stoch_val = df_row["stoch"]
        vol_ratio = df_row["vol_ratio"]
        flow_delta = df_row["order_flow_delta"]
        fin_agg = df_row["financial_aggression"]
        cvd_val = df_row["cvd_norm"]
        vwap_dist = df_row["vwap_dist"]
        hour_norm = df_row["hour_norm"]
        regime = df_row["regime"]

        l2_norm = max(-1.0, min(1.0, l2_imbalance / 100.0))

        dist_poc = float(df_row.get("dist_poc", 0.0))
        markov_comp = float(df_row.get("markov_compression", 0.0))

        rt = dict(realtime_data or {})
        sym = str(ativo).strip().upper() if ativo else ""
        if sym and hasattr(self, "realtime_data"):
            eng = (getattr(self, "realtime_data", None) or {}).get(sym)
            if isinstance(eng, dict):
                if "spoofing_signal" in eng:
                    rt["spoofing_signal"] = float(eng["spoofing_signal"])
                if "z_score_arb" in eng:
                    rt["z_score_arb"] = float(eng["z_score_arb"])

        vpin_agressao = float(rt.get("vpin", 50.0)) / 100.0
        z_arb_feat = float(
            rt.get("z_score_arb", rt.get("z_score", 0.0))
        )
        z_arb_feat = max(-10.0, min(10.0, z_arb_feat))
        dist_muro_compra = float(rt.get("dist_muro_compra", 0.0))
        dist_muro_venda = float(rt.get("dist_muro_venda", 0.0))
        spoof_strike_legacy = float(rt.get("spoofing", 0.0))
        spoofing_signal = float(rt.get("spoofing_signal", spoof_strike_legacy))
        spoofing_signal = max(-1.0, min(1.0, spoofing_signal))
        oi_delta = float(rt.get("oi_delta", 0.0))
        ofi_dinamico = float(rt.get("ofi", 0.0))

        use_mtf = bool(self.cfg.get("use_mtf_neural", False))
        use_65 = use_mtf and bool(self.cfg.get("use_65d_layout", False))

        cvd_rt_vetor = float(rt.get("cvd_vetor", 0.0))
        ofi_rt_vetor = float(rt.get("ofi_vetor", 0.0))

        # --- Oráculo Binance + on-chain: só no layout 65D (O(1): locks curtos) ---
        binance_lead = 0.0
        onchain_whale_n = 0.0
        binance_spread_rel = 0.0
        btc_adx_macro = 0.0
        if use_65:
            binance_lead = float(get_binance_lead_signal())
            onchain_whale_n = float(get_onchain_whale_alert_level()) * 0.5
            _bb, _ba, _bm = get_binance_oracle_top()
            if _bm > 1e-12:
                binance_spread_rel = float((_ba - _bb) / _bm)
            btc_adx_macro = float(
                max(0.0, min(100.0, getattr(self, "_btc_adx_macro", 0.0) or 0.0))
            )

        if use_65:
            # [65D base = 28] Substitui bias_1h/bias_4h duplicados dos blocos MTF por sinais globais + microestrutura de câmbio.
            base_features = [
                rsi,
                macd_norm,
                adx_val,
                bias_ema,
                atr_norm,
                binance_lead,
                onchain_whale_n,
                mom_5m_norm,
                stoch_val,
                vol_ratio,
                flow_delta,
                fin_agg,
                cvd_val,
                vwap_dist,
                hour_norm,
                regime,
                l2_norm,
                dist_poc,
                markov_comp,
                vpin_agressao,
                z_arb_feat,
                dist_muro_compra,
                dist_muro_venda,
                spoofing_signal,
                oi_delta,
                ofi_dinamico,
                binance_spread_rel,
                btc_adx_macro,
                cvd_rt_vetor,
                ofi_rt_vetor,
            ]
        else:
            base_features = [
                rsi,
                macd_norm,
                adx_val,
                bias_ema,
                atr_norm,
                float(bias_1h),
                float(bias_4h),
                mom_5m_norm,
                stoch_val,
                vol_ratio,
                flow_delta,
                fin_agg,
                cvd_val,
                vwap_dist,
                hour_norm,
                regime,
                l2_norm,
                dist_poc,
                markov_comp,
                vpin_agressao,
                z_arb_feat,
                dist_muro_compra,
                dist_muro_venda,
                spoof_strike_legacy,
                oi_delta,
                ofi_dinamico,
                cvd_rt_vetor,
                ofi_rt_vetor,
            ]

        if self.cfg.get("use_institutional_microstructure", False) and not use_mtf:
            cross = float(df_row.get("oi_funding_cross_hist", 0.0))
            if realtime_data:
                fr = float(realtime_data.get("funding_rate", 0.0))
                oi = float(realtime_data.get("oi_delta", 0.0))
                cross = float(np.clip(fr * 1e4 * oi / 100.0, -1.0, 1.0))
            base_features.extend(
                [
                    float(df_row.get("vpin_m1", 0.5)),
                    float(df_row.get("vpin_m5", 0.5)),
                    float(df_row.get("cvd_polar10_norm", 0.0)),
                    float(df_row.get("book_imb_approx", 0.0)),
                    float(df_row.get("oi_accel_proxy", 0.0)),
                    cross,
                ]
            )

        if use_mtf:
            if use_65:
                base_features.extend(
                    [
                        float(df_row.get("feat_tabajara", 0.0)),
                        float(df_row.get("feat_alphatrend_dist_pct", 0.0)),
                        float(df_row.get("feat_alphatrend_cross", 0.0)),
                        float(df_row.get("feat_macd_4color", 0.0)),
                        float(df_row.get("feat_pin_bar", 0.0)),
                        float(df_row.get("feat_sr_pos", 0.5)),
                        float(df_row.get("feat_mfi_raw", 50.0)),
                    ]
                )
                # [f_1h = 15] [f_4h = 15] — remove só hour_norm (já no núcleo 15m); bias_1h/4h e mom permanecem como contexto TF.
                f_1h = [
                    df_row.get("rsi_1h", rsi),
                    df_row.get("macd_1h", df_row.get("macd", 0))
                    / df_row.get("c_1h", p)
                    * 100,
                    df_row.get("adx_1h", adx_val),
                    float(bias_1h),
                    df_row.get("atr_1h", atr_val) / df_row.get("c_1h", p) * 100,
                    float(bias_1h),
                    float(bias_4h),
                    mom_5m_norm,
                    df_row.get("stoch_1h", stoch_val),
                    df_row.get("vol_ratio_1h", vol_ratio),
                    df_row.get("order_flow_delta_1h", flow_delta),
                    df_row.get("financial_aggression_1h", fin_agg),
                    df_row.get("cvd_norm_1h", cvd_val),
                    df_row.get("vwap_dist_1h", vwap_dist),
                    df_row.get("regime_1h", regime),
                ]
                f_4h = [
                    df_row.get("rsi_4h", rsi),
                    df_row.get("macd_4h", df_row.get("macd", 0))
                    / df_row.get("c_4h", p)
                    * 100,
                    df_row.get("adx_4h", adx_val),
                    float(bias_4h),
                    df_row.get("atr_4h", atr_val) / df_row.get("c_4h", p) * 100,
                    float(bias_1h),
                    float(bias_4h),
                    mom_5m_norm,
                    df_row.get("stoch_4h", stoch_val),
                    df_row.get("vol_ratio_4h", vol_ratio),
                    df_row.get("order_flow_delta_4h", flow_delta),
                    df_row.get("financial_aggression_4h", fin_agg),
                    df_row.get("cvd_norm_4h", cvd_val),
                    df_row.get("vwap_dist_4h", vwap_dist),
                    df_row.get("regime_4h", regime),
                ]
            else:
                f_1h = [
                    df_row.get("rsi_1h", rsi),
                    df_row.get("macd_1h", df_row.get("macd", 0))
                    / df_row.get("c_1h", p)
                    * 100,
                    df_row.get("adx_1h", adx_val),
                    float(bias_1h),
                    df_row.get("atr_1h", atr_val) / df_row.get("c_1h", p) * 100,
                    float(bias_1h),
                    float(bias_4h),
                    mom_5m_norm,
                    df_row.get("stoch_1h", stoch_val),
                    df_row.get("vol_ratio_1h", vol_ratio),
                    df_row.get("order_flow_delta_1h", flow_delta),
                    df_row.get("financial_aggression_1h", fin_agg),
                    df_row.get("cvd_norm_1h", cvd_val),
                    df_row.get("vwap_dist_1h", vwap_dist),
                    hour_norm,
                    df_row.get("regime_1h", regime),
                ]
                f_4h = [
                    df_row.get("rsi_4h", rsi),
                    df_row.get("macd_4h", df_row.get("macd", 0))
                    / df_row.get("c_4h", p)
                    * 100,
                    df_row.get("adx_4h", adx_val),
                    float(bias_4h),
                    df_row.get("atr_4h", atr_val) / df_row.get("c_4h", p) * 100,
                    float(bias_1h),
                    float(bias_4h),
                    mom_5m_norm,
                    df_row.get("stoch_4h", stoch_val),
                    df_row.get("vol_ratio_4h", vol_ratio),
                    df_row.get("order_flow_delta_4h", flow_delta),
                    df_row.get("financial_aggression_4h", fin_agg),
                    df_row.get("cvd_norm_4h", cvd_val),
                    df_row.get("vwap_dist_4h", vwap_dist),
                    hour_norm,
                    df_row.get("regime_4h", regime),
                ]
            base_features = base_features + f_1h + f_4h

        clean_features = []
        for x in base_features:
            if x != x or x == float("inf") or x == float("-inf") or x is None:
                clean_features.append(0.0)
            else:
                clean_features.append(float(x))

        if use_65 and len(clean_features) != 65:
            logger.warning(
                "neural_65d_shape_mismatch | got=%s expected=65 | ativo=%s",
                len(clean_features),
                sym or "?",
            )
        return clean_features

    def _sync_klines_tf_parquet(self, client, ativo, interval_api, tf_tag, max_rows):
        """Data Lake: Parquet local + delta; bulk inicial com paginação (pyarrow)."""
        dir_datalake = os.path.join(self.workspace_raiz, "lhn_datalake")
        os.makedirs(dir_datalake, exist_ok=True)
        # Partições canónicas MTF: klines_15_{SYMBOL}, klines_60_{SYMBOL}, klines_240_{SYMBOL}
        arquivo_canon = os.path.join(
            dir_datalake, f"klines_{interval_api}_{ativo}.parquet"
        )
        legacy_name = {"15": "15m", "60": "1h", "240": "4h"}.get(str(interval_api))
        arquivo_legacy = (
            os.path.join(dir_datalake, f"klines_{legacy_name}_{ativo}.parquet")
            if legacy_name
            else None
        )
        arquivo_read = (
            arquivo_canon
            if os.path.exists(arquivo_canon)
            else (arquivo_legacy if arquivo_legacy and os.path.exists(arquivo_legacy) else arquivo_canon)
        )
        arquivo_klines = arquivo_canon
        _cols = [
            "t",
            "o",
            "h",
            "l",
            "c",
            "v",
            "ct",
            "qv",
            "tr",
            "tb",
            "tq",
            "i",
        ]
        df = None
        if os.path.exists(arquivo_read):
            try:
                df = pd.read_parquet(arquivo_read)
                if len(df) < 100:
                    self.log_msg(
                        f"[{ativo}] Parquet {tf_tag} incompleto ({len(df)} linhas). Recriando."
                    )
                    df = None
                else:
                    ultimo_t = int(df["t"].iloc[-1])
                    time.sleep(0.5)
                    novas_k = fetch_klines_since_ms(
                        client, ativo, ultimo_t + 1, interval=interval_api
                    )
                    if novas_k:
                        df_novas = pd.DataFrame(novas_k, columns=_cols).astype(float)
                        df = pd.concat([df, df_novas], ignore_index=True)
                        df = df.drop_duplicates(subset=["t"], keep="last")
                        if len(df) > max_rows:
                            df = df.iloc[-max_rows:].copy()

                        def _w():
                            with self._datalake_write_lock:
                                try:
                                    df.to_parquet(
                                        arquivo_klines,
                                        engine="pyarrow",
                                        compression="snappy",
                                        index=False,
                                    )
                                except Exception:
                                    pass

                        self.enqueue_disk_io(_w)
                        self.log_msg(
                            f"☁️ [{ativo}] {tf_tag} Delta: +{len(df_novas)} velas. Total: {len(df)}."
                        )
                    else:
                        self.log_msg(
                            f"☁️ [{ativo}] Cache {tf_tag} OK ({len(df)} velas)."
                        )
            except Exception as e:
                self.erro_msg(
                    f"[{ativo}] Erro delta-sync {tf_tag}: {e}. Recriando data lake."
                )
                df = None

        if df is None:
            if self.cfg.get("use_binance_vision", False):
                self.log_msg(f"🌌 Data Lake massa Bybit ({tf_tag}) para {ativo}...")
                try:
                    time.sleep(0.5)
                    bulk = fetch_klines_historical_bulk(
                        client, ativo, interval=interval_api, months_back=12
                    )
                    if bulk:
                        df = pd.DataFrame(bulk, columns=_cols).astype(float)
                        if len(df) > max_rows:
                            df = df.iloc[-max_rows:].copy()

                        def _w2():
                            with self._datalake_write_lock:
                                try:
                                    df.to_parquet(
                                        arquivo_klines,
                                        engine="pyarrow",
                                        compression="snappy",
                                        index=False,
                                    )
                                except Exception:
                                    pass

                        self.enqueue_disk_io(_w2)
                        self.log_msg(
                            f"✅ Data Lake massivo {tf_tag} (Bybit): {len(df)} velas."
                        )
                except Exception as e:
                    self.log_msg(f"⚠️ Falha bulk Bybit {tf_tag} {ativo}: {e}")

            if df is None:
                try:
                    time.sleep(0.5)
                    k = fetch_klines_historical_bulk(
                        client, ativo, interval=interval_api, months_back=12
                    )
                    if not k:
                        self.erro_msg(f"REST retornou 0 klines {tf_tag} para {ativo}.")
                        return None
                    df = pd.DataFrame(k, columns=_cols).astype(float)
                    if len(df) > max_rows:
                        df = df.iloc[-max_rows:].copy()

                    def _w3():
                        with self._datalake_write_lock:
                            try:
                                df.to_parquet(
                                    arquivo_klines,
                                    engine="pyarrow",
                                    compression="snappy",
                                    index=False,
                                )
                            except Exception:
                                pass

                    self.enqueue_disk_io(_w3)
                    self.log_msg(
                        f"✅ Data Lake REST {tf_tag}: {len(df)} velas de {ativo}."
                    )
                except Exception as e_rest:
                    self.erro_msg(f"[{ativo}] Falha klines REST {tf_tag}: {e_rest}")
                    return None
        return df

    def processar_matriz_ativo(self, ativo, client):
        import traceback

        try:
            return self._processar_matriz_ativo_impl(ativo, client)
        except Exception as e:
            self.erro_msg(f"[{ativo}] ERRO CRÍTICO: {e}")
            self.erro_msg(traceback.format_exc()[:600])
            return [], [], [], []

    def _processar_matriz_ativo_impl(self, ativo, client):
        ativo = str(ativo or "").strip().upper()
        if not _is_valid_ai_market_symbol(ativo):
            return [], [], [], []
        # Lookback: com MTF — 15m≈180d; 1h/4h=1 ano. Sem MTF — 15m com lookback maior (config.NEURAL_BARS_*).
        max_15 = (
            NEURAL_BARS_15M_MTF
            if self.cfg.get("use_mtf_neural", False)
            else NEURAL_BARS_15M_BASE
        )
        df = self._sync_klines_tf_parquet(
            client, ativo, interval_api="15", tf_tag="15m", max_rows=max_15
        )  # Parquet: klines_15_{ativo}
        if df is None or len(df) < 100:
            return [], [], [], []

        df_feat = self._calcular_16d(df, include_lhn_pack=self._wants_lhn_65d_pack())
        df_feat["c_shift"] = df_feat["c"].shift(1)

        if self.cfg.get("use_mtf_neural", False):
            self.log_msg(
                f"🌀 MTF Parquet 15m/1h/4h ({self._neural_feature_dim()}D) — {ativo}..."
            )
            df1h = self._sync_klines_tf_parquet(
                client,
                ativo,
                interval_api="60",
                tf_tag="1h",
                max_rows=NEURAL_BARS_1H_YEAR,
            )  # Parquet: klines_60_{ativo}
            df4h = self._sync_klines_tf_parquet(
                client,
                ativo,
                interval_api="240",
                tf_tag="4h",
                max_rows=NEURAL_BARS_4H_YEAR,
            )  # Parquet: klines_240_{ativo}
            native_ok = (
                df1h is not None
                and len(df1h) >= 50
                and df4h is not None
                and len(df4h) >= 50
            )
            if native_ok:
                df_1h_feat = self._calcular_16d(df1h).add_suffix("_1h")
                df_4h_feat = self._calcular_16d(df4h).add_suffix("_4h")
            else:
                self.log_msg(
                    f"⚠️ [{ativo}] MTF 1h/4h nativo insuficiente — fallback reamostragem 15m."
                )
                df_datetime = df.copy()
                df_datetime["datetime"] = pd.to_datetime(df_datetime["t"], unit="ms")
                df_datetime.set_index("datetime", inplace=True)
                df_1h_rs = (
                    df_datetime.resample("1h")
                    .agg(
                        {
                            "t": "first",
                            "o": "first",
                            "h": "max",
                            "l": "min",
                            "c": "last",
                            "v": "sum",
                            "ct": "last",
                            "qv": "sum",
                            "tr": "sum",
                            "tb": "sum",
                            "tq": "sum",
                            "i": "last",
                        }
                    )
                    .dropna()
                )
                df_4h_rs = (
                    df_datetime.resample("4h")
                    .agg(
                        {
                            "t": "first",
                            "o": "first",
                            "h": "max",
                            "l": "min",
                            "c": "last",
                            "v": "sum",
                            "ct": "last",
                            "qv": "sum",
                            "tr": "sum",
                            "tb": "sum",
                            "tq": "sum",
                            "i": "last",
                        }
                    )
                    .dropna()
                )
                df_1h_feat = self._calcular_16d(df_1h_rs).add_suffix("_1h")
                df_4h_feat = self._calcular_16d(df_4h_rs).add_suffix("_4h")
            df_feat = df_feat.dropna(subset=["t"])
            df_1h_feat = df_1h_feat.dropna(subset=["t_1h"])
            df_4h_feat = df_4h_feat.dropna(subset=["t_4h"])
            _mcap = max(2000, min(500_000, int(self.cfg.get("neural_mtf_merge_cap", 80000) or 80000)))
            df_feat = df_feat.sort_values("t")
            if len(df_feat) > _mcap:
                df_feat = df_feat.iloc[-_mcap:].copy()
            if len(df_1h_feat) > _mcap:
                df_1h_feat = df_1h_feat.iloc[-_mcap:].copy()
            if len(df_4h_feat) > _mcap:
                df_4h_feat = df_4h_feat.iloc[-_mcap:].copy()
            # merge_asof no tempo de FECHO: cada vela 15m só vê 1h/4h já encerradas (sem leakage intrabar).
            df_feat["ct"] = pd.to_numeric(df_feat["ct"], errors="coerce").fillna(0).astype(
                "int64"
            )
            df_1h_feat["ct_1h"] = pd.to_numeric(
                df_1h_feat["ct_1h"], errors="coerce"
            ).fillna(0).astype("int64")
            df_4h_feat["ct_4h"] = pd.to_numeric(
                df_4h_feat["ct_4h"], errors="coerce"
            ).fillna(0).astype("int64")
            df_feat["_mtf_asof_left"] = df_feat["ct"].astype("int64")
            df_1h_feat["_mtf_asof_1h"] = df_1h_feat["ct_1h"].astype("int64")
            df_4h_feat["_mtf_asof_4h"] = df_4h_feat["ct_4h"].astype("int64")
            df_feat = pd.merge_asof(
                df_feat.sort_values("_mtf_asof_left"),
                df_1h_feat.sort_values("_mtf_asof_1h"),
                left_on="_mtf_asof_left",
                right_on="_mtf_asof_1h",
                direction="backward",
            )
            df_feat = df_feat.drop(
                columns=[c for c in ("_mtf_asof_1h",) if c in df_feat.columns],
                errors="ignore",
            )
            df_feat = pd.merge_asof(
                df_feat.sort_values("_mtf_asof_left"),
                df_4h_feat.sort_values("_mtf_asof_4h"),
                left_on="_mtf_asof_left",
                right_on="_mtf_asof_4h",
                direction="backward",
            )
            df_feat = df_feat.drop(
                columns=[
                    c
                    for c in ("_mtf_asof_left", "_mtf_asof_4h")
                    if c in df_feat.columns
                ],
                errors="ignore",
            )
            df_feat = df_feat.sort_values("t").reset_index(drop=True)
            df_feat = df_feat.ffill().fillna(0)

        X_sniper_l, y_sniper_l = [], []
        X_lateral_l, y_lateral_l = [], []
        time_steps = 10
        _stride = max(1, int(self.cfg.get("neural_label_stride", 1) or 1))
        for i in range(100, len(df_feat) - 15 - time_steps, _stride):
            window_df = df_feat.iloc[i : i + time_steps]
            row_atual = df_feat.iloc[i + time_steps - 1]
            p = row_atual["c"]
            if p == 0:
                continue
            future_window = df_feat["c"].iloc[i + time_steps : i + time_steps + 12]
            atr_val = row_atual["atr"]
            # Labels alinhados a R:R assimétrico (alvo ~5× distância do stop — expansão vs micro-scalp)
            rr_lab = float(self.cfg.get("label_rr_multiplier", 5.0) or 5.0)
            sl_atr = float(self.cfg.get("label_sl_atr_mult", 1.0) or 1.0)
            alvo_long = p + (atr_val * rr_lab * sl_atr)
            stop_long = p - (atr_val * sl_atr)
            alvo_short = p - (atr_val * rr_lab * sl_atr)
            stop_short = p + (atr_val * sl_atr)

            # [M4] Label Smoothing
            _smooth = (
                self.cfg.get("label_smoothing_factor", 0.05)
                if self.cfg.get("use_label_smoothing", True)
                else 0.0
            )
            max_futuro = future_window.max()
            min_futuro = future_window.min()
            use_ref = bool(self.cfg.get("use_reinforcement_lateral_training", False))
            if max_futuro >= alvo_long and min_futuro > stop_long:
                target = 1.0 - _smooth
                target_ref = 1.0
            elif min_futuro <= alvo_short and max_futuro < stop_short:
                target = 0.0 + _smooth
                target_ref = -1.0
            else:
                continue
            _adx_cut = float(
                self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
            )
            try:
                adx_val = float(row_atual.get("adx", _adx_cut))
            except (TypeError, ValueError):
                adx_val = _adx_cut
            features = [
                self._extrair_features_linha(w_row, ativo=ativo)
                for _, w_row in window_df.iterrows()
            ]
            if adx_val >= _adx_cut:
                X_sniper_l.append(features)
                y_sniper_l.append(target)
            else:
                X_lateral_l.append(features)
                if use_ref:
                    y_lateral_l.append(float(target_ref))
                else:
                    y_lateral_l.append(target)
        return X_sniper_l, y_sniper_l, X_lateral_l, y_lateral_l

    def treinar_ia(self):
        if TENSORFLOW_ERROR:
            self.erro_msg(f"⛔ FALHA AO CARREGAR TENSORFLOW: {TENSORFLOW_ERROR}")
            return
        try:
            modelos_dir = os.path.join(getattr(self, "workspace_raiz", "."), "modelos")
            os.makedirs(modelos_dir, exist_ok=True)
            self.arq_sniper_titular = os.path.join(modelos_dir, "SNIPER_TITULAR.keras")
            self.arq_lateral_titular = os.path.join(
                modelos_dir, "LATERAL_TITULAR.keras"
            )
            self.arquivo_cerebro_sniper = self.arq_sniper_titular
            self.arquivo_cerebro_lateral = self.arq_lateral_titular
            self.arquivo_cerebro = self.arq_sniper_titular
            shape_d = f"{self._neural_feature_dim()}D"
            self.log_msg(f"⚠️ FORJANDO IA SNIPER NEXUS — V90 Alpha ({shape_d})...")
            client = self.get_bybit_client()
            # Núcleo fixo ATIVOS_ELITE_PERMANENTE (10 pares; não é ranking dinâmico por market cap)
            self.ativos_elite_top10 = list(ATIVOS_ELITE_PERMANENTE)

            # Até 15 símbolos: os 10 elite + extras da lista de tickers (radar) até o teto
            ativos_treino = _sanitize_ai_market_symbols(self.ativos_elite_top10)
            current = _sanitize_ai_market_symbols(getattr(self, "tickers", []))
            for t in current:
                if t not in ativos_treino:
                    ativos_treino.append(t)
                if len(ativos_treino) >= 15:
                    break
            X_sniper, y_sniper = [], []
            X_lateral, y_lateral = [], []
            futures = []
            for ativo in ativos_treino:
                futures.append(
                    self._train_executor.submit(
                        self.processar_matriz_ativo, ativo, client
                    )
                )
                time.sleep(0.1)
            for future in futures:
                try:
                    X_sn_l, y_sn_l, X_lat_l, y_lat_l = future.result()
                    X_sniper.extend(X_sn_l)
                    y_sniper.extend(y_sn_l)
                    X_lateral.extend(X_lat_l)
                    y_lateral.extend(y_lat_l)
                except Exception as e:
                    self.erro_msg(f"Erro treino: {e}")

            if not hasattr(self, "model_sniper") or self.model_sniper is None:
                self.model_sniper = self.construir_rede_neural()
            if not hasattr(self, "model_lateral") or self.model_lateral is None:
                self.model_lateral = self.construir_rede_neural(
                    self._lateral_neural_mode()
                )
            self.model = self.model_sniper

            _min_n = max(8, int(self.cfg.get("neural_train_min_samples", 16) or 16))
            if not (len(X_sniper) > _min_n or len(X_lateral) > _min_n):
                self.erro_msg(
                    f"⛔ Treinamento abortado: Sniper {len(X_sniper)} / Lateral {len(X_lateral)} amostras "
                    f"(mínimo {_min_n + 1} em pelo menos um regime). Verifique dados no datalake / Bybit."
                )
                self.is_searching = False
                return

            if len(X_sniper) > _min_n and self.model_sniper and _LHNNeuralBatchSequence:
                _rdx = float(
                    self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
                )
                self.log_msg(
                    f"🎯 Treinamento SNIPER (ADX≥{_rdx:.0f}) com {len(X_sniper)} amostras..."
                )
                _n_sn = len(X_sniper)
                _bs_sn = self._neural_batch_size(_n_sn)
                _ep_sn = self._neural_fit_epochs()
                arr_x = np.array(X_sniper)
                arr_y = np.array(y_sniper)
                split = max(1, int(_n_sn * 0.8))
                if split >= _n_sn:
                    split = _n_sn - 1
                train_gen = _LHNNeuralBatchSequence(
                    arr_x[:split], arr_y[:split], _bs_sn
                )
                val_x = arr_x[split:]
                val_y = arr_y[split:]
                _cb_sn = self._neural_fit_callbacks(len(val_x) > 0)
                if len(val_x) > 0:
                    self._keras_fit(
                        self.model_sniper,
                        train_gen,
                        validation_data=(val_x, val_y),
                        epochs=_ep_sn,
                        callbacks=_cb_sn,
                        verbose=0,
                    )
                else:
                    self._keras_fit(
                        self.model_sniper,
                        train_gen,
                        epochs=_ep_sn,
                        callbacks=_cb_sn,
                        verbose=0,
                    )
                self.model_sniper.save(self.arquivo_cerebro_sniper)
                self.log_msg(
                    "✅ Cérebro Sniper titular persistido (SNIPER_TITULAR.keras)."
                )

            if len(X_lateral) > _min_n and self.model_lateral and _LHNNeuralBatchSequence:
                _rdx_l = float(
                    self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
                )
                self.log_msg(
                    f"🧱 Treinamento LATERAL (ADX<{_rdx_l:.0f}) com {len(X_lateral)} amostras..."
                )
                _n_lat = len(X_lateral)
                _bs_lat = self._neural_batch_size(_n_lat)
                _ep_lat = self._neural_fit_epochs()
                arr_xl = np.array(X_lateral)
                arr_yl = np.array(y_lateral)
                split_l = max(1, int(_n_lat * 0.8))
                if split_l >= _n_lat:
                    split_l = _n_lat - 1
                train_gen_l = _LHNNeuralBatchSequence(
                    arr_xl[:split_l], arr_yl[:split_l], _bs_lat
                )
                val_xl = arr_xl[split_l:]
                val_yl = arr_yl[split_l:]
                _cb_lat = self._neural_fit_callbacks(len(val_xl) > 0)
                if len(val_xl) > 0:
                    self._keras_fit(
                        self.model_lateral,
                        train_gen_l,
                        validation_data=(val_xl, val_yl),
                        epochs=_ep_lat,
                        callbacks=_cb_lat,
                        verbose=0,
                    )
                else:
                    self._keras_fit(
                        self.model_lateral,
                        train_gen_l,
                        epochs=_ep_lat,
                        callbacks=_cb_lat,
                        verbose=0,
                    )
                self.model_lateral.save(self.arquivo_cerebro_lateral)
                self.log_msg(
                    "✅ Cérebro Lateral titular persistido (LATERAL_TITULAR.keras)."
                )

            _did_persist = (len(X_sniper) > _min_n and self.model_sniper) or (
                len(X_lateral) > _min_n and self.model_lateral
            )

            self.log_msg(
                "🧹 Limpando tensores gigantes da memória (Garbage Collection Segura)..."
            )
            # [CORREÇÃO 1: MEMORY LEAK (CRÍTICO)] Anulação direta das referências pesadas
            X_sniper = y_sniper = X_lateral = y_lateral = None
            df = df_final = arr_x = arr_y = arr_xl = arr_yl = None
            train_gen = val_x = val_y = train_gen_l = val_xl = val_yl = None

            import gc

            gc.collect()
            if _did_persist and K is not None:
                K.clear_session()
                try:
                    if os.path.exists(self.arquivo_cerebro_sniper):
                        self.model_sniper = models.load_model(
                            self.arquivo_cerebro_sniper,
                            compile=False,
                            custom_objects=keras_custom_objects(),
                        )
                        self._recompile_loaded_gladiador(self.model_sniper, "sigmoid")
                    if os.path.exists(self.arquivo_cerebro_lateral):
                        self.model_lateral = models.load_model(
                            self.arquivo_cerebro_lateral,
                            compile=False,
                            custom_objects=keras_custom_objects(),
                        )
                        self._recompile_loaded_gladiador(
                            self.model_lateral, self._lateral_neural_mode()
                        )
                    self.model = self.model_sniper
                except Exception as _reload_e:
                    self.erro_msg(
                        f"Pós-treino: recarga Keras após clear_session: {_reload_e}"
                    )

            self.log_msg("🏁 Forja Dupla V90 Alpha concluída.")
            self.ia_treinada = True
            self.treinamento_concluido = True
            if hasattr(self, "_limpar_acumuladores_virtuais_pos_forja"):
                self._limpar_acumuladores_virtuais_pos_forja()
            self.log_msg(
                "ℹ️ Nota: A precisão elevada reflete a seletividade da IA em tendências claras (Sniper Mode)."
            )
            if getattr(self, "_workspace_ok", False) and getattr(
                self, "workspace_raiz", None
            ):
                self.is_searching = True
                self.log_msg("🚀 LHN SOVEREIGN V90 Alpha INICIADO — busca ativa.")
            else:
                self.is_searching = False
                self.log_msg(
                    "⚠️ Treino concluído, mas workspace não confirmado — use START_ENGINE após montar Workspace_LHN."
                )

            if hasattr(self, "iniciar_bot"):
                self.iniciar_bot()

            if not getattr(self, "_loop_analise_started", False):
                self._loop_analise_started = True
                self.submit_background_task(self.loop_analise_neural)
        except Exception as e:
            self.erro_msg(f"Erro Crítico em treinar_ia: {e}")
            self.is_searching = False

    def forjar_arena_completa(self):
        """Treina as 4 IAs: Titulares (LSTM+Attention) vs Reservas (Transformer)."""
        if TENSORFLOW_ERROR:
            self.erro_msg(f"⛔ FALHA TENSORFLOW: {TENSORFLOW_ERROR}")
            return
        if not getattr(self, "workspace_raiz", None):
            self.erro_msg(
                "⛔ Workspace não definido — monte Workspace_LHN antes da forja."
            )
            return

        self.log_msg("⚔️ [ARENA NEXUS] Iniciando a Grande Forja dos 4 Gladiadores...")
        modelos_dir = os.path.join(self.workspace_raiz, "modelos")
        os.makedirs(modelos_dir, exist_ok=True)
        self.arq_sniper_titular = os.path.join(modelos_dir, "SNIPER_TITULAR.keras")
        self.arq_sniper_reserva = os.path.join(modelos_dir, "SNIPER_RESERVA.keras")
        self.arq_lateral_titular = os.path.join(modelos_dir, "LATERAL_TITULAR.keras")
        self.arq_lateral_reserva = os.path.join(modelos_dir, "LATERAL_RESERVA.keras")
        self.arquivo_cerebro_sniper = self.arq_sniper_titular
        self.arquivo_cerebro_lateral = self.arq_lateral_titular
        self.arquivo_cerebro = self.arq_sniper_titular

        try:
            client = self.get_bybit_client()
        except Exception as e:
            self.erro_msg(f"⛔ Cliente Bybit indisponível: {e}")
            return

        if not hasattr(self, "ativos_elite_top10") or not self.ativos_elite_top10:
            self.ativos_elite_top10 = list(ATIVOS_ELITE_PERMANENTE)
        ativos_treino = list(self.ativos_elite_top10)[:10]

        X_sniper, y_sniper, X_lateral, y_lateral = [], [], [], []
        for ativo in ativos_treino:
            try:
                self.log_msg(f"🧬 Extraindo DNA de mercado de {ativo}...")
                X_sn, y_sn, X_lat, y_lat = self.processar_matriz_ativo(ativo, client)
                X_sniper.extend(X_sn)
                y_sniper.extend(y_sn)
                X_lateral.extend(X_lat)
                y_lateral.extend(y_lat)
            except Exception as e:
                self.erro_msg(f"Erro ao extrair {ativo}: {e}")

        _arena_min = max(8, int(self.cfg.get("neural_train_min_samples", 16) or 16))
        if len(X_sniper) < _arena_min and len(X_lateral) < _arena_min:
            self.erro_msg(
                f"⛔ Dados insuficientes para forjar a Arena (Sniper e Lateral < {_arena_min} amostras)."
            )
            return

        arr_x_sn = arr_y_sn = arr_x_lat = arr_y_lat = None
        uso_transformer_original = bool(self.cfg.get("use_transformer", False))
        _ep = self._neural_fit_epochs()
        _ep_res = max(_ep, int(round(_ep * 1.15)))

        try:
            arr_x_sn = np.array(X_sniper)
            arr_y_sn = np.array(y_sniper)
            arr_x_lat = np.array(X_lateral)
            arr_y_lat = np.array(y_lateral)

            self.log_msg("🛡️ Forjando TITULARES (LSTM + Attention — estabilidade)...")
            self.cfg["use_transformer"] = False

            if len(X_sniper) > _arena_min:
                m_sn_titular = self.construir_rede_neural("sigmoid")
                self._keras_fit(
                    m_sn_titular,
                    arr_x_sn,
                    arr_y_sn,
                    epochs=_ep,
                    batch_size=self._neural_batch_size(len(arr_x_sn)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                m_sn_titular.save(self.arq_sniper_titular)
                self.log_msg("✅ SNIPER_TITULAR forjado (LSTM).")
                if K is not None:
                    K.clear_session()

            if len(X_lateral) > _arena_min:
                m_lat_titular = self.construir_rede_neural(self._lateral_neural_mode())
                self._keras_fit(
                    m_lat_titular,
                    arr_x_lat,
                    arr_y_lat,
                    epochs=_ep,
                    batch_size=self._neural_batch_size(len(arr_x_lat)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                m_lat_titular.save(self.arq_lateral_titular)
                self.log_msg("✅ LATERAL_TITULAR forjado (LSTM).")
                if K is not None:
                    K.clear_session()

            self.log_msg("⚔️ Forjando RESERVAS (Transformer — agressividade)...")
            self.cfg["use_transformer"] = True

            if len(X_sniper) > _arena_min:
                m_sn_reserva = self.construir_rede_neural("sigmoid")
                self._keras_fit(
                    m_sn_reserva,
                    arr_x_sn,
                    arr_y_sn,
                    epochs=_ep_res,
                    batch_size=self._neural_batch_size(len(arr_x_sn)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                m_sn_reserva.save(self.arq_sniper_reserva)
                self.log_msg("✅ SNIPER_RESERVA forjado (Transformer).")
                if K is not None:
                    K.clear_session()

            if len(X_lateral) > _arena_min:
                m_lat_reserva = self.construir_rede_neural(self._lateral_neural_mode())
                self._keras_fit(
                    m_lat_reserva,
                    arr_x_lat,
                    arr_y_lat,
                    epochs=_ep_res,
                    batch_size=self._neural_batch_size(len(arr_x_lat)),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                m_lat_reserva.save(self.arq_lateral_reserva)
                self.log_msg("✅ LATERAL_RESERVA forjado (Transformer).")
                if K is not None:
                    K.clear_session()

            self.ia_treinada = True
            self.treinamento_concluido = True
            if hasattr(self, "_limpar_acumuladores_virtuais_pos_forja"):
                self._limpar_acumuladores_virtuais_pos_forja()
            if getattr(self, "_workspace_ok", False) and getattr(
                self, "workspace_raiz", None
            ):
                self.is_searching = True
            if hasattr(self, "iniciar_bot"):
                self.iniciar_bot()

        finally:
            self.cfg["use_transformer"] = uso_transformer_original
            try:
                if arr_x_sn is not None:
                    del arr_x_sn, arr_y_sn, arr_x_lat, arr_y_lat
            except Exception:
                pass
            
            # [CORREÇÃO: DEREFERENCING BRUTAL]
            arr_x_sn = arr_y_sn = arr_x_lat = arr_y_lat = None
            X_sniper = y_sniper = X_lateral = y_lateral = None
            m_sn_titular = m_lat_titular = m_sn_reserva = m_lat_reserva = None

            gc.collect()
            if K is not None:
                K.clear_session()
            self.log_msg(
                "🏁 [ARENA NEXUS] Grande Forja terminada. Recarregando gladiadores..."
            )
            try:
                self.ligar_cerebro_ia()
            except Exception as e:
                self.erro_msg(f"Recarga pós-forja: {e}")

    def forcar_retreino(self):
        if not getattr(self, "workspace_raiz", None):
            return
        md = os.path.join(self.workspace_raiz, "modelos")
        paths = [
            os.path.join(md, "SNIPER_TITULAR.keras"),
            os.path.join(md, "SNIPER_RESERVA.keras"),
            os.path.join(md, "LATERAL_TITULAR.keras"),
            os.path.join(md, "LATERAL_RESERVA.keras"),
            getattr(self, "arquivo_cerebro_sniper", None)
            or getattr(self, "arquivo_cerebro", None),
            getattr(self, "arquivo_cerebro_lateral", None),
        ]
        for path in paths:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
        self.ia_treinada = False
        self.treinamento_concluido = False
        self._loop_analise_started = False
        self.log_msg(
            "🔄 Cérebros Arena apagados. Iniciando forja completa (LSTM titulares / Transformer reservas)..."
        )
        self.submit_background_task(self.forjar_arena_completa)

    def _construir_cortex_guardiao(self):
        """[V90] Constrói ou carrega a Rede Neural DQN do Guardião."""
        import os

        import numpy as np
        from tensorflow.keras.layers import Dense, Dropout, Input
        from tensorflow.keras.models import Sequential, load_model
        from tensorflow.keras.optimizers import Adam

        caminho_modelo = os.path.join(
            getattr(self, "workspace_raiz", "./"), "modelos", "guardiao_v90.keras"
        )

        if os.path.exists(caminho_modelo):
            self.log_msg(
                "🧠 [GUARDIÃO] Córtex Keras carregado com sucesso das memórias passadas."
            )
            self.modelo_guardiao = load_model(caminho_modelo, compile=False)
            import tensorflow as tf
            import tensorflow.keras.backend as K

            def pain_mse(y_true, y_pred):
                mse = K.square(y_pred - y_true)
                pain_weight = tf.where(y_true < 0, 1.0 + tf.abs(y_true) * 10.0, 1.0)
                return K.mean(mse * pain_weight)

            self.modelo_guardiao.compile(
                optimizer=Adam(learning_rate=0.001), loss=pain_mse
            )
        else:
            self.log_msg(
                "⚠️ [GUARDIÃO] Córtex Vazio detectado. Forjando nova Rede Neural DQN..."
            )
            modelo = Sequential(
                [
                    Input(
                        shape=(5,)
                    ),  # Entradas: [PnL%, VPIN, ADX, Tempo_Aberto, Volatilidade]
                    Dense(64, activation="relu"),
                    Dropout(0.2),
                    Dense(32, activation="relu"),
                    Dense(
                        3, activation="linear"
                    ),  # Saídas (Q-Values): [HOLD, SCALE_OUT, PANIC_SELL]
                ]
            )
            import tensorflow as tf
            import tensorflow.keras.backend as K

            def pain_mse(y_true, y_pred):
                # Aprendizado pela Dor (V90 Pilar 1): Penaliza pesos com base na magnitude do prejuízo financeiro
                mse = K.square(y_pred - y_true)
                pain_weight = tf.where(y_true < 0, 1.0 + tf.abs(y_true) * 10.0, 1.0)
                return K.mean(mse * pain_weight)

            modelo.compile(optimizer=Adam(learning_rate=0.001), loss=pain_mse)
            self.modelo_guardiao = modelo

            os.makedirs(os.path.dirname(caminho_modelo), exist_ok=True)
            self.modelo_guardiao.save(caminho_modelo)

    def forjar_guardiao_historico(self):
        """[V90] Bootcamp: Treina o Guardião lendo o histórico da RAM (Lista de Dicionários)."""
        import os

        import numpy as np

        self.log_msg(
            "🌌 [BOOTCAMP V90] Iniciando treinamento histórico do Guardião (RAM Memory)..."
        )

        if not hasattr(self, "modelo_guardiao"):
            self._construir_cortex_guardiao()

        try:
            operacoes = getattr(self, "historico_operacoes", [])

            _gmin = max(8, int(self.cfg.get("neural_train_min_samples", 16) or 16))
            if not operacoes or len(operacoes) < max(12, _gmin // 2):
                self.log_msg(
                    f"⚠️ Histórico em memória insuficiente para o Bootcamp (mín. ~{max(12, _gmin // 2)} ops)."
                )
                return

            X_train = []
            y_train = []

            _op_cap = min(len(operacoes), 50_000)
            for op in operacoes[:_op_cap]:
                lucro = float(op.get("lucro", 0.0))
                direcao = op.get("tipo", "LONG")

                # Converte lucro absoluto em percentual fictício para a lógica da rede
                margem_usada = float(op.get("margem_gasta", 10.0))
                if margem_usada == 0:
                    margem_usada = 10.0
                pnl_perc = (lucro / margem_usada) * 100.0

                if lucro > 0:
                    estado = [pnl_perc, 85.0, 30.0, 15.0, 1.2]
                    q_values = [1.0, 0.5, -1.0]  # Prioriza HOLD
                else:
                    estado = [pnl_perc, 15.0, 15.0, 60.0, 3.5]
                    q_values = [-1.0, 0.0, 1.0]  # Prioriza PANIC_SELL

                X_train.append(estado)
                y_train.append(q_values)

            X_train = np.array(X_train, dtype=float)
            y_train = np.array(y_train, dtype=float)

            self.log_msg(
                f"🧠 [BOOTCAMP V90] Injetando {len(X_train)} memórias sintéticas institucionais extraídas da RAM..."
            )

            self._keras_fit(
                self.modelo_guardiao,
                X_train,
                y_train,
                epochs=self._neural_fit_epochs(),
                batch_size=self._neural_batch_size(len(X_train)),
                callbacks=self._neural_fit_callbacks(False),
                verbose=0,
            )

            caminho_modelo = os.path.join(
                getattr(self, "workspace_raiz", "./"), "modelos", "guardiao_v90.keras"
            )
            self.modelo_guardiao.save(caminho_modelo)
            self.log_msg(
                "✅ [BOOTCAMP V90] Treinamento Concluído! O Guardião agora possui intuição de mercado."
            )

            # [CORREÇÃO: DEREFERENCING BRUTAL]
            X_train = y_train = None
            import gc
            gc.collect()
            if 'K' in globals() and K is not None:
                K.clear_session()

        except Exception as e:
            self.erro_msg(f"Falha no Bootcamp do Guardião: {e}")

    def _sincronizar_e_extrair_features(self, ativo, client):
        try:
            # Puxa o dado L2 do EngineMixin (Se disponível)
            l2_imbalance = getattr(self, "l2_books", {}).get(ativo, 0.0)

            with self._ops_lock:
                _cached_df = self.kline_cache.get(ativo)
                _has_cache = _cached_df is not None and len(_cached_df) >= 100

            if not _has_cache:
                dir_datalake = os.path.join(self.workspace_raiz, "lhn_datalake")
                arquivo_klines_write = os.path.join(
                    dir_datalake, f"klines_15_{ativo}.parquet"
                )
                arquivo_klines_read = arquivo_klines_write
                arquivo_klines_legacy = os.path.join(
                    dir_datalake, f"klines_15m_{ativo}.parquet"
                )
                if not os.path.exists(arquivo_klines_read) and os.path.exists(
                    arquivo_klines_legacy
                ):
                    arquivo_klines_read = arquivo_klines_legacy
                df_local = None
                if os.path.exists(arquivo_klines_read):
                    try:
                        df_local = pd.read_parquet(arquivo_klines_read)
                        ultimo_t = int(df_local["t"].iloc[-1])
                        time.sleep(0.5)
                        novas_k = fetch_klines_since_ms(
                            client, ativo, ultimo_t + 1, interval="15"
                        )
                        if novas_k:
                            df_novas = pd.DataFrame(
                                novas_k,
                                columns=[
                                    "t",
                                    "o",
                                    "h",
                                    "l",
                                    "c",
                                    "v",
                                    "ct",
                                    "qv",
                                    "tr",
                                    "tb",
                                    "tq",
                                    "i",
                                ],
                            ).astype(float)
                            df_local = pd.concat(
                                [df_local, df_novas], ignore_index=True
                            )
                            if len(df_local) > 50_000:
                                df_local = df_local.iloc[-50_000:]

                            def _safe_write_parquet_sync():
                                with self._datalake_write_lock:
                                    try:
                                        df_local.to_parquet(
                                            arquivo_klines_write,
                                            engine="pyarrow",
                                            compression="snappy",
                                            index=False,
                                        )
                                    except Exception:
                                        pass

                            self.enqueue_disk_io(_safe_write_parquet_sync)
                    except Exception:
                        logger.exception(
                            "kline_delta_sync_failed | ts=%s | ativo=%s | payload=%s",
                            int(time.time() * 1000),
                            ativo,
                            {"arquivo_klines_read": arquivo_klines_read},
                        )
                        df_local = None
                if df_local is None:
                    k15 = self._binance_futures_klines_safe(
                        client, symbol=ativo, interval="15m", limit=400
                    )
                    if k15 is None:
                        return None
                    df_local = pd.DataFrame(
                        k15,
                        columns=[
                            "t",
                            "o",
                            "h",
                            "l",
                            "c",
                            "v",
                            "ct",
                            "qv",
                            "tr",
                            "tb",
                            "tq",
                            "i",
                        ],
                    ).astype(float)

                    def _safe_write_parquet_sync():
                        with self._datalake_write_lock:
                            try:
                                df_local.to_parquet(
                                    arquivo_klines_write,
                                    engine="pyarrow",
                                    compression="snappy",
                                    index=False,
                                )
                            except Exception:
                                pass

                    self.enqueue_disk_io(_safe_write_parquet_sync)
                df_local = df_local.iloc[-400:]
                with self._ops_lock:
                    self.kline_cache[ativo] = df_local
                    self.ultima_vela_t[ativo] = df_local["t"].iloc[-1]
            else:
                # Com use_kline_ws + feed recente, o cache já é atualizado pelo Túnel 4 (sem REST).
                _ws_map = getattr(self, "_kline_ws_last_ts", None) or {}
                _ws_fresh = (
                    bool(self.cfg.get("use_kline_ws", True))
                    and ativo in _ws_map
                    and (time.time() - float(_ws_map.get(ativo, 0))) < 300.0
                )
                if not _ws_fresh:
                    k15 = self._binance_futures_klines_safe(
                        client, symbol=ativo, interval="15m", limit=4
                    )
                    if k15 is None:
                        return None
                    df_novo = pd.DataFrame(
                        k15,
                        columns=[
                            "t",
                            "o",
                            "h",
                            "l",
                            "c",
                            "v",
                            "ct",
                            "qv",
                            "tr",
                            "tb",
                            "tq",
                            "i",
                        ],
                    ).astype(float)
                    with self._ops_lock:
                        cached_df = self.kline_cache.get(ativo)
                        cached_records = (
                            cached_df.to_dict("records")
                            if cached_df is not None and len(cached_df) > 0
                            else []
                        )

                    # Transformação pesada fora do lock para não bloquear pipeline WS.
                    t_novo_start = df_novo["t"].iloc[0]
                    cached_records = [
                        row for row in cached_records if row["t"] < t_novo_start
                    ]
                    cached_records.extend(df_novo.to_dict("records"))
                    if len(cached_records) > 400:
                        cached_records = cached_records[-400:]
                    df_updated = pd.DataFrame(cached_records)

                    with self._ops_lock:
                        self.kline_cache[ativo] = df_updated
                        self.ultima_vela_t[ativo] = df_updated["t"].iloc[-1]

            with self._ops_lock:
                df = self.kline_cache[ativo].copy()
            p = df["c"].iloc[-1]
            if p == 0:
                return None
            df_feat = self._calcular_16d(
                df, include_lhn_pack=self._wants_lhn_65d_pack()
            )
            adx_val = (
                float(df_feat["adx"].iloc[-1]) if "adx" in df_feat.columns else 0.0
            )
            if hasattr(
                self, "validar_regime_mercado"
            ) and not self.validar_regime_mercado(ativo, adx_val):
                return None
            # [INSTITUCIONAL V90] — sensores em tempo real (VPIN, muros, OI, spoofing)
            rt_data = {}
            p_atual_ativo = df["c"].iloc[-1]
            muros = getattr(self, "liquidity_walls", {}).get(ativo, {})
            if muros and p_atual_ativo > 0:
                rt_data["dist_muro_compra"] = (
                    (p_atual_ativo - muros.get("bid_wall_px", p_atual_ativo))
                    / p_atual_ativo
                ) * 100
                rt_data["dist_muro_venda"] = (
                    (muros.get("ask_wall_px", p_atual_ativo) - p_atual_ativo)
                    / p_atual_ativo
                ) * 100
            else:
                rt_data["dist_muro_compra"] = 0.0
                rt_data["dist_muro_venda"] = 0.0
            vd = getattr(self, "vpin_data", {}).copy().get(ativo, {})
            v_buy = vd.get("buy_vol", 0.0)
            v_sell = vd.get("sell_vol", 0.0)
            v_total = v_buy + v_sell
            rt_data["vpin"] = (
                (max(v_buy, v_sell) / v_total * 100) if v_total > 0 else 50.0
            )
            rt_data["spoofing"] = float(
                getattr(self, "spoof_strike", {}).get(ativo, 0.0)
            )
            rt_data["oi_delta"] = float(
                getattr(self, "_market_filter_cache", {})
                .get(ativo, {})
                .get("oi_delta_pct", 0.0)
            )
            try:
                _fm = self._coletar_filtros_mercado(ativo, client)
                rt_data["funding_rate"] = float(_fm.get("funding_rate", 0.0))
                rt_data["oi_delta"] = float(
                    _fm.get("oi_delta_pct", rt_data["oi_delta"])
                )
            except Exception:
                rt_data["funding_rate"] = 0.0
            rt_data["ofi"] = 0.0
            _rd = getattr(self, "realtime_data", {}).get(ativo, {})
            if isinstance(_rd, dict):
                if "z_score_arb" in _rd:
                    rt_data["z_score_arb"] = float(_rd["z_score_arb"])
                if "spoofing_signal" in _rd:
                    rt_data["spoofing_signal"] = float(_rd["spoofing_signal"])
            features_array = self._extrair_features_linha(
                df_feat.iloc[-1],
                l2_imbalance=l2_imbalance,
                realtime_data=rt_data,
                ativo=ativo,
            )
            with self._ops_lock:
                if ativo not in self.temporal_windows:
                    from collections import deque

                    self.temporal_windows[ativo] = deque(maxlen=10)
                self.temporal_windows[ativo].append(features_array)
                if len(self.temporal_windows[ativo]) < 10:
                    return None
                features_snapshot = list(self.temporal_windows[ativo])

            tr = pd.concat(
                [
                    df["h"] - df["l"],
                    abs(df["h"] - df["c"].shift()),
                    abs(df["l"] - df["c"].shift()),
                ],
                axis=1,
            ).max(axis=1)
            atr_val = tr.rolling(14).mean().iloc[-1]
            vol_ratio = (
                df["v"].iloc[-1]
                / (df["v"].rolling(self.cfg["vol_sma"]).mean().iloc[-1] + 1e-9)
            ) * 100
            sell_vol = df["v"].iloc[-1] - df["tb"].iloc[-1]
            flow_delta = (
                (df["tb"].iloc[-1] - sell_vol) / (df["v"].iloc[-1] + 1e-9)
            ) * 100

            macd = (
                df["c"].ewm(span=self.cfg["macd_f"], adjust=False).mean()
                - df["c"].ewm(span=self.cfg["macd_s"], adjust=False).mean()
            )
            macd_val = macd.iloc[-1]
            macd_sig = macd.ewm(span=self.cfg["macd_sig"], adjust=False).mean().iloc[-1]

            return {
                "ativo": ativo,
                "df": df,
                "p": p,
                "features": features_snapshot,
                "atr_val": atr_val,
                "vol_ratio": vol_ratio,
                "flow_delta": flow_delta,
                "macd_val": macd_val,
                "macd_sig": macd_sig,
                "tr": tr,
                "adx_val": adx_val,
            }
        except Exception:
            logger.exception(
                "feature_sync_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                ativo,
                {"client": str(type(client))},
            )
            return None

    def _coletar_filtros_mercado(self, ativo, client):
        now = time.time()
        ativo = str(ativo or "").strip().upper()
        if not _is_valid_ai_market_symbol(ativo):
            data = {"ts": now, "funding_rate": 0.0, "oi_delta_pct": 0.0}
            self._market_filter_cache[ativo] = data
            return data
        cache_item = self._market_filter_cache.get(ativo)
        if cache_item and (now - cache_item.get("ts", 0)) < 30:
            return cache_item
        if now < float(getattr(self, "_market_filter_rest_backoff_until", 0.0) or 0.0):
            return cache_item or {
                "ts": now,
                "funding_rate": 0.0,
                "oi_delta_pct": 0.0,
            }

        funding_rate = 0.0
        oi_delta_pct = 0.0
        try:
            premium = get_mark_price_and_funding(client, ativo)
            funding_rate = float(premium.get("lastFundingRate", 0.0))
        except Exception as exc:
            if _is_expected_market_data_error(exc):
                self._market_filter_rest_backoff_until = max(
                    float(getattr(self, "_market_filter_rest_backoff_until", 0.0) or 0.0),
                    now + 20.0,
                )
                if now - float(getattr(self, "_market_filter_last_warn_ts", 0.0) or 0.0) >= 15.0:
                    self._market_filter_last_warn_ts = now
                    self.log_msg(
                        f"⚠️ [MKT FILTER] Funding indisponível ({type(exc).__name__}: {exc}). Usando cache/zeros por 20s."
                    )
            else:
                logger.exception(
                    "funding_filter_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    ativo,
                    {"endpoint": "get_tickers_linear"},
                )

        try:
            oi_now = float(get_open_interest_now(client, ativo))
            oi_prev = self._open_interest_prev.get(ativo, oi_now)
            if oi_prev > 0:
                oi_delta_pct = ((oi_now - oi_prev) / oi_prev) * 100.0
            self._open_interest_prev[ativo] = oi_now
        except Exception as exc:
            if _is_expected_market_data_error(exc):
                self._market_filter_rest_backoff_until = max(
                    float(getattr(self, "_market_filter_rest_backoff_until", 0.0) or 0.0),
                    now + 20.0,
                )
                if now - float(getattr(self, "_market_filter_last_warn_ts", 0.0) or 0.0) >= 15.0:
                    self._market_filter_last_warn_ts = now
                    self.log_msg(
                        f"⚠️ [MKT FILTER] Open Interest indisponível ({type(exc).__name__}: {exc}). Usando cache/zeros por 20s."
                    )
            else:
                logger.exception(
                    "oi_filter_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    ativo,
                    {"endpoint": "get_open_interest"},
                )

        data = {"ts": now, "funding_rate": funding_rate, "oi_delta_pct": oi_delta_pct}
        self._market_filter_cache[ativo] = data
        return data

    def _reiniciar_thread_analise_neural(self):
        """Watchdog: nova geração do loop; o thread antigo sai ao detectar troca de geração."""
        with self._analise_restart_lock:
            self._analise_generation = int(getattr(self, "_analise_generation", 0)) + 1

        self.submit_background_task(self.loop_analise_neural)

    def _watchdog_loop_pulso_neural(self):
        """Cão de guarda: motor RUNNING e sem pulso >300s → alerta e reinicia loop_analise_neural_async."""
        import time

        while getattr(self, "is_app_alive", True):
            try:
                time.sleep(60)
                if not getattr(self, "estrategia_rodando", False):
                    continue
                pulse = getattr(self, "_ultima_pulse_neural_ts", None)
                if pulse is None:
                    continue
                if time.time() - float(pulse) <= 300.0:
                    continue
                now = time.time()
                if (
                    now
                    - float(getattr(self, "_last_watchdog_neural_restart_ts", 0) or 0)
                    < 60.0
                ):
                    continue
                self._last_watchdog_neural_restart_ts = now
                self.log_msg(
                    "⚠️ [WATCHDOG] Pulso neural ausente > 300s (motor RUNNING) — reiniciando loop_analise_neural_async."
                )
                self._reiniciar_thread_analise_neural()
            except Exception:
                import logging

                logger = logging.getLogger("LHN_Engine")
                logger.exception("watchdog_loop_pulso_neural")

    def loop_analise_neural(self):
        """Ponte síncrona para iniciar o motor assíncrono Omnibus em uma Thread isolada."""
        import asyncio

        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            self.log_msg(
                "🚀 [OMNISCIENCE] Iniciando ponte assíncrona AI na Thread local..."
            )
            new_loop.run_until_complete(self.loop_analise_neural_async())
        except Exception as e:
            self.erro_msg(f"🛑 [CRÍTICO] Falha na ponte assíncrona AI: {e}")
        finally:
            new_loop.close()

    async def loop_analise_neural_async(self):
        import asyncio
        import math
        import time
        from datetime import datetime

        import numpy as np
        import pandas as pd

        my_gen = int(getattr(self, "_analise_generation", 0))
        client = await asyncio.to_thread(self.get_bybit_client)
        while getattr(self, "is_app_alive", True):
            self._ultima_pulse_neural_ts = time.time()
            if int(getattr(self, "_analise_generation", 0)) != my_gen:
                self.log_msg(
                    "🔄 [IA] Geração do loop neural obsoleta — encerrando task antiga."
                )
                return
            if not getattr(self, "is_searching", False) or not self.ia_treinada:
                await asyncio.sleep(1)
                continue
            _neural_run = getattr(self, "modo_sniper_liberado", True) or getattr(
                self, "modo_lateral_macro_liberado", False
            )
            if not _neural_run:
                await asyncio.sleep(2)
                continue

            try:
                cycle_sinais = 0
                cycle_ordens = 0
                cycle_consensus_blocked = 0
                cycle_btc_blocked = 0
                cycle_certeza_baixa = 0
                cycle_smart_exits = 0

                t_cycle_start = time.time()
                features_to_predict, ativos_processados = [], []
                current_tickers = _sanitize_ai_market_symbols(getattr(self, "tickers", []))
                _kline_ws_map = getattr(self, "_kline_ws_last_ts", None) or {}
                current_tickers = [s for s in current_tickers if s in _kline_ws_map]

                sem = asyncio.Semaphore(10)

                async def extrair_feature_async(a, idx):
                    async with sem:
                        try:
                            if idx > 0:
                                await asyncio.sleep(0.05 * (idx % 10))
                            return await asyncio.to_thread(
                                self._sincronizar_e_extrair_features, a, client
                            )
                        except Exception as e:
                            return None

                resultados = await asyncio.gather(
                    *(
                        extrair_feature_async(a, i)
                        for i, a in enumerate(current_tickers)
                    )
                )

                for res in resultados:
                    if res:
                        features_to_predict.append(res["features"])
                        ativos_processados.append(res)

                if len(ativos_processados) > 0:
                    self._ultima_varredura_ts = time.time()

                if not features_to_predict:
                    await asyncio.sleep(1)
                    continue

                X_input = np.array(features_to_predict)
                use_mc_dropout = bool(self.cfg.get("use_mc_dropout", False))
                pred_std = None
                n_batch = len(ativos_processados)
                m_sn_t = (
                    getattr(self, "model_sniper_titular", None)
                    or getattr(self, "model_sniper", None)
                    or self.model
                )
                m_sn_r = getattr(self, "model_sniper_reserva", None) or m_sn_t
                m_lat_t = getattr(self, "model_lateral_titular", None) or getattr(
                    self, "model_lateral", None
                )
                m_lat_r = getattr(self, "model_lateral_reserva", None) or m_lat_t
                lateral_ok = m_lat_t is not None

                force_lat = getattr(
                    self, "modo_lateral_macro_liberado", False
                ) and not getattr(self, "modo_sniper_liberado", True)
                _adx_thr = float(
                    self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
                )
                idx_lat, idx_snp = [], []
                for j, res in enumerate(ativos_processados):
                    adx_atual = float(res.get("adx_val", _adx_thr) or _adx_thr)
                    if force_lat and lateral_ok:
                        idx_lat.append(j)
                    elif adx_atual < _adx_thr and lateral_ok:
                        idx_lat.append(j)
                    else:
                        idx_snp.append(j)

                if force_lat and not lateral_ok:
                    await asyncio.sleep(2)
                    continue

                predictions_titular = []
                predictions_reserva = []
                vetos_bayesianos = []
                
                for i in range(len(X_input)):
                    self._ultima_pulse_neural_ts = time.time()
                    X_b = X_input[i:i+1]
                    if i in idx_lat:
                        m_t = self.model_lateral_titular
                        m_r = self.model_lateral_reserva
                    else:
                        m_t = self.model_sniper_titular
                        m_r = self.model_sniper_reserva
                        
                    res_t = self._inferencia_bayesiana(m_t, X_b, n_iter=20, threshold_incerteza=0.15)
                    self._ultima_pulse_neural_ts = time.time()
                    res_r = self._inferencia_bayesiana(m_r, X_b, n_iter=20, threshold_incerteza=0.15)
                    self._ultima_pulse_neural_ts = time.time()
                    
                    predictions_titular.append([res_t["predicao"]])
                    predictions_reserva.append([res_r["predicao"]])
                    vetos_bayesianos.append(res_t["vetado_por_varianca"])

                use_tanh_lat = bool(
                    self.cfg.get("use_reinforcement_lateral_training", False)
                )
                idx_lat_set = set(idx_lat)

                bias_btc = 0
                if self.cfg.get("escudo_btc", True):
                    try:
                        k_btc = await asyncio.to_thread(
                            self._binance_futures_klines_safe,
                            client,
                            symbol="BTCUSDT",
                            interval="15m",
                            limit=50,
                        )
                        if k_btc is not None:
                            df_btc = pd.DataFrame(
                                k_btc,
                                columns=[
                                    "t",
                                    "o",
                                    "h",
                                    "l",
                                    "c",
                                    "v",
                                    "ct",
                                    "qv",
                                    "tr",
                                    "tb",
                                    "tq",
                                    "i",
                                ],
                            ).astype(float)
                            bias_btc = (
                                1
                                if df_btc["c"].ewm(span=9).mean().iloc[-1]
                                > df_btc["c"].ewm(span=21).mean().iloc[-1]
                                else -1
                            )
                    except Exception:
                        pass

                segundo_atual = datetime.now().second
                _cert_stretch = float(self.cfg.get("certeza_stretch", 1.0) or 1.0)

                for i, res in enumerate(ativos_processados):
                    ativo, df, p = res["ativo"], res["df"], res["p"]
                    temperatura = float(
                        self.cfg.get("calibracao_temperatura", 0.35) or 0.35
                    )
                    temperatura = max(0.05, temperatura)
                    raw_p = float(predictions_titular[i][0])
                    if use_tanh_lat and i in idx_lat_set:
                        certeza_frac = (raw_p + 1.0) / 2.0
                        certeza_frac = max(0.0, min(1.0, certeza_frac))
                        prob_ia = certeza_frac * 100.0
                    else:
                        prob_ia = (
                            1
                            / (
                                1
                                + math.exp(
                                    -max(
                                        min(
                                            (raw_p - 0.5) / temperatura,
                                            50,
                                        ),
                                        -50,
                                    )
                                )
                            )
                        ) * 100
                    prob_ia = _apply_certeza_stretch(prob_ia, _cert_stretch)
                    self.ia_cache_probs[ativo] = prob_ia
                    while len(self.ia_cache_probs) > 200:
                        self.ia_cache_probs.pop(next(iter(self.ia_cache_probs)))
                    if vetos_bayesianos[i]:
                        self.log_msg("WARNING: [VETO BAYESIANO] Operação barrada pelo Guardião. Incerteza interna da rede superior ao limite institucional.")
                        cycle_certeza_baixa += 1
                        continue

                    self.submit_background_task(
                        self.salvar_estado_db,
                        ativo,
                        res["features"],
                        1 if prob_ia > 50 else 0,
                        "SNIPER_V90 FINAL",
                    )

                    raw_p_reserva = float(predictions_reserva[i][0])
                    if use_tanh_lat and i in idx_lat_set:
                        prob_reserva = (raw_p_reserva + 1.0) / 2.0 * 100.0
                    else:
                        prob_reserva = (
                            1
                            / (
                                1
                                + math.exp(
                                    -max(
                                        min(
                                            (raw_p_reserva - 0.5) / temperatura,
                                            50,
                                        ),
                                        -50,
                                    )
                                )
                            )
                        ) * 100
                    prob_reserva = _apply_certeza_stretch(prob_reserva, _cert_stretch)
                    sinal_reserva = None
                    if prob_reserva > 85.0:
                        sinal_reserva = "LONG"
                    elif prob_reserva < 15.0:
                        sinal_reserva = "SHORT"
                    if sinal_reserva and getattr(self, "arquivo_db_memoria", None):
                        regime_sombra = "LATERAL" if i in idx_lat_set else "SNIPER"
                        _abertas = getattr(self, "_arena_sombra_abertas", None) or []
                        if any(
                            x.get("ativo") == ativo and x.get("regime") == regime_sombra
                            for x in _abertas
                        ):
                            sinal_reserva = None
                    if sinal_reserva and getattr(self, "arquivo_db_memoria", None):
                        regime_sombra = "LATERAL" if i in idx_lat_set else "SNIPER"
                        shadow_uid = (
                            f"{ativo}_{regime_sombra}_{int(time.time() * 1000)}"
                        )
                        alvo_base = float(self.cfg.get("alvo_lucro_base", 0.0) or 0.0)
                        _lat_macro = getattr(self, "modo_lateral_macro_liberado", False)
                        hard_sl_pct = float(self.cfg.get("hard_sl_pct", 0.015) or 0.015)
                        if _lat_macro and alvo_base > 0:
                            if sinal_reserva == "LONG":
                                sl_p = float(p) * (1.0 - hard_sl_pct)
                                tp_p = float(p) * (1.0 + alvo_base)
                            else:
                                sl_p = float(p) * (1.0 + hard_sl_pct)
                                tp_p = float(p) * (1.0 - alvo_base)
                        else:
                            _dyn_ar = self._compute_dynamic_sl_tp_rr(
                                float(p),
                                float(res.get("atr_val", p * 0.01) or p * 0.01),
                                sinal_reserva,
                                res.get("df"),
                                self.cfg,
                            )
                            sl_p, tp_p = float(_dyn_ar["sl"]), float(_dyn_ar["tp"])
                        margem_sim = float(self.cfg.get("margem_entrada", 10.0) or 10.0)
                        _feat = res["features"]
                        _agent_res = (
                            "ARENA_LATERAL_RESERVA"
                            if regime_sombra == "LATERAL"
                            else "ARENA_SNIPER_RESERVA"
                        )

                        def _salvar_sombra():
                            def _arena_db_worker(conn):
                                conn.execute(
                                    """
                                        INSERT INTO arena_reserva_log (
                                            regime, ativo, direcao, preco_entrada,
                                            pnl_simulado, lucro_usd, shadow_uid,
                                            status, sl_price, tp_price, margem_sim
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)
                                        """,
                                    (
                                        regime_sombra,
                                        ativo,
                                        sinal_reserva,
                                        float(p),
                                        0.0,
                                        None,
                                        shadow_uid,
                                        sl_p,
                                        tp_p,
                                        margem_sim,
                                    ),
                                )
                                conn.commit()

                            try:
                                self.enqueue_deep_memory_write(
                                    _arena_db_worker,
                                    max_attempts=6,
                                    base_delay=0.1,
                                    timeout=8.0,
                                )
                            except Exception:
                                pass
                            try:
                                if not hasattr(self, "_arena_sombra_lock"):
                                    self._arena_sombra_lock = threading.Lock()
                                with self._arena_sombra_lock:
                                    if not hasattr(self, "_arena_sombra_abertas"):
                                        self._arena_sombra_abertas = []
                                    self._arena_sombra_abertas.append(
                                        {
                                            "shadow_uid": shadow_uid,
                                            "ativo": ativo,
                                            "tipo": sinal_reserva,
                                            "preco": float(p),
                                            "sl": sl_p,
                                            "tp": tp_p,
                                            "margem_sim": margem_sim,
                                            "regime": regime_sombra,
                                        }
                                    )
                            except Exception:
                                pass

                        _salvar_sombra()
                        self.submit_background_task(
                            self.salvar_estado_db,
                            ativo,
                            _feat,
                            1 if prob_reserva > 50 else 0,
                            _agent_res,
                        )

                    votos_long, votos_short = 0, 0
                    delta_rsi = df["c"].diff()
                    rsi = (
                        100
                        - (
                            100
                            / (
                                1
                                + (
                                    delta_rsi.where(delta_rsi > 0, 0)
                                    .rolling(window=self.cfg["rsi_p"])
                                    .mean()
                                    / -delta_rsi.where(delta_rsi < 0, 0)
                                    .rolling(window=self.cfg["rsi_p"])
                                    .mean()
                                    .replace(0, 1)
                                )
                            )
                        )
                    ).iloc[-1]
                    if rsi < self.cfg["rsi_os"]:
                        votos_long += 1
                    elif rsi > self.cfg["rsi_ob"]:
                        votos_short += 1

                    val_emas = [
                        df["c"].ewm(span=self.cfg["emas"][j]).mean().iloc[-1]
                        for j in range(self.cfg["ema_count"])
                    ]
                    if (
                        all(
                            val_emas[j] > val_emas[j + 1]
                            for j in range(len(val_emas) - 1)
                        )
                        if len(val_emas) > 1
                        else (p > val_emas[0])
                    ):
                        votos_long += 1
                    elif (
                        all(
                            val_emas[j] < val_emas[j + 1]
                            for j in range(len(val_emas) - 1)
                        )
                        if len(val_emas) > 1
                        else (p < val_emas[0])
                    ):
                        votos_short += 1

                    if res["macd_val"] > res["macd_sig"]:
                        votos_long += 1
                    elif res["macd_val"] < res["macd_sig"]:
                        votos_short += 1

                    if res["vol_ratio"] > 120:
                        if votos_long > votos_short:
                            votos_long += 1
                        elif votos_short > votos_long:
                            votos_short += 1

                    with self._ops_lock:
                        if ativo in self.operacoes_abertas:
                            op = self.operacoes_abertas[ativo]
                            op["ia_prob"] = prob_ia
                            if self.cfg.get("smart_exit", True):
                                if time.time() - float(
                                    op.get("ts_abertura", time.time())
                                ) > 120:
                                    if (op["tipo"] == "LONG" and prob_ia < 20.0) or (
                                        op["tipo"] == "SHORT" and prob_ia > 80.0
                                    ):
                                        op["fechar_agora"] = (
                                            f"AI EXIT (prob={prob_ia:.1f}%)"
                                        )
                                        cycle_smart_exits += 1
                            continue

                        if (
                            len(getattr(self, "operacoes_abertas", {}))
                            >= MAX_OPERACOES_SIMULTANEAS
                        ):
                            continue

                    sinal = (
                        "LONG"
                        if votos_long >= self.cfg["confluencia_min"]
                        else (
                            "SHORT"
                            if votos_short >= self.cfg["confluencia_min"]
                            else None
                        )
                    )
                    if sinal:
                        # V90 FINAL.6: Candle-Sync Entry
                        if segundo_atual < 58:
                            continue
                        cycle_sinais += 1
                        certeza = prob_ia if sinal == "LONG" else (100 - prob_ia)
                        
                        limiar_certeza = float(self.cfg.get("ai_certainty_threshold", 85.0))
                        if certeza < limiar_certeza:
                            if hasattr(self, "log_msg"):
                                self.log_msg(f"🛑 Sinal descartado por baixa assimetria de risco (Confiança {certeza:.1f}% < {limiar_certeza:.1f}%)")
                            sinal = "NEUTRO"
                            cycle_certeza_baixa += 1
                            continue

                        ok_sm, fund_bonus = self._lateral_smart_money_adjust(
                            ativo, sinal, client
                        )
                        if not ok_sm:
                            cycle_consensus_blocked += 1
                            continue
                        certeza = min(100.0, certeza + fund_bonus)

                        if certeza < 85.0:
                            if hasattr(self, "log_msg"):
                                self.log_msg(f"🛑 Sinal descartado por baixa assimetria de risco (Confiança {certeza:.1f}% < 85%)")
                            sinal = "NEUTRO"
                            cycle_certeza_baixa += 1
                            continue

                        id_sinal = f"{ativo}_{int(df['t'].iloc[-1])}"
                        if id_sinal in self.historico_sinais_vela:
                            continue
                        if hasattr(
                            self, "validar_consenso_triplo"
                        ) and not self.validar_consenso_triplo(ativo, sinal, certeza):
                            cycle_consensus_blocked += 1
                            continue
                        if self.cfg.get("use_funding_filter", False) or self.cfg.get(
                            "use_oi_filter", False
                        ):
                            f_market = self._coletar_filtros_mercado(ativo, client)
                            if self.cfg.get("use_funding_filter", False):
                                fr = float(f_market.get("funding_rate", 0.0))
                                if (sinal == "LONG" and fr > 0.0008) or (
                                    sinal == "SHORT" and fr < -0.0008
                                ):
                                    cycle_consensus_blocked += 1
                                    continue
                            if self.cfg.get("use_oi_filter", False):
                                oi_delta = float(f_market.get("oi_delta_pct", 0.0))
                                if (sinal == "LONG" and oi_delta < 0.0) or (
                                    sinal == "SHORT" and oi_delta > 0.0
                                ):
                                    cycle_consensus_blocked += 1
                                    continue
                        if self.cfg.get("escudo_btc", True) and (
                            (sinal == "LONG" and bias_btc == -1)
                            or (sinal == "SHORT" and bias_btc == 1)
                        ):
                            cycle_btc_blocked += 1
                            continue

                        tick_ts_ms = None
                        with self._ops_lock:
                            tick_ts_ms = getattr(self, "tick_timestamps", {}).get(ativo)
                        self.disparar_ordem(
                            ativo,
                            sinal,
                            p,
                            certeza,
                            self.cfg["margem_entrada"],
                            res["atr_val"],
                            tick_ts_ms=tick_ts_ms,
                            market_context=res,
                        )
                        cycle_ordens += 1
                        self.historico_sinais_vela.append(id_sinal)
                        if len(self.historico_sinais_vela) > 500:
                            self.historico_sinais_vela = self.historico_sinais_vela[
                                -250:
                            ]

                lateral_count, trending_count = 0, 0
                if hasattr(self, "get_regime_summary"):
                    lateral_count, trending_count = self.get_regime_summary()

                filtered_total = (
                    len(current_tickers) - len(ativos_processados)
                    if "current_tickers" in dir()
                    else 0
                )
                parts = [
                    f"Radar: {len(ativos_processados) if 'ativos_processados' in dir() else 0}/{len(current_tickers) if 'current_tickers' in dir() else 0} ativos"
                ]
                if lateral_count > 0:
                    parts.append(f"Lateral: {lateral_count}")
                if trending_count > 0:
                    parts.append(f"Trend: {trending_count}")
                if cycle_sinais > 0:
                    parts.append(f"Sinais: {cycle_sinais}")
                if cycle_ordens > 0:
                    parts.append(f"Ordens: {cycle_ordens}")
                if cycle_consensus_blocked > 0:
                    parts.append(f"Bloq.Consenso: {cycle_consensus_blocked}")
                if cycle_btc_blocked > 0:
                    parts.append(f"Bloq.BTC: {cycle_btc_blocked}")
                if cycle_certeza_baixa > 0:
                    parts.append(f"Certeza<Min: {cycle_certeza_baixa}")
                if cycle_smart_exits > 0:
                    parts.append(f"SmartExit: {cycle_smart_exits}")
                with self._ops_lock:
                    ops_count = len(getattr(self, "operacoes_abertas", {}))
                parts.append(f"Abertos: {ops_count}/{MAX_OPERACOES_SIMULTANEAS}")

                self.log_msg(f"🔍 {' | '.join(parts)}")
                if cycle_ordens == 0:
                    hb_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.log_msg(
                        f"[{hb_ts}] 💓 Heartbeat: Motor LHN Nominal. Varredura concluída (Nenhuma confluência detectada)."
                    )

                elapsed_cycle = time.time() - t_cycle_start
                # ~60s entre ciclos completos de varredura (alinhado a limites REST / carga HFT).
                wait_s = max(0.0, 60.0 - elapsed_cycle)

                if not hasattr(self, "_stop_event_analise_async"):
                    self._stop_event_analise_async = asyncio.Event()

                try:
                    await asyncio.wait_for(
                        self._stop_event_analise_async.wait(), timeout=wait_s
                    )
                    self._stop_event_analise_async.clear()
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                import traceback

                self.log_msg(f"Erro Crítico no Córtex: {e}\n{traceback.format_exc()}")
                continue

    def disparar_ordem(
        self,
        ativo,
        sinal,
        preco_atual,
        certeza,
        margem,
        atr,
        tick_ts_ms=None,
        market_context=None,
        arena_regime_override=None,
    ):
        """Dispara a execução de uma ordem (Simulada ou Real).

        market_context: dict com 'df', 'adx_val', 'atr_val', 'vol_ratio' (radar) —
        ativa TP/SL dinâmicos (ATR+Bollinger) e filtro sniper antes da execução.
        arena_regime_override: ex. \"ARBITRAGEM\" — etiqueta a perna (Telegram VIP / sinais).
        """
        if ativo is None:
            return
        sym = str(ativo).strip()
        if not sym or sym.lower() in ("none", "null", ""):
            return
        ativo = sym
        is_arbitragem = str(arena_regime_override or "").strip().upper() == "ARBITRAGEM"

        # Cooldown anti-spam HFT (VPIN + Sniper): micro-trava de segundos por ativo
        _cd = getattr(self, "vpin_cooldown", {})
        if _cd is None:
            self.vpin_cooldown = {}
            _cd = self.vpin_cooldown
        if time.time() - _cd.get(ativo, 0) < 5:
            return
        with self._ativo_transicao_lock:
            if ativo in self._ativo_em_transicao:
                return
            self._ativo_em_transicao.add(ativo)
        pending_reserved = False
        try:
            rm = getattr(self, "risk_manager", None)
            if rm is not None and rm.ws_feed_blocks_new_orders():
                self.websocket_online = False
                self.monitoramento_apenas_por_ws = True
                self.log_msg(
                    "🛑 [KILL SWITCH] Feed WebSocket inativo — monitoramento apenas (sem novas ordens)."
                )
                return
            if rm is None and self.cfg.get("use_ws", True):
                _last = float(getattr(self, "_ws_price_last_tick_ts", 0) or 0)
                _stale = float(self.cfg.get("ws_feed_stale_sec", 30))
                if _last <= 0 or (time.time() - _last) > _stale:
                    self.websocket_online = False
                    self.monitoramento_apenas_por_ws = True
                    self.log_msg(
                        "🛑 [KILL SWITCH] Feed WebSocket inativo — monitoramento apenas (sem novas ordens)."
                    )
                    return
            self.websocket_online = True
            self.monitoramento_apenas_por_ws = False

            with self._ops_lock:
                if ativo in self.operacoes_abertas:
                    _ex = self.operacoes_abertas[ativo]
                    if not _ex.get("_pending"):
                        # 2. HARD BLOCK ANTI-MARTINGALE (Sobrevivência)
                        if _ex.get("pnl_flutuante_usd", 0.0) < 0:
                            self.log_msg(f"⛔ PROIBIDO MARTINGALE: PnL negativo em {ativo}. Ordem de scale-in bloqueada.")
                            return "PROIBIDO_MARTINGALE"
                        return
                n_open = sum(
                    1
                    for v in getattr(self, "operacoes_abertas", {}).values()
                    if not v.get("_pending")
                )
                if n_open >= MAX_OPERACOES_SIMULTANEAS:
                    self.log_msg(
                        f"⚠️ Trade bloqueado: Exposição máxima atingida ({MAX_OPERACOES_SIMULTANEAS}/{MAX_OPERACOES_SIMULTANEAS})."
                    )
                    return
                self.operacoes_abertas[ativo] = {
                    "_pending": True,
                    "_ts": time.time(),
                }
                pending_reserved = True

            with self._ops_lock:
                margem_alocada_total = sum(
                    float(op.get("margem", 0.0) or 0.0)
                    for op in getattr(self, "operacoes_abertas", {}).values()
                    if not op.get("_pending")
                )
            saldo_livre = max(
                0.0, float(getattr(self, "saldo_atual", 0.0)) - margem_alocada_total
            )
            if saldo_livre <= 0:
                return

            # Gestão de Risco Kelly Criterion (estilo V87: evita divisão por zero sem histórico)
            if self.cfg.get("use_kelly", True) and not is_arbitragem:
                kelly_pct = 0.05  # Default 5% da banca
                if (
                    self.total_trades > 5
                    and self.total_losses > 0
                    and self.total_wins > 0
                ):
                    wr_dec = self.total_wins / self.total_trades
                    avg_profit = self.total_profit_usd / self.total_wins
                    avg_loss = self.total_loss_usd / self.total_losses
                    if avg_loss <= 0.0:
                        avg_loss = 0.0001
                    ratio = max(avg_profit / avg_loss, 0.1)
                    kelly_pct = wr_dec - ((1.0 - wr_dec) / ratio)
                    if kelly_pct < 0:
                        kelly_pct = 0.005  # margem mínima de segurança
                risco_base = max(0.005, min(kelly_pct, 0.15))

                # Kelly Dinâmico: Escala com a certeza da IA
                risco_calculado = risco_base * (float(certeza) / 100.0)
                
                # Trava Institucional: Limite Máximo de 20%
                TETO_RISCO_GLOBAL_PCT = 0.20
                if risco_calculado > TETO_RISCO_GLOBAL_PCT:
                    risco_calculado = TETO_RISCO_GLOBAL_PCT
                    
                margem = saldo_livre * risco_calculado

            # [Fase 1] Hard Floor (Notional Mínimo Operacional)
            if float(margem) < 5.0 and saldo_livre >= 5.0:
                margem = 5.0

            saldo_disponivel = saldo_livre
            saldo_ref = float(getattr(self, "saldo_atual", 0.0))
            limites = obter_limites_risco(saldo_ref)
            limite_risco = limites["pct_max_por_operacao"]
            hard_cap_risco = saldo_ref * limite_risco
            margem_pre_cap = float(margem)
            margem = min(margem_pre_cap, hard_cap_risco, saldo_livre)
            if self.cfg.get("use_kelly", True) and margem_pre_cap > hard_cap_risco:
                self.log_msg(
                    f"⚠️ Kelly limitado pelo teto Tier {limites['tier']} ({limite_risco*100:.0f}% por operação ≈ US${hard_cap_risco:.2f})."
                )
            if margem <= 0:
                return

            # =================================================================
            # [V90 FINAL.5] MOTOR DE RISCO: KELLY DINÂMICO (TETO GLOBAL DE 20%)
            # =================================================================
            margem_final = float(margem)
            prob_ia = certeza

            if (
                not is_arbitragem
                and prob_ia
                and isinstance(prob_ia, (int, float))
                and prob_ia >= 50.0
            ):
                multiplicador_maximo = float(self.cfg.get("kelly_max_multiplier", 3.0))
                escala_certeza = (prob_ia - 50.0) / 50.0
                fator_kelly = 1.0 + (escala_certeza**2) * (multiplicador_maximo - 1.0)

                margem_antiga = margem_final
                margem_final = round(margem_final * fator_kelly, 2)

                if margem_final > margem_antiga and fator_kelly > 1.1:
                    self.log_msg(
                        f"⚖️ [KELLY DINÂMICO] Convicção de {prob_ia:.1f}% em {ativo}: Margem ampliada de US${margem_antiga} para US${margem_final}"
                    )

            # [CORREÇÃO 4] Piso rígido garantido no CORE para o Gate
            margem_final = max(5.00, float(margem_final))
            
            # [CORREÇÃO 2] Trava rígida limitando ao caixa livre real
            margem_final = min(
                margem_final,
                saldo_ref * limites["pct_max_por_operacao"],
                saldo_livre
            )
            with self._ops_lock:
                margem_em_uso_chk = sum(
                    float(op.get("margem", 0.0) or 0.0)
                    for op in getattr(self, "operacoes_abertas", {}).values()
                    if not op.get("_pending")
                )
            teto_exposicao = saldo_ref * limites["pct_exposicao_total"]
            if margem_em_uso_chk + margem_final > teto_exposicao + 1e-9:
                self.log_msg(
                    f"🛡️ [RISCO] Hard stop Tier {limites['tier']}: margem total projetada "
                    f"US${margem_em_uso_chk + margem_final:.2f} excede o teto de exposição "
                    f"US${teto_exposicao:.2f} ({limites['pct_exposicao_total']*100:.0f}% do saldo). Ordem bloqueada."
                )
                return
            if margem_final < 5.0:
                self.log_msg(
                    f"🛡️ [RISCO] Margem final calculada (US${margem_final:.2f}) abaixo do mínimo operacional. Tiro abortado."
                )
                return

            margem = margem_final
            # =================================================================

            alav = self.cfg["alavancagem"]

            # --- Gate unificado (exchange + caixa): simulação e conta real ---
            try:
                _client = self.get_bybit_client()
                _info = self.get_futures_exchange_info_cached(_client, ttl_sec=86400)
            except Exception:
                _info = {"symbols": []}
            _avail = self.get_available_balance_usdt_for_orders()
            gate = validate_linear_open_order(
                symbol=ativo,
                margem=float(margem),
                leverage=int(alav),
                mark_price=float(preco_atual),
                exchange_info=_info,
                available_balance_usdt=_avail,
                max_order_usd=float(self.cfg.get("max_order_usd", 500_000.0)),
                margin_buffer_pct=float(self.cfg.get("order_margin_buffer_pct", 0.002)),
            )
            if not gate.ok:
                self.log_msg(f"🛡️ [GATE] Ordem não autorizada ({ativo}): {gate.motivo}")
                return
            margem = gate.margem_usada

            # Stop Loss e Take Profit: dinâmicos (ATR + Bollinger) com R:R 1:5 a 1:10
            atr_safe = max(float(atr) if atr is not None else 0.0, preco_atual * 0.002)
            hard_sl_pct = float(self.cfg.get("hard_sl_pct", 0.015) or 0.015)
            # alvo_base legado pode existir via config; para cumprir separação estrita por ADX
            # (Sniper vs Lateral) usamos R:R regime-específico logo abaixo.
            _alvo_base_legacy = float(self.cfg.get("alvo_lucro_base", 0.0) or 0.0)

            _adx_thr_ent = float(
                self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
            )
            adx_val = _adx_thr_ent
            if isinstance(market_context, dict):
                adx_val = float(
                    market_context.get("adx_val", _adx_thr_ent) or _adx_thr_ent
                )
            is_sniper = adx_val >= _adx_thr_ent

            _df_mc = None
            if isinstance(market_context, dict):
                _df_mc = market_context.get("df")
            if is_sniper:
                # Sniper (ADX >= 25): alvo projetado mais agressivo + gate estrito
                dyn = self._compute_dynamic_sl_tp_rr(
                    preco_atual,
                    atr_safe,
                    sinal,
                    _df_mc,
                    # Mantém padrão alto de R:R (1:5 .. 1:10) via defaults do próprio método.
                    dict(self.cfg),
                )
                sl, tp = dyn["sl"], dyn["tp"]
                if isinstance(market_context, dict) and not self._sniper_volatility_gate_accept(
                    market_context,
                    sinal,
                    preco_atual,
                    float(dyn["sl_dist"]),
                    float(dyn["rr"]),
                ):
                    return
            else:
                # Lateral (ADX < 25): NÃO aplicar gate do Sniper.
                # Targets mais curtos: R:R na faixa ~1:2 .. 1:3 e TP dinâmico via Bollinger/ATR.
                cfg_lat = dict(self.cfg)
                cfg_lat["rr_ratio_min"] = 2.0
                cfg_lat["rr_ratio_max"] = 3.0
                sl_tp_lat = self._compute_dynamic_sl_tp_rr(
                    preco_atual,
                    atr_safe,
                    sinal,
                    _df_mc,
                    cfg_lat,
                )
                sl, tp = sl_tp_lat["sl"], sl_tp_lat["tp"]

            # 1. CHANDELIER EXIT (ATR Dinâmico Inicial)
            # Substitui as saídas estáticas/legadas pelo ATR Chandelier (2.5x ATR de SL, 10x ATR de TP para surfar)
            if sinal == "LONG":
                sl = preco_atual - (2.5 * atr_safe)
                tp = preco_atual + (10.0 * atr_safe)
            else:
                sl = preco_atual + (2.5 * atr_safe)
                tp = preco_atual - (10.0 * atr_safe)

            qty_real = 0
            if getattr(self, "is_real_account_mode", lambda: False)():
                t_order_ms = int(time.time() * 1000)
                sucesso, qty_real, margem = self.executar_ordem_real(
                    ativo, sinal, margem, alav, preco_atual
                )
                if not sucesso:
                    return
                if tick_ts_ms:
                    self.log_msg(
                        f"⚡ LATÊNCIA HFT ({ativo}): {max(0, t_order_ms - int(tick_ts_ms))} ms | T_tick={int(tick_ts_ms)} | T_order={t_order_ms}"
                    )
                # [V90 FINAL.6] Ativação do Airbag de Emergência (Substitui o SL público)
                if hasattr(self, "loop") and self.loop:
                    try:
                        import asyncio

                        asyncio.run_coroutine_threadsafe(
                            self._armar_airbag_async(ativo, sinal, preco_atual),
                            self.loop,
                        )
                    except Exception as e:
                        self.erro_msg(f"⚠️ Erro ao agendar Airbag para {ativo}: {e}")
            else:
                # Mesma quantidade dimensionada pelo gate que na conta real (paridade paper/replay)
                qty_real = float(gate.qty)
                if tick_ts_ms:
                    t_order_ms = int(time.time() * 1000)
                    self.log_msg(
                        f"⚡ LATÊNCIA HFT ({ativo}): {max(0, t_order_ms - int(tick_ts_ms))} ms | T_tick={int(tick_ts_ms)} | T_order={t_order_ms}"
                    )

            with self._ops_lock:
                _regime = (
                    str(arena_regime_override).strip().upper()
                    if arena_regime_override
                    else ("SNIPER" if is_sniper else "LATERAL")
                )
                self.operacoes_abertas[ativo] = {
                    "tipo": sinal,
                    "preco": preco_atual,
                    "margem": margem,
                    "alav": alav,
                    # Separação estrita pelo pivô ADX=25 (rede Sniper vs rede Lateral) ou override (pairs)
                    "arena_regime": _regime,
                    "adx_val": adx_val,
                    "sl": sl,
                    "tp": tp,
                    "certeza": certeza,
                    "ia_prob": certeza if sinal == "LONG" else (100 - certeza),
                    "ts_abertura": time.time(),
                    "qty_real": qty_real,
                    "qtd": str(float(qty_real)) if qty_real else "0",
                    "dca_level": 1,
                    "max_dca": 3,
                    "atr_dca": atr_safe,
                }
                if not hasattr(self, "vpin_cooldown") or self.vpin_cooldown is None:
                    self.vpin_cooldown = {}
                self.vpin_cooldown[ativo] = time.time()
                pending_reserved = False
            self.log_msg(
                f"🚀 ORDEM ABERTA: {sinal} em {ativo} | Preço: {preco_atual:.4f} | Certeza: {certeza:.1f}%"
            )

            # ── [V90] HOOK DE SINAIS: Registrar + Transmitir ──────────────────────────
            try:
                _ts_sinal = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                _sinal_entry = {
                    "id": id(ativo) ^ int(time.time()),
                    "timestamp": _ts_sinal,
                    "par": ativo,
                    "acao": sinal,
                    "preco_entrada": round(float(preco_atual), 6),
                    "tp1": round(float(tp), 6),
                    "tp2": None,
                    "tp3": None,
                    "sl": round(float(sl), 6),
                    "certeza": round(float(certeza), 2),
                    "fluxo": round(
                        float(getattr(self, "pontuacao_sentimento_atual", 0.0)), 2
                    ),
                    "status": "ABERTA",
                }
                # Buffer em memória (máx 1000 entradas)
                if (
                    not hasattr(self, "sinais_historico")
                    or self.sinais_historico is None
                ):
                    self.sinais_historico = []
                self.sinais_historico.append(_sinal_entry)
                if len(self.sinais_historico) > 1000:
                    self.sinais_historico = self.sinais_historico[-1000:]
                if hasattr(self, "persistir_historico_sinais"):
                    self.persistir_historico_sinais()
            except Exception as _e_sinal:
                pass  # Nunca bloquear o motor HFT por causa do log de sinais

            # Transmitir para o bot de sinais do Telegram (non-blocking) — não pairs / arb
            try:
                if (
                    _regime != "ARBITRAGEM"
                    and hasattr(self, "transmitir_sinal_telegram")
                    and hasattr(self, "loop")
                    and self.loop
                ):
                    import asyncio as _asyncio_sinal

                    vpin_score = float(getattr(self, "pontuacao_sentimento_atual", 0.0))
                    _asyncio_sinal.run_coroutine_threadsafe(
                        self.transmitir_sinal_telegram(
                            ativo,
                            sinal,
                            preco_atual,
                            certeza,
                            sl=sl,
                            tp1=tp,
                            vpin=vpin_score,
                        ),
                        self.loop,
                    )
            except Exception:
                pass  # Transmissão falhou — motor não é afectado
            # ─────────────────────────────────────────────────────────────────────────

            margem_usada = margem
            direcao = sinal
            preco = preco_atual
            from datetime import datetime

            hora_str = datetime.now().strftime("%d/%m/%Y %H:%M")
            modo_texto = f" | Tier {limites['tier']} ({limite_risco*100:.0f}%/op)"
            msg_entrada = (
                f"[{hora_str}] LHN Sovereign: 🟢 Alerta de Entrada (Sniper):\n\n"
                f"🎯 NOVO ALVO TRAVADO\n"
                f"Ativo: {ativo} | Direção: {direcao}\n"
                f"Preço de Entrada: {preco}\n"
                f"Certeza da IA: {certeza}%\n"
                f"Margem Usada: $ {margem_usada:.2f}{modo_texto}"
            )
            try:
                import asyncio as _asyncio_local

                if (
                    _regime != "ARBITRAGEM"
                    and hasattr(self, "loop")
                    and self.loop
                ):
                    _asyncio_local.run_coroutine_threadsafe(
                        self.enviar_alerta_telegram(msg_entrada), self.loop
                    )
            except Exception as e:
                print(f"❌ [TELEGRAM ERROR] {e}")

        finally:
            if pending_reserved:
                with self._ops_lock:
                    _op = self.operacoes_abertas.get(ativo)
                    if _op and _op.get("_pending"):
                        del self.operacoes_abertas[ativo]
            with self._ativo_transicao_lock:
                self._ativo_em_transicao.discard(ativo)

    def salvar_estado_db(self, ativo, features, acao, agent_id="SNIPER_V90 FINAL"):
        """Salva a experiência AI para treinamento futuro."""
        db_path = getattr(self, "db_path", None) or getattr(
            self, "arquivo_db_memoria", None
        )
        if not db_path or not os.path.isfile(db_path):
            return

        import json

        dim_exp = int(self._neural_feature_dim())
        if (
            not isinstance(features, list)
            or len(features) != 10
            or not isinstance(features[0], list)
            or len(features[0]) != dim_exp
        ):
            logger.warning(
                "replay_buffer_skip_save_shape | ativo=%s | need=%s | got=%s",
                ativo,
                dim_exp,
                len(features[0]) if isinstance(features, list) and features and isinstance(features[0], list) else None,
            )
            return

        def _worker(conn):
            cols = [
                c[1]
                for c in conn.execute("PRAGMA table_info(replay_buffer)").fetchall()
            ]
            if "reward" not in cols:
                conn.execute("ALTER TABLE replay_buffer ADD COLUMN reward REAL")
            if "error_flag" not in cols:
                conn.execute(
                    "ALTER TABLE replay_buffer ADD COLUMN error_flag INTEGER DEFAULT 0"
                )
            if "agent_id" not in cols:
                conn.execute(
                    "ALTER TABLE replay_buffer ADD COLUMN agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'"
                )
            state_json = json.dumps(features)
            conn.execute(
                "INSERT INTO replay_buffer (ativo, state, action, reward, error_flag, agent_id) VALUES (?, ?, ?, ?, ?, ?)",
                (ativo, state_json, acao, None, 0, agent_id),
            )
            if (
                time.time() - float(getattr(self, "_replay_maintenance_ts", 0.0))
            ) > 1800:
                conn.execute(
                    "DELETE FROM replay_buffer WHERE timestamp < datetime('now','-30 day')"
                )
                conn.execute("PRAGMA incremental_vacuum(200)")
                self._replay_maintenance_ts = time.time()
            conn.commit()

        try:
            self.enqueue_deep_memory_write(
                _worker, max_attempts=6, base_delay=0.12, timeout=12.0, db_path=db_path
            )
        except Exception:
            logger.exception(
                "save_state_db_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                ativo,
                {"acao": acao},
            )

    def registrar_resultado_replay(self, ativo, reward, agent_id="SNIPER_V90 FINAL"):
        """Atualiza a última memória aberta do ativo para Prioritized Experience Replay."""
        if not getattr(self, "arquivo_db_memoria", None) or not os.path.isfile(
            self.arquivo_db_memoria
        ):
            return
        try:

            def _worker(conn):
                cols = [
                    c[1]
                    for c in conn.execute("PRAGMA table_info(replay_buffer)").fetchall()
                ]
                if "reward" not in cols:
                    conn.execute("ALTER TABLE replay_buffer ADD COLUMN reward REAL")
                if "error_flag" not in cols:
                    conn.execute(
                        "ALTER TABLE replay_buffer ADD COLUMN error_flag INTEGER DEFAULT 0"
                    )
                if "agent_id" not in cols:
                    conn.execute(
                        "ALTER TABLE replay_buffer ADD COLUMN agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'"
                    )

                row = conn.execute(
                    "SELECT id FROM replay_buffer WHERE ativo = ? AND reward IS NULL AND agent_id = ? ORDER BY id DESC LIMIT 1",
                    (ativo, agent_id),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        "SELECT id FROM replay_buffer WHERE ativo = ? AND agent_id = ? ORDER BY id DESC LIMIT 1",
                        (ativo, agent_id),
                    ).fetchone()
                if row is not None:
                    import math

                    raw_r = float(reward)
                    error_flag = 2 if raw_r <= -0.25 else (1 if raw_r < 0 else 0)

                    # [V90 Pilar 1] Reward shaping + castigo reforçado em SL / stop-out
                    if raw_r <= -0.25:
                        shaped_r = -1.0
                    elif raw_r <= 0.2:
                        shaped_r = 0.08
                    else:
                        shaped_r = min(1.0, math.tanh(max(0.0, raw_r) * 1.18))

                    # Garante o teto rígido [-1.0, 1.0] contra Exploding Gradients
                    shaped_r = max(-1.0, min(1.0, shaped_r))

                    conn.execute(
                        "UPDATE replay_buffer SET reward = ?, error_flag = ?, done = 1 WHERE id = ?",
                        (shaped_r, error_flag, int(row[0])),
                    )
                    conn.commit()

            self.enqueue_deep_memory_write(
                _worker, max_attempts=6, base_delay=0.1, timeout=8.0
            )
            if self.cfg.get("incremental_replay_on_trade_close", True):
                self._schedule_incremental_replay_fit()
        except Exception:
            logger.exception(
                "register_replay_result_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                ativo,
                {"reward": reward},
            )

    def forjar_memoria_recente(self):
        """Experience Replay horário: últimos 10k ticks do Data Lake → só modelos RESERVA (shadow)."""
        import gc
        import os

        import numpy as np
        import pandas as pd

        try:
            if not getattr(self, "ia_treinada", False):
                return

            if (
                self.cfg.get("replay_buffer_purge_on_layout_upgrade", False)
                and self._wants_lhn_65d_pack()
                and not getattr(self, "_replay_layout_purge_done", False)
            ):
                self._replay_layout_purge_done = True

                def _wipe_rb(conn):
                    conn.execute("DELETE FROM replay_buffer")
                    conn.commit()

                try:
                    self.enqueue_deep_memory_write(
                        _wipe_rb,
                        max_attempts=4,
                        base_delay=0.15,
                        timeout=30.0,
                    )
                    if not getattr(self, "_silent_neural_refresh", False):
                        self.log_msg(
                            "🧹 [REPLAY] replay_buffer limpo (upgrade V90.65D — evita tensores 58D legados)."
                        )
                except Exception:
                    logger.exception("replay_buffer_purge_failed")

            _sv = getattr(self, "_silent_neural_refresh", False)
            pq_path = self.get_neural_lake_path()
            if not pq_path or not os.path.exists(pq_path):
                try:
                    from neural_lake_export import try_build_lake_from_replay

                    n_built, _built_path = try_build_lake_from_replay(
                        getattr(self, "arquivo_db_memoria", None),
                        getattr(self, "workspace_raiz", None),
                        getattr(self, "cfg", None),
                    )
                    if n_built > 0:
                        if not _sv:
                            self.log_msg(
                                f"🌌 Data Lake neural gerado a partir do replay_buffer ({n_built} amostras)."
                            )
                        pq_path = self.get_neural_lake_path()
                except Exception as e:
                    self.erro_msg(
                        f"Exportação replay → neural_training_lake.parquet: {e}"
                    )
                if not pq_path or not os.path.exists(pq_path):
                    if not _sv:
                        self.log_msg(
                            "⚠️ Data Lake neural ausente. Gere/atualize neural_training_lake.parquet "
                            "(ex.: `python scripts/build_neural_training_lake.py` ou acumule replay_buffer)."
                        )
                    return

            if not _sv:
                self.log_msg(
                    "🧠 [SHADOW REPLAY] Data Lake → Reservas (peso extra em alvos extremos / stress)..."
                )

            df = pd.read_parquet(pq_path)
            if len(df) == 0:
                return

            _tail = int(self.cfg.get("shadow_lake_tail_rows", 0) or 0)
            if _tail > 0:
                df_recente = df.tail(max(1000, min(len(df), _tail)))
            else:
                df_recente = df
            if (
                "features" not in df_recente.columns
                or "target" not in df_recente.columns
            ):
                return

            dim_exp = int(self._neural_feature_dim())
            xs_lake, ys_lake = [], []
            for features_raw, target_raw in zip(
                df_recente["features"].values, df_recente["target"].values
            ):
                state_arr = self._rehydrate_replay_state(features_raw, dim_exp)
                if state_arr is None:
                    continue
                xs_lake.append(state_arr)
                ys_lake.append(target_raw)
            if not xs_lake:
                if not _sv:
                    self.log_msg(
                        f"⚠️ [SHADOW REPLAY] Lake sem amostras reidratáveis para {dim_exp}D. Pulando."
                    )
                return
            X = np.asarray(xs_lake, dtype=np.float32)
            y = np.asarray(ys_lake, dtype=np.float64).reshape(-1)
            if X.ndim != 3 or X.shape[-1] != dim_exp:
                if not _sv:
                    self.log_msg(
                        f"⚠️ [SHADOW REPLAY] Dimensão do lake ({X.shape}) ≠ modelo ({dim_exp}D). Pulando."
                    )
                return

            sample_w = np.ones(len(y), dtype=np.float64)
            extreme = (y <= 0.12) | (y >= 0.88)
            _ew = float(self.cfg.get("replay_parquet_extreme_weight", 12.0))
            sample_w[extreme] = _ew

            epochs_replay = max(3, int(self.cfg.get("replay_buffer_fit_epochs", 8) or 8))
            batch_sz = self._neural_batch_size(len(y))

            m_sn_r = getattr(self, "model_sniper_reserva", None)
            m_lat_r = getattr(self, "model_lateral_reserva", None)

            if m_sn_r is not None:
                if not _sv:
                    self.log_msg(
                        "🧠 [FORJA 1H] SNIPER_RESERVA (Transformer) — Experience Replay..."
                    )
                self._keras_fit(
                    m_sn_r,
                    X,
                    y,
                    sample_weight=sample_w,
                    epochs=epochs_replay,
                    batch_size=min(batch_sz, max(1, len(y))),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                if getattr(self, "arq_sniper_reserva", None):
                    m_sn_r.save(self.arq_sniper_reserva)

            if m_lat_r is not None:
                y_lat = np.where(y > 0.5, 1.0, -1.0)
                if not _sv:
                    self.log_msg(
                        "🧠 [FORJA 1H] LATERAL_RESERVA — Experience Replay (rótulo ±1)..."
                    )
                self._keras_fit(
                    m_lat_r,
                    X,
                    y_lat,
                    sample_weight=sample_w,
                    epochs=epochs_replay,
                    batch_size=min(batch_sz, max(1, len(y_lat))),
                    callbacks=self._neural_fit_callbacks(False),
                    verbose=0,
                )
                if getattr(self, "arq_lateral_reserva", None):
                    m_lat_r.save(self.arq_lateral_reserva)

            try:
                if getattr(self, "arq_sniper_reserva", None) and os.path.exists(
                    self.arq_sniper_reserva
                ):
                    self.model_sniper_reserva = models.load_model(
                        self.arq_sniper_reserva,
                        compile=False,
                        custom_objects=keras_custom_objects(),
                    )
                    self._recompile_loaded_gladiador(
                        self.model_sniper_reserva, "sigmoid"
                    )
                if getattr(self, "arq_lateral_reserva", None) and os.path.exists(
                    self.arq_lateral_reserva
                ):
                    self.model_lateral_reserva = models.load_model(
                        self.arq_lateral_reserva,
                        compile=False,
                        custom_objects=keras_custom_objects(),
                    )
                    self._recompile_loaded_gladiador(
                        self.model_lateral_reserva, self._lateral_neural_mode()
                    )
            except Exception as e:
                self.erro_msg(f"Recarga pós-replay reservas: {e}")

            if not _sv:
                self.log_msg(
                    "✅ [SHADOW REPLAY] Reservas atualizadas (titulares intactos)."
                )

        except Exception as e:
            self.erro_msg(f"❌ Erro Experience Replay reservas: {e}")

        finally:
            # [CORREÇÃO: DEREFERENCING BRUTAL]
            X = y = df_recente = m_sn_r = m_lat_r = sample_w = None
            gc.collect()
            if 'K' in globals() and K is not None:
                K.clear_session()

    def loop_aprendizado_continuo(self):
        while getattr(self, "is_app_alive", True):
            for _ in range(3600):
                if not getattr(self, "is_app_alive", True):
                    return
                time.sleep(1)
            if not getattr(self, "is_searching", False) or not self.ia_treinada:
                continue
            try:
                self.log_msg(
                    "🌌 [SHADOW RL] Replay buffer → RESERVAS (peso maior em perdas)..."
                )
                if getattr(self, "arquivo_db_memoria", None) and os.path.exists(
                    self.arquivo_db_memoria
                ):
                    import json

                    shape_dim = int(self._neural_feature_dim())

                    def _read_rows(conn, agent_needle: str):
                        cols = [
                            c[1]
                            for c in conn.execute(
                                "PRAGMA table_info(replay_buffer)"
                            ).fetchall()
                        ]
                        if "reward" not in cols:
                            conn.execute(
                                "ALTER TABLE replay_buffer ADD COLUMN reward REAL"
                            )
                        if "error_flag" not in cols:
                            conn.execute(
                                "ALTER TABLE replay_buffer ADD COLUMN error_flag INTEGER DEFAULT 0"
                            )
                        if "agent_id" not in cols:
                            conn.execute(
                                "ALTER TABLE replay_buffer ADD COLUMN agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'"
                            )

                        prioritized_rows = conn.execute(
                            """
                            SELECT id, state, action, reward
                            FROM replay_buffer
                            WHERE state IS NOT NULL AND action IS NOT NULL
                              AND agent_id = ?
                            ORDER BY
                                CASE WHEN reward IS NULL THEN 1 ELSE 0 END,
                                reward ASC
                            LIMIT 300
                            """,
                            (agent_needle,),
                        ).fetchall()

                        random_rows = conn.execute(
                            """
                            SELECT id, state, action, reward
                            FROM replay_buffer
                            WHERE state IS NOT NULL AND action IS NOT NULL
                              AND agent_id = ?
                            ORDER BY RANDOM()
                            LIMIT 200
                            """,
                            (agent_needle,),
                        ).fetchall()
                        return prioritized_rows, random_rows

                    def _collect_for_agent(agent_needle: str):
                        pr, rr = self._run_sqlite_with_retry(
                            self.arquivo_db_memoria,
                            lambda c: _read_rows(c, agent_needle),
                            max_attempts=6,
                            base_delay=0.1,
                            timeout=10.0,
                        )
                        rows_map = {}
                        for r in pr + rr:
                            rows_map[r[0]] = r
                        Xc, yc, wc = [], [], []
                        for _, state_json, action, reward_val in rows_map.values():
                            try:
                                state_arr = self._rehydrate_replay_state(
                                    state_json, shape_dim
                                )
                                if state_arr is None:
                                    continue
                                Xc.append(state_arr)
                                act_i = int(action) if action is not None else 0
                                yc.append(float(act_i))
                                w = 1.0
                                if reward_val is not None:
                                    w = self._sample_weight_replay_reward(
                                        float(reward_val)
                                    )
                                wc.append(w)
                            except Exception:
                                continue
                        return Xc, yc, wc

                    def _collect_lateral(agent_needle: str):
                        pr, rr = self._run_sqlite_with_retry(
                            self.arquivo_db_memoria,
                            lambda c: _read_rows(c, agent_needle),
                            max_attempts=6,
                            base_delay=0.1,
                            timeout=10.0,
                        )
                        rows_map = {}
                        for r in pr + rr:
                            rows_map[r[0]] = r
                        Xc, yc, wc = [], [], []
                        for _, state_json, action, reward_val in rows_map.values():
                            try:
                                state_arr = self._rehydrate_replay_state(
                                    state_json, shape_dim
                                )
                                if state_arr is None:
                                    continue
                                Xc.append(state_arr)
                                act_i = int(action) if action is not None else 0
                                yc.append(1.0 if act_i >= 1 else -1.0)
                                w = 1.0
                                if reward_val is not None:
                                    w = self._sample_weight_replay_reward(
                                        float(reward_val)
                                    )
                                wc.append(w)
                            except Exception:
                                continue
                        return Xc, yc, wc

                    m_sn_r = getattr(self, "model_sniper_reserva", None)
                    m_lat_r = getattr(self, "model_lateral_reserva", None)

                    _cl_min = max(8, int(self.cfg.get("continuous_learn_min_samples", 12) or 12))
                    _cl_ep = max(2, int(self.cfg.get("continuous_learn_epochs", 8) or 8))
                    _cl_bs = max(32, int(self.cfg.get("continuous_learn_batch_size", 256) or 256))

                    if m_sn_r is not None:
                        X_cont, y_cont, weights_cont = _collect_for_agent(
                            "ARENA_SNIPER_RESERVA"
                        )
                        if len(X_cont) > _cl_min:
                            X_arr = np.array(X_cont)
                            y_arr = np.array(y_cont)
                            w_arr = np.array(weights_cont)
                            self._keras_fit(
                                m_sn_r,
                                X_arr,
                                y_arr,
                                sample_weight=w_arr,
                                epochs=_cl_ep,
                                batch_size=min(_cl_bs, len(X_cont)),
                                callbacks=self._neural_fit_callbacks(False),
                                verbose=0,
                            )
                            if getattr(self, "arq_sniper_reserva", None):
                                m_sn_r.save(self.arq_sniper_reserva)
                            self.log_msg(
                                f"🌌 SNIPER_RESERVA: {len(X_cont)} amostras replay ({shape_dim}D)."
                            )

                    if m_lat_r is not None:
                        X_cont, y_cont, weights_cont = _collect_lateral(
                            "ARENA_LATERAL_RESERVA"
                        )
                        if len(X_cont) > _cl_min:
                            X_arr = np.array(X_cont)
                            y_arr = np.array(y_cont)
                            w_arr = np.array(weights_cont)
                            self._keras_fit(
                                m_lat_r,
                                X_arr,
                                y_arr,
                                sample_weight=w_arr,
                                epochs=_cl_ep,
                                batch_size=min(_cl_bs, len(X_cont)),
                                callbacks=self._neural_fit_callbacks(False),
                                verbose=0,
                            )
                            if getattr(self, "arq_lateral_reserva", None):
                                m_lat_r.save(self.arq_lateral_reserva)
                            self.log_msg(
                                f"🌌 LATERAL_RESERVA: {len(X_cont)} amostras replay ({shape_dim}D)."
                            )
            except Exception:
                logger.exception(
                    "continuous_learning_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {},
                )
