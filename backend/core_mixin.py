# CoreMixin auto-extracted
import asyncio
import base64
import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import urlencode

import aiohttp
from bybit_helpers import (BYBIT_REST_TIMEOUT_SEC,
                           build_exchange_info_from_instruments,
                           fetch_all_linear_instruments,
                           get_usdt_available_balance, linear_tickers_24h)
from config import *
from cryptography.fernet import Fernet
from lhn_log_sanitize import sanitize_log_message
from pybit.unified_trading import HTTP

try:
    from pybit.exceptions import FailedRequestError
except ImportError:
    FailedRequestError = Exception  # type: ignore

logger = logging.getLogger(__name__)

# Cofre neural — quota dura (~45 GB margem) + WORM (recompensas extremas)
_ZELADOR_INTERVAL_SEC = 3600.0
_ZELADOR_SQL_BATCH = 8000
_ZELADOR_PARQUET_TRIM_MAX = 24

# TensorFlow/Keras: uma única região crítica para fit/predict concorrentes
keras_model_lock = threading.Lock()

# Fonte única: config.ATIVOS_ELITE_PERMANENTE (alias legado para imports antigos)
ATIVOS_IMUTAVEIS = ATIVOS_ELITE_PERMANENTE

for _sym_elite in ATIVOS_ELITE_PERMANENTE:
    if _sym_elite not in NOMES_ATIVOS_MASTER:
        logger.warning(
            "ATIVOS_ELITE_PERMANENTE: %s ausente em NOMES_ATIVOS_MASTER — corrija config.py",
            _sym_elite,
        )


class CoreMixin:
    def __init__(self):
        pass  # super().__init__() removido para evitar recursão
        self.is_app_alive = True
        self.log_history = []
        self.news_history = []
        self.noticias_recentes = []
        self.historico_operacoes = []
        # Nexus HFT V90: Soft Cutoff de Sessão — epoch em RAM; SQLite (Deep Memory) intocável.
        self.session_start_epoch = 0.0
        self.operacoes_abertas = {}
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_profit_usd = 0.0
        self.total_loss_usd = 0.0
        self.saldo_atual = 1000.0
        # Separação explícita de saldos (simulação x real) para evitar amnésia ao alternar modos.
        self.saldo_simulacao = 1000.0
        self.saldo_real = 0.0
        self.kline_cache = {}
        self.ultima_vela_t = {}
        self._kline_ws_last_ts = {}
        self.historico_sinais_vela = []
        self.historico_recente = []
        self.limites_alavancagem = {}
        self.ativos_elite_top10 = list(ATIVOS_ELITE_PERMANENTE)
        self.tick_timestamps = {}
        # Anti-spam HFT: micro-cooldown (~5s) por ativo após VPIN/Sniper
        self.vpin_cooldown = {}

        # [FIX] Garantir nomes_ativos_master desde o início (evita tickers vazio e AttributeError)
        self.nomes_ativos_master = dict(NOMES_ATIVOS_MASTER)
        _elite = list(ATIVOS_ELITE_PERMANENTE)
        _elite_set = set(_elite)
        _rest_init = [
            k for k in self.nomes_ativos_master.keys() if k not in _elite_set
        ][:90]
        self.tickers = _elite + _rest_init

        self.limites_gerenciamento = {
            "margem_fixa": 10.0,
            "alavancagem": 20,
            "usar_kelly": True,
        }

        self._ops_lock = threading.RLock()
        self.websocket_online = True
        self.monitoramento_apenas_por_ws = False
        self._leverage_by_symbol = {}
        self._futures_exchange_info_cache = None
        self._futures_exchange_info_ts = 0.0
        self._futures_exchange_info_lock = threading.Lock()
        self.is_searching = False
        self._workspace_ok = False
        self._log_preflight_buffer = []
        self._saldo_inicial_sessao = None
        self._saldo_real_confirmado = False
        # Janela curta de proteção contra sobrescrita assíncrona após ajuste manual do saldo simulado.
        self._saldo_manual_override_until = 0.0

        # [FIX 13] Cache do singleton do cliente HTTP Bybit (pybit)
        self._client_cache = None
        self._client_key_cache = ("", "")
        self._client_lock = threading.Lock()
        self._client_init_lock = threading.Lock()

        # [FIX 3/5/6] Events para encerramento limpo
        self._stop_event_noticias = threading.Event()
        self._stop_event_leverage = threading.Event()
        self._stop_event_top_ativos = threading.Event()

        # Fila centralizada para I/O em disco (Headless)
        self._disk_io_queue = queue.Queue()
        threading.Thread(
            target=self._loop_disk_io, daemon=True, name="Disk-IO-Worker"
        ).start()
        # Pool global para tarefas background (evita vazamento por Thread() dispersa).
        self._bg_executor = ThreadPoolExecutor(
            max_workers=16, thread_name_prefix="LHN-BG"
        )

    def _loop_disk_io(self):
        """Loop dedicado para operações de I/O em disco."""
        while getattr(self, "is_app_alive", True):
            try:
                task = self._disk_io_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                task()
            except Exception as e:
                if hasattr(self, "erro_msg"):
                    self.erro_msg(f"⚠️ Falha em I/O assíncrono de disco: {e}")

    def enqueue_disk_io(self, task):
        if not hasattr(self, "_disk_io_queue") or self._disk_io_queue is None:
            task()
            return
        self._disk_io_queue.put(task)

    def _keras_locked_run(self, fn):
        with keras_model_lock:
            return fn()

    def _keras_fit(self, model, *args, **kwargs):
        if model is None:
            return None
        with keras_model_lock:
            return model.fit(*args, **kwargs)

    def _keras_predict(self, model, x, **kwargs):
        if model is None:
            return None
        with keras_model_lock:
            return model.predict(x, **kwargs)

    @staticmethod
    def _sqlite_locked_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "database is locked" in msg or "database table is locked" in msg

    def _run_sqlite_with_retry(
        self,
        db_path,
        worker_fn,
        max_attempts=5,
        base_delay=0.15,
        timeout=15.0,
    ):
        """Executa worker SQLite com retry/backoff para lock contention."""
        last_exc = None
        for attempt in range(max_attempts):
            conn = None
            try:
                conn = sqlite3.connect(db_path, timeout=timeout)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=10000;")
                result = worker_fn(conn)
                return result
            except Exception as exc:
                last_exc = exc
                if not self._sqlite_locked_error(exc) or attempt >= max_attempts - 1:
                    raise
                time.sleep(min(base_delay * (2**attempt), 2.0))
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
        if last_exc is not None:
            raise last_exc

    def enqueue_deep_memory_write(
        self,
        worker_fn,
        max_attempts=5,
        base_delay=0.15,
        timeout=15.0,
        db_path=None,
    ):
        """Fila única (disk worker) + retry para INSERT/UPDATE em LHN_DEEP_MEMORY.sqlite."""
        db_path = db_path or getattr(self, "arquivo_db_memoria", None)
        if not db_path or not os.path.isfile(db_path):
            return

        def _task():
            try:
                self._run_sqlite_with_retry(
                    db_path,
                    worker_fn,
                    max_attempts=max_attempts,
                    base_delay=base_delay,
                    timeout=timeout,
                )
            except Exception:
                logger.exception(
                    "deep_memory_write_failed | db=%s | ts=%s",
                    db_path,
                    int(time.time() * 1000),
                )

        self.enqueue_disk_io(_task)

    def _cfg_cofre_quota_bytes(self) -> int:
        c = getattr(self, "cfg", None) or {}
        try:
            gb = float(c.get("cofre_quota_gb", 45.0) or 45.0)
        except (TypeError, ValueError):
            gb = 45.0
        gb = max(5.0, min(gb, 49.5))
        return int(gb * (1024**3))

    def _cfg_cofre_worm_reward_abs(self) -> float:
        c = getattr(self, "cfg", None) or {}
        try:
            v = float(c.get("cofre_worm_reward_abs", 25.0) or 25.0)
        except (TypeError, ValueError):
            v = 25.0
        return max(1.0, v)

    def _cfg_cofre_neutral_reward_eps(self) -> float:
        c = getattr(self, "cfg", None) or {}
        try:
            v = float(c.get("cofre_neutral_reward_eps", 0.35) or 0.35)
        except (TypeError, ValueError):
            v = 0.35
        return max(0.01, min(v, 50.0))

    def _cfg_cofre_worm_profit_usd(self) -> float:
        c = getattr(self, "cfg", None) or {}
        try:
            v = float(c.get("cofre_worm_profit_usd", 15.0) or 15.0)
        except (TypeError, ValueError):
            v = 15.0
        return max(0.5, v)

    def _cofre_bytes_sqlite_bundle(self) -> int:
        """Soma .sqlite + -wal + -shm do Deep Memory."""
        base = getattr(self, "arquivo_db_memoria", None) or ""
        if not base:
            return 0
        total = 0
        for p in (base, base + "-wal", base + "-shm"):
            if os.path.isfile(p):
                try:
                    total += os.path.getsize(p)
                except OSError:
                    pass
        return total

    def _cofre_bytes_lhn_datalake_parquet(self) -> int:
        dl = getattr(self, "path_lhn_datalake", None) or ""
        if not dl or not os.path.isdir(dl):
            return 0
        total = 0
        try:
            for name in os.listdir(dl):
                if not name.lower().endswith(".parquet"):
                    continue
                fp = os.path.join(dl, name)
                if os.path.isfile(fp):
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
        except OSError:
            return 0
        return total

    def _cofre_bytes_total_uso(self) -> int:
        return self._cofre_bytes_sqlite_bundle() + self._cofre_bytes_lhn_datalake_parquet()

    def _zelador_disk_work(self) -> None:
        """
        Executa na fila Disk-IO (serializado com outros writes): expurgo WORM + checkpoint + VACUUM opcional.
        Não invoca Keras nem engine.
        """
        db_path = getattr(self, "arquivo_db_memoria", None)
        if not db_path or not os.path.isfile(db_path):
            return
        quota = self._cfg_cofre_quota_bytes()
        worm_r = self._cfg_cofre_worm_reward_abs()
        eps = self._cfg_cofre_neutral_reward_eps()
        worm_p = self._cfg_cofre_worm_profit_usd()
        max_rounds = 32
        round_n = 0
        stagnant_rounds = 0
        while (
            round_n < max_rounds
            and self._cofre_bytes_total_uso() > quota
            and getattr(self, "is_app_alive", True)
        ):
            round_n += 1
            conn = None
            try:
                conn = sqlite3.connect(db_path, timeout=120.0)
                ch0 = conn.total_changes
                conn.execute("PRAGMA busy_timeout=60000")
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                # 1) Logs do guardião (não treino pesado)
                conn.execute(
                    f"""
                    DELETE FROM guardiao_shadow_log
                    WHERE id IN (
                        SELECT id FROM guardiao_shadow_log
                        ORDER BY id ASC
                        LIMIT {_ZELADOR_SQL_BATCH}
                    )
                    """
                )
                # 2) Histórico de operações: apenas fechos "neutros" (PnL pequeno)
                conn.execute(
                    """
                    DELETE FROM historico_operacoes
                    WHERE id IN (
                        SELECT id FROM historico_operacoes
                        WHERE ABS(COALESCE(profit_usd, 0.0)) < ?
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (worm_p, _ZELADOR_SQL_BATCH),
                )
                # 3) replay_buffer: remove neutros; preserva |reward| grande (WORM)
                conn.execute(
                    """
                    DELETE FROM replay_buffer
                    WHERE rowid IN (
                        SELECT rowid FROM replay_buffer
                        WHERE COALESCE(agent_id, '') NOT LIKE 'GUARDIAN%'
                          AND ABS(COALESCE(reward, 0.0)) < ?
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (worm_r, _ZELADOR_SQL_BATCH),
                )
                # 4) punit_memory: neutros apenas (magnitude baixa)
                conn.execute(
                    """
                    DELETE FROM punit_memory
                    WHERE rowid IN (
                        SELECT rowid FROM punit_memory
                        WHERE ABS(COALESCE(reward, 0.0)) < ?
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (eps, _ZELADOR_SQL_BATCH),
                )
                # 5) llm_context_logs (texto grande) — mais antigos primeiro
                try:
                    conn.execute(
                        f"""
                        DELETE FROM llm_context_logs
                        WHERE id IN (
                            SELECT id FROM llm_context_logs
                            ORDER BY id ASC
                            LIMIT {_ZELADOR_SQL_BATCH // 2}
                        )
                        """
                    )
                except sqlite3.Error:
                    pass
                conn.commit()
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                if conn.total_changes == ch0:
                    stagnant_rounds += 1
                else:
                    stagnant_rounds = 0
            except Exception as e:
                logger.exception("zelador_sql_round_failed | round=%s | err=%s", round_n, e)
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

            # 6) Parquet: aparar klines antigos (nunca o lake neural principal nesta ronda)
            if self._cofre_bytes_total_uso() > quota:
                self._zelador_trim_parquet_klines()

            if stagnant_rounds >= 3 and self._cofre_bytes_total_uso() > quota:
                break

        if round_n == 0:
            return

        busy = bool(getattr(self, "estrategia_rodando", False))
        cx = None
        try:
            cx = sqlite3.connect(db_path, timeout=180.0)
            cx.execute("PRAGMA busy_timeout=120000")
            if busy:
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "🧹 [ZELADOR] Motor ativo — incremental_vacuum (sem VACUUM total)."
                    )
                for _ in range(12):
                    try:
                        cx.execute("PRAGMA incremental_vacuum(5000)")
                    except sqlite3.Error:
                        break
            else:
                cx.execute("VACUUM")
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "🧹 [ZELADOR] VACUUM concluído — espaço devolvido ao SO."
                    )
        except Exception as e:
            logger.exception("zelador_vacuum_failed | err=%s", e)
            if hasattr(self, "erro_msg"):
                self.erro_msg(f"⚠️ [ZELADOR] VACUUM falhou (não bloqueante): {e}")
        finally:
            if cx is not None:
                try:
                    cx.close()
                except Exception:
                    pass

    def _zelador_trim_parquet_klines(self) -> None:
        """Remove ficheiros klines_*.parquet mais antigos (não apaga neural_training_lake)."""
        dl = getattr(self, "path_lhn_datalake", None) or ""
        if not dl or not os.path.isdir(dl):
            return
        try:
            rows = []
            for name in os.listdir(dl):
                if not name.lower().endswith(".parquet"):
                    continue
                if name.lower().startswith("neural_training"):
                    continue
                if not name.lower().startswith("klines_"):
                    continue
                fp = os.path.join(dl, name)
                if os.path.isfile(fp):
                    try:
                        rows.append((os.path.getmtime(fp), fp))
                    except OSError:
                        pass
            rows.sort(key=lambda x: x[0])
            for _, fp in rows[:_ZELADOR_PARQUET_TRIM_MAX]:
                try:
                    os.remove(fp)
                    if hasattr(self, "log_msg"):
                        self.log_msg(f"🧹 [ZELADOR] Parquet aparado: {os.path.basename(fp)}")
                except OSError as e:
                    logger.warning("zelador_parquet_remove_failed | path=%s | err=%s", fp, e)
        except OSError:
            pass

    def loop_zelador_cofre_neural(self) -> None:
        """
        Thread de fundo: a cada hora mede SQLite+WAL+SHM + Parquet datalake.
        Acima da quota (45 GB default) enfileira expurgo WORM na fila Disk-IO.
        """
        time.sleep(120.0)
        while getattr(self, "is_app_alive", True):
            try:
                used = self._cofre_bytes_total_uso()
                cap = self._cfg_cofre_quota_bytes()
                if used > cap:
                    gb_u, gb_c = used / (1024**3), cap / (1024**3)
                    if hasattr(self, "log_msg"):
                        self.log_msg(
                            f"🧹 [ZELADOR] Quota excedida ({gb_u:.2f} GB > {gb_c:.2f} GB) — "
                            "expurgo WORM (fila Disk-IO)…"
                        )

                    def _run():
                        try:
                            self._zelador_disk_work()
                        except Exception:
                            logger.exception("zelador_disk_work_outer")

                    self.enqueue_disk_io(_run)
            except Exception:
                logger.exception("loop_zelador_cofre_neural_tick_failed")
            time.sleep(_ZELADOR_INTERVAL_SEC)

    def submit_background_task(self, task, *args, **kwargs):
        """Despacha tarefa não bloqueante no pool global com fallback seguro."""
        executor = getattr(self, "_bg_executor", None)
        if executor is None:
            threading.Thread(target=task, args=args, kwargs=kwargs, daemon=True).start()
            return None
        try:
            return executor.submit(task, *args, **kwargs)
        except Exception:
            threading.Thread(target=task, args=args, kwargs=kwargs, daemon=True).start()
            return None

    def loop_atualizar_top_ativos(self):
        """Top 100: posições 0–9 = ATIVOS_ELITE_PERMANENTE; 10–99 = maior volume (excl. elite). Elite nunca rotaciona."""
        while getattr(self, "is_app_alive", True):
            try:
                client = self.get_bybit_client()
                # Estatísticas 24h (linear USDT) — turnover24h como quote volume
                tickers_24h = linear_tickers_24h(client)

                # Filtra apenas pares USDT e que tenham quoteVolume
                pares_validos = []
                for t in tickers_24h:
                    symbol = t["symbol"]
                    if symbol.endswith("USDT") and not any(
                        x in symbol for x in ["_", "-"]
                    ):
                        pares_validos.append(
                            {"symbol": symbol, "volume": float(t["quoteVolume"])}
                        )

                # Ordena por volume decrescente
                ranking = sorted(pares_validos, key=lambda x: x["volume"], reverse=True)

                _elite_set = set(ATIVOS_ELITE_PERMANENTE)
                outros_por_volume = [
                    r["symbol"] for r in ranking if r["symbol"] not in _elite_set
                ]
                # Única fonte para 0–9: ATIVOS_ELITE_PERMANENTE; 10–99: volume (fora da elite)
                tickers_antes = list(getattr(self, "tickers", []) or [])
                self.tickers = list(ATIVOS_ELITE_PERMANENTE) + outros_por_volume[:90]

                if not self.tickers:
                    time.sleep(60)
                    continue

                self.ativos_elite_top10 = list(ATIVOS_ELITE_PERMANENTE)

                for s in list(self.tickers):
                    if s not in self.nomes_ativos_master:
                        base = s.replace("USDT", "")
                        self.nomes_ativos_master[s] = base

                mudanca = len(
                    set(self.tickers).symmetric_difference(set(tickers_antes))
                )
                if mudanca > 5 or not tickers_antes:
                    self.log_msg(
                        f"🔄 Rotação de Ativos: Δ≈{mudanca} no Top 100. "
                        f"Elite permanente (10) + Top 90 por volume."
                    )
                    if getattr(self, "is_searching", False):
                        self.reiniciar_websocket()

            except Exception as e:
                self.erro_msg(f"Erro ao atualizar ranking de ativos: {e}")

            # [FIX 11] Espera eficiente de 4h com Event (não consome CPU)
            if hasattr(self, "_stop_event_top_ativos"):
                self._stop_event_top_ativos.wait(timeout=14400)
                self._stop_event_top_ativos.clear()
            else:
                time.sleep(14400)

    def escolher_diretorio(self):
        """[V90 FINAL HEADLESS] Não suportado sem Tkinter. Workspace é configurado automaticamente."""
        self.log_msg(
            "⚠️ escolher_diretorio() não disponível no modo Headless V90 FINAL. Use configurar_workspace_autonomo()."
        )

    def _reconcile_neural_feature_stack(self):
        """MTF (58D) e microestrutura institucional (32D) não se combinam na extração atual.
        Se ambos True no JSON/UI, desliga MTF para que os +6 dims institucionais entrem no vetor.
        Para treinar só 58D MTF, desligue use_institutional_microstructure no painel."""
        if not getattr(self, "cfg", None):
            return
        inst = bool(self.cfg.get("use_institutional_microstructure", False))
        mtf = bool(self.cfg.get("use_mtf_neural", False))
        if inst and mtf:
            self.cfg["use_mtf_neural"] = False
            if hasattr(self, "log_msg"):
                self.log_msg(
                    "⚙️ [NEURAL] use_institutional_microstructure + use_mtf_neural "
                    "→ MTF desligado (pipeline 32D institucional; para 58D MTF, desative a microestrutura)."
                )

    def inject_cfg_workspace_paths(self):
        """Sincroniza self.cfg com o workspace montado (absoluto). Evita chaves vazias após merge do JSON."""
        if not getattr(self, "cfg", None):
            return
        wr = getattr(self, "workspace_raiz", None)
        if not wr:
            return
        wr = os.path.abspath(str(wr).strip())
        self.cfg["workspace_raiz"] = wr
        self.cfg["workspace_root"] = wr
        pd = getattr(self, "path_dados", "") or ""
        pm = getattr(self, "path_modelos", "") or ""
        pl = getattr(self, "path_lhn_datalake", "") or ""
        self.cfg["path_dados"] = os.path.abspath(pd) if pd else ""
        self.cfg["path_modelos"] = os.path.abspath(pm) if pm else ""
        self.cfg["path_lhn_datalake"] = os.path.abspath(pl) if pl else ""

    def configurar_workspace_autonomo(self):
        self.api_key = "YOUR_API_KEY_HERE"
        self.api_secret = "YOUR_API_SECRET_HERE"
        self.modo_real = False

        dir_bot = os.path.join(os.path.dirname(__file__), "..", "Workspace_LHN")
        self.workspace_raiz = os.path.abspath(dir_bot)
        self.path_dados = os.path.abspath(os.path.join(self.workspace_raiz, "Dados"))
        self.path_demonstrativos = os.path.abspath(
            os.path.join(self.workspace_raiz, "Demonstrativos")
        )
        self.path_modelos = os.path.abspath(
            os.path.join(self.workspace_raiz, "modelos")
        )
        self.path_lhn_datalake = os.path.abspath(
            os.path.join(self.workspace_raiz, "lhn_datalake")
        )

        for p in [
            self.workspace_raiz,
            self.path_dados,
            self.path_demonstrativos,
            self.path_modelos,
            self.path_lhn_datalake,
        ]:
            os.makedirs(p, exist_ok=True)

        self.arquivo_historico = os.path.abspath(
            os.path.join(self.path_demonstrativos, "LHN_HISTORICO_FINALIZADAS.txt")
        )
        self.arquivo_historico_json = os.path.abspath(
            os.path.join(self.path_dados, "LHN_HISTORICO_FINALIZADAS.json")
        )
        self.arquivo_sinais_historico = os.path.abspath(
            os.path.join(self.path_dados, "LHN_SINAIS_HISTORICO.json")
        )
        self.arquivo_saldo_simulacao = os.path.abspath(
            os.path.join(self.path_dados, "LHN_SALDO_SIM.txt")
        )
        self.arquivo_cerebro = os.path.abspath(
            os.path.join(self.path_dados, "LHN_IA_DEEP_QUANT_V90 FINAL.keras")
        )
        self.arquivo_config = os.path.abspath(
            os.path.join(self.path_dados, "LHN_API_CONFIG.json")
        )
        self.arquivo_config_master = os.path.abspath(
            os.path.join(self.path_dados, "LHN_CONFIG_MASTER.json")
        )
        self.arquivo_api_keys = os.path.abspath(
            os.path.join(self.path_dados, "LHN_API_LOCK.enc")
        )
        # [FIX] Inicializar caminho do Deep Memory DB para evitar AttributeError em modos que usam SQLite
        self.arquivo_db_memoria = os.path.abspath(
            os.path.join(self.path_dados, "LHN_DEEP_MEMORY.sqlite")
        )
        # [V90] Sandbox de checkpointing: persiste posições abertas entre reinicializações
        self.arquivo_sandbox = os.path.abspath(
            os.path.join(self.path_dados, "LHN_SANDBOX_STATE.json")
        )
        if hasattr(self, "cfg") and self.cfg is not None:
            self.cfg["arquivo_sandbox"] = self.arquivo_sandbox

        self.carregar_configuracoes_conta()
        self.saldo_atual = self.carregar_saldo()
        if self.is_real_account_mode():
            self.saldo_real = float(self.saldo_atual)
            self.saldo_simulacao = self._carregar_saldo_simulacao_local()
        else:
            self.saldo_simulacao = float(self.saldo_atual)
        self.carregar_historico_stats()
        self.carregar_historico_sinais()
        # [FIX] Montar imediatamente o Hipocampo SSD quando em modo Headless
        try:
            self.init_deep_memory_db()
        except Exception:
            pass

        if os.path.exists(self.arquivo_config_master):
            try:
                with open(self.arquivo_config_master, "r", encoding="utf-8") as f:
                    saved_cfg = json.load(f)
                    self.cfg.update(saved_cfg)
                    # JSON antigo pode zerar workspace_* — realinhar com os caminhos reais no disco
                    self.inject_cfg_workspace_paths()
                    if "tickers_salvos" in saved_cfg:
                        self.tickers = saved_cfg["tickers_salvos"]
                        self.reiniciar_websocket()
                self.log_msg(
                    "⚙️ Sandbox Quantitativo carregado do Workspace Automático."
                )
            except Exception:
                logger.exception(
                    "workspace_master_config_load_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": self.arquivo_config_master},
                )

        self._workspace_ok = True
        self.inject_cfg_workspace_paths()
        self._reconcile_neural_feature_stack()

        # [V90] Reconciliação de Estado: re-absorve posições abertas após reinicialização
        try:
            self.reconciliar_estado_boot()
        except Exception as e:
            logger.warning("reconciliar_estado_boot falhou (não bloqueia boot): %s", e)

        if getattr(self, "_log_preflight_buffer", None):
            if not hasattr(self, "log_history"):
                self.log_history = []
            self.log_history.extend(self._log_preflight_buffer)
            if len(self.log_history) > 200:
                self.log_history = self.log_history[-200:]
            self._log_preflight_buffer.clear()
        self.log_msg(
            f"📁 Workspace montado: {self.workspace_raiz} (modelos + lhn_datalake + Dados)."
        )

    def reconciliar_estado_boot(self):
        """
        [V90] Executa ao boot para re-absorver posições abertas que sobreviveram a um
        desligamento abrupto. Modo Real: lê /v5/position/list da Bybit.
        Modo Simulação: lê o arquivo LHN_SANDBOX_STATE.json (se existir).
        NUNCA bloqueia o boot — todos os erros são capturados silenciosamente.
        """
        try:
            modo_real = getattr(self, "modo_real", False)
            sandbox_path = getattr(self, "arquivo_sandbox", None)

            if modo_real:
                # ── Modo Real: reconciliar com posições abertas na Bybit ────────
                try:
                    client = self.get_bybit_client()
                    if not client:
                        return
                    resp = client.get_positions(category="linear", settleCoin="USDT")
                    posicoes_bybit = (resp or {}).get("result", {}).get(
                        "list", []
                    ) or []

                    reabsorvidas = 0
                    with self._ops_lock:
                        for pos in posicoes_bybit:
                            size = float(pos.get("size", 0) or 0)
                            if size <= 0:
                                continue  # posição zerada / não activa
                            sym = str(pos.get("symbol", "")).strip().upper()
                            if not sym or sym in self.operacoes_abertas:
                                continue  # já rastreada

                            side = str(pos.get("side", "")).upper()
                            tipo = "LONG" if side == "BUY" else "SHORT"
                            entry = float(
                                pos.get("avgPrice", 0) or pos.get("entryPrice", 0) or 0
                            )
                            margin = float(
                                pos.get("positionIM", 0)
                                or pos.get("positionMM", 0)
                                or 0
                            )
                            lev = int(float(pos.get("leverage", 1) or 1))
                            unreal = float(pos.get("unrealisedPnl", 0) or 0)
                            sl_v = float(pos.get("stopLoss", 0) or 0)
                            tp_v = float(pos.get("takeProfit", 0) or 0)

                            self.operacoes_abertas[sym] = {
                                "tipo": tipo,
                                "preco": entry,
                                "preco_entrada": entry,
                                "margem": margin,
                                "alavancagem": lev,
                                "qtd": str(size),
                                "qty_real": size,
                                "sl": sl_v if sl_v > 0 else None,
                                "tp": tp_v if tp_v > 0 else None,
                                "pnl_nao_realizado": unreal,
                                "ts_abertura": time.time(),
                                "timestamp": time.time() * 1000,
                                "origem": "RECONCILIACAO_BOOT",
                                "certeza": 50.0,
                            }
                            reabsorvidas += 1

                    if reabsorvidas > 0:
                        self.log_msg(
                            f"🔄 [RECONCILIAÇÃO] {reabsorvidas} posição(ões) REAL(IS) re-absorvida(s) da Bybit no boot."
                        )
                    else:
                        self.log_msg(
                            "✅ [RECONCILIAÇÃO] Nenhuma posição órfã detectada na Bybit."
                        )

                except Exception as e:
                    # Falha de rede ou API não deve impedir o bot de ligar
                    logger.warning("reconciliar_estado_boot (real) falhou: %s", e)
                    self.log_msg(
                        f"⚠️ [RECONCILIAÇÃO] Falha ao consultar Bybit no boot: {e}"
                    )

            else:
                # ── Modo Simulação: reconciliar com o arquivo sandbox ──────────
                if not sandbox_path or not os.path.exists(sandbox_path):
                    return
                try:
                    with open(sandbox_path, "r", encoding="utf-8") as f:
                        state = json.load(f)

                    ops_salvas = state.get("operacoes_abertas", {})
                    saldo_salvo = float(state.get("saldo", 0) or 0)
                    snap_ts = float(state.get("snapshot_ts", 0) or 0)

                    # Só restaurar se o snapshot for recente (<24h)
                    if time.time() - snap_ts > 86400:
                        self.log_msg(
                            "ℹ️ [RECONCILIAÇÃO] Snapshot de simulação >24h — ignorado (stale)."
                        )
                        return

                    reabsorvidas = 0
                    with self._ops_lock:
                        for sym, op in ops_salvas.items():
                            if sym not in self.operacoes_abertas:
                                self.operacoes_abertas[sym] = op
                                reabsorvidas += 1

                    if saldo_salvo > 0 and not getattr(self, "saldo_atual", 0):
                        self.saldo_simulacao = saldo_salvo
                        self.saldo_atual = saldo_salvo

                    if reabsorvidas > 0:
                        self.log_msg(
                            f"📦 [RECONCILIAÇÃO] {reabsorvidas} posição(ões) SIMULADA(S) restaurada(s) do snapshot."
                        )
                    else:
                        self.log_msg(
                            "✅ [RECONCILIAÇÃO] Sandbox SIML vazio ou já sincronizado."
                        )

                except Exception as e:
                    logger.warning(
                        "reconciliar_estado_boot (sim) sandbox read falhou: %s", e
                    )

        except Exception as e:
            # Barreira final: garantir que o boot não trava seja qual for o erro
            logger.warning("reconciliar_estado_boot: erro inesperado: %s", e)

    def _json_sanitize_cfg(self, obj):
        """Garante tipos nativos para json.dump (numpy, etc.)."""
        try:
            import numpy as np
        except ImportError:
            np = None  # type: ignore
        if np is not None:
            if isinstance(obj, np.generic):
                return obj.item()
            if isinstance(obj, np.ndarray):
                return obj.tolist()
        if isinstance(obj, dict):
            return {str(k): self._json_sanitize_cfg(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._json_sanitize_cfg(v) for v in obj]
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        if isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        return str(obj)

    def salvar_configuracoes_gerais(self):
        if not self.workspace_raiz:
            return
        try:
            self.cfg["tickers_salvos"] = self.tickers
            safe = self._json_sanitize_cfg(self.cfg)
            with open(self.arquivo_config_master, "w", encoding="utf-8") as f:
                json.dump(safe, f, indent=4, ensure_ascii=False)
            self.log_msg("💾 Sandbox Quantitativo salvo com sucesso.")
        except Exception as e:
            self.erro_msg(f"Falha ao salvar configurações gerais: {e}")

    def restaurar_padroes_fabrica(self):
        self.cfg.update(DEFAULT_CFG.copy())
        self.tickers = list(self.nomes_ativos_master.keys())
        self.reiniciar_websocket()
        self.salvar_configuracoes_gerais()
        self.log_msg("🔄 Parâmetros de Fábrica restaurados.")

    def get_hardware_key(self, prefix="LHN_SOVEREIGN_V1_FINAL_BINANCE"):
        """[V90 Pilar 3] Gera chave AES-256 estrita baseada APENAS em Variável de Ambiente."""
        master_key = os.environ.get("LHN_MASTER_KEY")
        if not master_key or len(master_key.strip()) < 16:
            msg_erro = "🛑 ERRO FATAL DE SEGURANÇA: 'LHN_MASTER_KEY' não definida ou com menos de 16 caracteres. O uso de MAC Address (uuid.getnode) foi expurgado na V90 para prevenir vazamentos do banco AES na nuvem. Defina a chave nas variáveis de ambiente do SO ou no contêiner Docker."
            if hasattr(self, "log_msg"):
                self.log_msg(msg_erro)
            raise RuntimeError(msg_erro)

        seed = f"{prefix}_{master_key.strip()}"
        key_sha = hashlib.sha256(seed.encode()).digest()
        return base64.urlsafe_b64encode(key_sha)

    def _apply_bybit_credentials_from_env(self) -> None:
        """Prioriza BYBIT_API_KEY / BYBIT_API_SECRET (ou LHN_BYBIT_*) sobre ficheiro cifrado."""
        ek = (
            os.environ.get("BYBIT_API_KEY") or os.environ.get("LHN_BYBIT_API_KEY") or ""
        ).strip()
        es = (
            os.environ.get("BYBIT_API_SECRET")
            or os.environ.get("LHN_BYBIT_API_SECRET")
            or ""
        ).strip()
        if not ek or not es:
            return
        if getattr(self, "api_key", "") == ek and getattr(self, "api_secret", "") == es:
            return
        self.api_key = ek
        self.api_secret = es
        if hasattr(self, "log_msg"):
            self.log_msg(
                "🔐 Credenciais Bybit carregadas de variáveis de ambiente (BYBIT_API_KEY / BYBIT_API_SECRET)."
            )
        self.invalidar_client_cache()

    def carregar_configuracoes_conta(self):
        try:
            from lhn_secrets_persist import load_lhn_dotenv

            load_lhn_dotenv(override=False)
        except Exception:
            logger.exception("load_lhn_dotenv_failed")

        if hasattr(self, "arquivo_api_keys") and os.path.exists(self.arquivo_api_keys):
            try:
                with open(self.arquivo_api_keys, "rb") as f:
                    token_enc = f.read()

                # 1. Tentar Decrypt com a Nova Chave Mestra
                chave_nova = self.get_hardware_key(
                    prefix="LHN_SOVEREIGN_V1_FINAL_BINANCE"
                )
                try:
                    fernet = Fernet(chave_nova)
                    json_dump = fernet.decrypt(token_enc).decode("utf-8")
                except Exception:
                    # 2. Tentar Decrypt com a Chave Legada (V87)
                    chave_legada = self.get_hardware_key(prefix="LHNSovereignV87")
                    fernet_legado = Fernet(chave_legada)
                    json_dump = fernet_legado.decrypt(token_enc).decode("utf-8")
                    if hasattr(self, "log_msg"):
                        self.log_msg(
                            "⚡ Migração AES: Chave V87 detectada. Atualizando para V1 Final..."
                        )
                    try:
                        dados = json.loads(json_dump)
                    except (json.JSONDecodeError, TypeError, ValueError) as decode_ex:
                        raise ValueError(
                            f"Falha ao ler JSON das credenciais AES legadas: {decode_ex}"
                        ) from decode_ex
                    if not isinstance(dados, dict):
                        raise ValueError(
                            "Falha ao ler JSON das credenciais AES legadas: payload inválido"
                        )
                    self.api_key = dados.get("api_key", "")
                    self.api_secret = dados.get("api_secret", "")
                    self.modo_real = dados.get("modo_real", False)
                    self.salvar_configuracoes_conta()
                    self.invalidar_client_cache()
                    return

                try:
                    dados = json.loads(json_dump)
                except (json.JSONDecodeError, TypeError, ValueError) as decode_ex:
                    raise ValueError(
                        f"Falha ao ler JSON das credenciais AES: {decode_ex}"
                    ) from decode_ex
                if not isinstance(dados, dict):
                    raise ValueError(
                        "Falha ao ler JSON das credenciais AES: payload inválido"
                    )

                self.api_key = dados.get("api_key", "")
                self.api_secret = dados.get("api_secret", "")
                self.modo_real = dados.get("modo_real", False)
                self.invalidar_client_cache()
            except Exception as e:
                self.erro_msg(f"Falha na descriptografia da chave AES: {e}")
                self.api_key = "YOUR_API_KEY_HERE"
                self.api_secret = "YOUR_API_SECRET_HERE"
                self.modo_real = False
                self.log_msg(
                    "🔑 Não foi possível descriptografar as credenciais atuais. Atualize as API Keys no painel de Settings para revalidar o acesso."
                )
            finally:
                self._apply_bybit_credentials_from_env()
        else:
            self._apply_bybit_credentials_from_env()

    def salvar_configuracoes_conta(self):
        if not hasattr(self, "arquivo_api_keys"):
            return
        try:
            chave_cripto = self.get_hardware_key()
            fernet = Fernet(chave_cripto)

            dados = {
                "api_key": self.api_key,
                "api_secret": self.api_secret,
                "modo_real": self.modo_real,
            }
            json_dump = json.dumps(dados).encode("utf-8")
            token_enc = fernet.encrypt(json_dump)

            with open(self.arquivo_api_keys, "wb") as f:
                f.write(token_enc)
            self.invalidar_client_cache()
            try:
                from lhn_secrets_persist import persist_bybit_credentials_dotenv

                if self.api_key and self.api_secret:
                    persist_bybit_credentials_dotenv(self.api_key, self.api_secret)
            except Exception:
                logger.exception("persist_dotenv_after_aes_save_failed")
        except Exception as e:
            self.erro_msg(f"Erro ao salvar configurações AES-256: {e}")

    def _fallback_saldo_simulacao(self) -> float:
        cfg = (
            getattr(self, "cfg", {})
            if isinstance(getattr(self, "cfg", {}), dict)
            else {}
        )
        try:
            val = float(cfg.get("saldo_simulacao_inicial", 1000.0))
            return max(0.0, val)
        except Exception:
            return 1000.0

    def _carregar_saldo_simulacao_local(self) -> float:
        if self.arquivo_saldo_simulacao and os.path.exists(
            self.arquivo_saldo_simulacao
        ):
            try:
                with open(self.arquivo_saldo_simulacao, "r", encoding="utf-8") as f:
                    return max(0.0, float(f.read().strip()))
            except Exception:
                logger.exception(
                    "saldo_file_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": self.arquivo_saldo_simulacao},
                )
        return self._fallback_saldo_simulacao()

    def _persistir_saldo_simulacao(self) -> None:
        if not self.arquivo_saldo_simulacao:
            return
        saldo_str = str(float(getattr(self, "saldo_simulacao", 0.0) or 0.0))

        def _write():
            with open(self.arquivo_saldo_simulacao, "w", encoding="utf-8") as f:
                f.write(saldo_str)

        self.enqueue_disk_io(_write)

    def _ajustar_saldo_pos_fechamento(self, lucro: float, is_real: bool) -> None:
        """Atualiza o saldo correto por ambiente sem perder estado da simulação."""
        lucro_f = float(lucro or 0.0)
        if is_real:
            self.saldo_real = (
                float(getattr(self, "saldo_real", self.saldo_atual) or 0.0) + lucro_f
            )
            self.saldo_atual = float(self.saldo_real)
        else:
            self.saldo_simulacao = (
                float(getattr(self, "saldo_simulacao", self.saldo_atual) or 0.0)
                + lucro_f
            )
            self.saldo_atual = float(self.saldo_simulacao)

    def _aplicar_modo_conta_no_saldo(self) -> float:
        """Sincroniza saldo_atual com o modo ativo sem sobrescrever o outro saldo."""
        if self.is_real_account_mode():
            self.saldo_atual = float(
                getattr(self, "saldo_real", self.saldo_atual) or 0.0
            )
        else:
            self.saldo_atual = float(
                getattr(self, "saldo_simulacao", self._fallback_saldo_simulacao())
                or 0.0
            )
        return float(self.saldo_atual)

    def carregar_saldo(self):
        if self.is_real_account_mode():
            try:
                client = self.get_bybit_client()
                saldo_real = float(get_usdt_available_balance(client))
                self.saldo_real = saldo_real
                self._saldo_real_confirmado = saldo_real > 0.0
                return saldo_real
            except FailedRequestError:
                self._saldo_real_confirmado = False
                return 0.0
            except Exception:
                self._saldo_real_confirmado = False
                return 0.0
        else:
            saldo_sim = self._carregar_saldo_simulacao_local()
            self.saldo_simulacao = saldo_sim
            return saldo_sim

    def salvar_saldo(self):
        # Persistência sempre do saldo de SIMULAÇÃO (nunca escrever saldo real no arquivo local).
        self._persistir_saldo_simulacao()

    def set_saldo_simulado_manual(self, novo_saldo: float):
        """Aplica saldo manual em simulação e protege contra sobrescrita assíncrona imediata."""
        saldo = float(novo_saldo)
        if saldo < 0:
            saldo = 0.0
        self.saldo_simulacao = saldo
        self.saldo_atual = saldo
        self._saldo_inicial_sessao = saldo
        if hasattr(self, "cfg") and isinstance(self.cfg, dict):
            self.cfg["saldo_simulacao_inicial"] = saldo
        self._saldo_manual_override_until = time.time() + 86400.0
        self.salvar_saldo()
        if hasattr(self, "salvar_configuracoes_gerais"):
            try:
                self.salvar_configuracoes_gerais()
            except Exception:
                pass

    def saldo_manual_override_ativo(self) -> bool:
        try:
            return (
                float(getattr(self, "_saldo_manual_override_until", 0.0) or 0.0)
                > time.time()
            )
        except Exception:
            return False

    def registrar_sinal_feed(
        self,
        par: str,
        acao: str,
        *,
        certeza: float = 0.0,
        preco_entrada: float = 0.0,
        fluxo=None,
        status: str = "ALERTA",
    ) -> None:
        """
        Acrescenta entrada ao buffer `sinais_historico` consumido por GET /api/signals
        (VPIN, alertas e qualquer evento que deva aparecer no painel sem exceção).
        """
        if not hasattr(self, "sinais_historico") or self.sinais_historico is None:
            self.sinais_historico = []
        sym = str(par or "").strip()
        act = str(acao or "").strip().upper()
        entry = {
            "id": int(time.time() * 1000) ^ (hash(sym) & 0xFFFF),
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "par": sym,
            "acao": act,
            "preco_entrada": float(preco_entrada or 0.0),
            "tp1": 0.0,
            "tp2": None,
            "tp3": None,
            "sl": 0.0,
            "certeza": float(certeza or 0.0),
            "fluxo": float(fluxo) if fluxo is not None else None,
            "status": status,
        }
        self.sinais_historico.append(entry)
        if len(self.sinais_historico) > 1000:
            self.sinais_historico = self.sinais_historico[-1000:]
        if hasattr(self, "persistir_historico_sinais"):
            try:
                self.persistir_historico_sinais()
            except Exception:
                logger.exception("signals_history_persist_failed")

    @staticmethod
    def _to_bool_strict(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("1", "true", "yes", "on", "sim"):
                return True
            if v in ("0", "false", "no", "off", "nao", "não", ""):
                return False
        return bool(value)

    def is_real_account_mode(self) -> bool:
        """
        Modo real quando o atributo `modo_real` está ativo.

        O cfg pode conter `conta_real: false` por legado (ex.: POST /api/saldo em simulação
        gravava isso) enquanto o usuário já ligou `modo_real` no painel — nesse caso o
        atributo `modo_real` prevalece sobre o flag stale no JSON.
        """
        modo_real_flag = self._to_bool_strict(getattr(self, "modo_real", False))
        cfg = (
            getattr(self, "cfg", {})
            if isinstance(getattr(self, "cfg", {}), dict)
            else {}
        )
        conta_real_cfg = cfg.get("conta_real", None)
        if conta_real_cfg is None:
            conta_real_cfg = cfg.get("modo_real", None)
        if conta_real_cfg is None:
            return modo_real_flag
        cr_ok = self._to_bool_strict(conta_real_cfg)
        if modo_real_flag and not cr_ok:
            return True
        return modo_real_flag and cr_ok

    def get_available_balance_usdt_for_orders(self) -> float:
        """
        Saldo USDT disponível para novas posições (margem isolada):
        - Conta real: saldo disponível reportado pela API Bybit.
        - Simulação: saldo simulado menos margem já alocada em operações abertas.
        """
        if self.is_real_account_mode():
            try:
                return float(get_usdt_available_balance(self.get_bybit_client()))
            except Exception:
                return max(0.0, float(getattr(self, "saldo_atual", 0.0)))
        with self._ops_lock:
            margem_alocada = sum(
                float(op.get("margem", 0.0) or 0.0)
                for op in getattr(self, "operacoes_abertas", {}).values()
                if not op.get("_pending")
            )
        saldo = float(
            self.saldo_real if self.is_real_account_mode() else self.saldo_simulacao
        )
        return max(0.0, saldo - margem_alocada)

    def ensure_isolated_margin_linear(self, symbol: str, leverage: int) -> bool:
        """
        Força modo de margem ISOLADA (tradeMode=1) no perpetual linear USDT.
        O bot não solicita Cross Margin (tradeMode=0).
        """
        if not self.is_real_account_mode():
            return True
        _cfg = getattr(self, "cfg", {}) or {}
        if isinstance(_cfg, dict) and not _cfg.get("force_isolated_margin", True):
            return True
        if not getattr(self, "api_key", None) or not getattr(self, "api_secret", None):
            return True
        sym = str(symbol or "").strip().upper()
        if not sym:
            return False
        lev = max(1, int(leverage))
        lev_s = str(lev)
        try:
            client = self.get_bybit_client()
            r = client.switch_margin_mode(
                category="linear",
                symbol=sym,
                tradeMode=1,
                buyLeverage=lev_s,
                sellLeverage=lev_s,
            )
            if isinstance(r, dict):
                if r.get("retCode") == 0:
                    return True
                msg = str(r.get("retMsg") or "")
                if (
                    "1100" in msg
                    or "not modified" in msg.lower()
                    or "already" in msg.lower()
                ):
                    return True
                self.erro_msg(f"⚠️ [MARGEM] Isolated {sym}: {r}")
                return False
            return True
        except Exception as e:
            es = str(e).lower()
            if "1100" in str(e) or "already" in es or "not need" in es:
                return True
            self.erro_msg(f"⚠️ [MARGEM] Falha ao forçar Isolated em {sym}: {e}")
            return False

    def get_bybit_client(self):
        """[FIX 13] Singleton thread-safe do cliente HTTP Bybit V5 (linear)."""
        self._apply_bybit_credentials_from_env()
        chave_atual = (getattr(self, "api_key", ""), getattr(self, "api_secret", ""))
        with self._client_lock:
            if self._client_cache is not None and self._client_key_cache == chave_atual:
                return self._client_cache
        with self._client_init_lock:
            with self._client_lock:
                if self._client_cache is None or self._client_key_cache != chave_atual:
                    # [FIX ErrCode 10002] recv_window no teto (60s) tolera drift de relógio local
                    # vs servidor Bybit; abaixo disso o pybit pode falhar ao alternar conta real.
                    if chave_atual[0] and chave_atual[1]:
                        self._client_cache = HTTP(
                            api_key=chave_atual[0],
                            api_secret=chave_atual[1],
                            testnet=False,
                            timeout=int(BYBIT_REST_TIMEOUT_SEC),
                            recv_window=60000,
                        )
                    else:
                        self._client_cache = HTTP(
                            testnet=False,
                            timeout=int(BYBIT_REST_TIMEOUT_SEC),
                            recv_window=60000,
                        )
                    self._client_key_cache = chave_atual
                return self._client_cache

    def get_binance_client(self):
        """Alias legado; a corretora ativa é Bybit Unified V5 (linear)."""
        return self.get_bybit_client()

    def get_futures_exchange_info_cached(self, client, ttl_sec=86400):
        """Cache de instrumentos linear (equivalente ao antigo exchange_info da Binance)."""
        now = time.time()
        with self._futures_exchange_info_lock:
            if (
                self._futures_exchange_info_cache is not None
                and (now - self._futures_exchange_info_ts) < ttl_sec
            ):
                return self._futures_exchange_info_cache
        try:
            rows = fetch_all_linear_instruments(client)
            info = build_exchange_info_from_instruments(rows)
            with self._futures_exchange_info_lock:
                self._futures_exchange_info_cache = info
                self._futures_exchange_info_ts = time.time()
            return info
        except Exception:
            return {"symbols": []}

    def invalidar_client_cache(self):
        with self._client_lock:
            self._client_cache = None

    def log_msg(self, msg):
        from datetime import datetime

        safe = sanitize_log_message(
            str(msg),
            getattr(self, "api_key", None),
            getattr(self, "api_secret", None),
        )
        hora = datetime.now().strftime("%H:%M:%S")
        str_log = f"[{hora}] {safe}"
        try:
            print(str_log)
        except UnicodeEncodeError:
            try:
                print(str_log.encode("ascii", "replace").decode("ascii"))
            except Exception:
                logger.exception(
                    "log_print_fallback_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"msg": str_log},
                )

        if not getattr(self, "_workspace_ok", False):
            if not hasattr(self, "_log_preflight_buffer"):
                self._log_preflight_buffer = []
            self._log_preflight_buffer.append(str_log)
            if len(self._log_preflight_buffer) > 500:
                self._log_preflight_buffer = self._log_preflight_buffer[-400:]
        else:
            if not hasattr(self, "log_history"):
                self.log_history = []
            self.log_history.append(str_log)
            if len(self.log_history) > 200:
                self.log_history.pop(0)

        if "ORDEM ABERTA" in msg.upper() and hasattr(self, "enviar_alerta_telegram"):
            try:
                partes = msg.upper().split(" EM ")
                if len(partes) > 1:
                    symbol = partes[1].split(" ")[0].replace("|", "").strip()
                    with self._ops_lock:
                        _raw = self.operacoes_abertas.get(symbol)
                        op = dict(_raw) if _raw else {}

                    if op:
                        _ar = str(op.get("arena_regime", "") or "").strip().upper()
                        # Só VIP para cérebro direcional; pares / regimes explícitos ficam fora.
                        if _ar and _ar not in ("SNIPER", "LATERAL"):
                            return
                        _sup_map = getattr(
                            self, "_telegram_vip_suppress_until_symbol", None
                        )
                        if not isinstance(_sup_map, dict):
                            _sup_map = {}
                            self._telegram_vip_suppress_until_symbol = _sup_map
                        if time.time() < float(_sup_map.get(symbol, 0.0) or 0.0):
                            # Pós-ejeção rápida (<60s): não reenviar VIP no re-prendimento imediato.
                            return
                        direcao = (
                            "🚀 LONG (COMPRA)"
                            if op.get("tipo", "") in ["LONG", "COMPRA"]
                            else "🩸 SHORT (VENDA)"
                        )
                        preco = float(op.get("preco", 0.0))
                        alavancagem = int(op.get("alav", 20))
                        certeza = float(op.get("certeza", 0.0))
                        sl_real = float(op.get("sl", 0.0))
                        tp_real = float(op.get("tp", 0.0))
                        if "LONG" in direcao:
                            tp1 = preco + ((tp_real - preco) * 0.33)
                            tp2 = tp_real
                            sl = sl_real
                        else:
                            tp1 = preco - ((preco - tp_real) * 0.33)
                            tp2 = tp_real
                            sl = sl_real

                        hora_str = datetime.now().strftime("%d/%m/%Y %H:%M")
                        hora_atual = datetime.now().strftime("%H:%M:%S")

                        sinal_vip = (
                            f"[{hora_str}] LHN Sovereign: ⚡ SINAL VIP EXCLUSIVO ⚡\n\n"
                            f"💎 Ativo: #{symbol}\n"
                            f"🧭 Direção: {direcao}\n"
                            f"🎯 Preço de Entrada: $ {preco:,.4f}\n\n"
                            f"💰 ALVOS DE LUCRO (Take Profit):\n"
                            f"• TP 1 (Seguro): $ {tp1:,.4f}\n"
                            f"• TP 2 (Alvo IA): $ {tp2:,.4f}\n"
                            f"• TP 3 (Moon): Livre / Trailing Stop\n\n"
                            f"🛑 DEFESA (Stop Loss): $ {sl:,.4f}\n\n"
                            f"⚙️ DADOS TÉCNICOS:\n"
                            f"• Alavancagem Max: {alavancagem}x (Isolada)\n"
                            f"• Certeza da IA: {certeza:.1f}%\n"
                            f"• Horário Local: {hora_atual}\n\n"
                            f"⚠️ Siga o gerenciamento de risco. Sugestão: 2% da banca."
                        )

                        lo = getattr(self, "loop", None)
                        if lo is not None:
                            try:
                                asyncio.run_coroutine_threadsafe(
                                    self.enviar_alerta_telegram(sinal_vip), lo
                                )
                            except Exception:
                                pass
            except Exception:
                pass

    def erro_msg(self, msg):
        from datetime import datetime

        safe = sanitize_log_message(
            str(msg),
            getattr(self, "api_key", None),
            getattr(self, "api_secret", None),
        )
        hora = datetime.now().strftime("%H:%M:%S")
        str_log = f"[{hora}] [ERRO] {safe}"
        try:
            print(str_log)
        except UnicodeEncodeError:
            try:
                print(str_log.encode("ascii", "replace").decode("ascii"))
            except Exception:
                logger.exception(
                    "erro_print_fallback_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"msg": str_log},
                )

        if not getattr(self, "_workspace_ok", False):
            if not hasattr(self, "_log_preflight_buffer"):
                self._log_preflight_buffer = []
            self._log_preflight_buffer.append(str_log)
            if len(self._log_preflight_buffer) > 500:
                self._log_preflight_buffer = self._log_preflight_buffer[-400:]
        else:
            if not hasattr(self, "log_history"):
                self.log_history = []
            self.log_history.append(str_log)
            if len(self.log_history) > 200:
                self.log_history.pop(0)

    def carregar_historico_stats(self):
        if not hasattr(self, "historico_operacoes"):
            self.historico_operacoes = []

        # [BUG FIX B6] Resetar contadores e lista antes de recarregar o ficheiro
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_profit_usd = 0.0
        self.total_loss_usd = 0.0
        self.historico_operacoes = []
        loaded_from_json = False
        if getattr(self, "arquivo_historico_json", None) and os.path.exists(
            self.arquivo_historico_json
        ):
            try:
                with open(self.arquivo_historico_json, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            self.historico_operacoes.append(item)
                    loaded_from_json = True
            except (json.JSONDecodeError, TypeError, ValueError):
                logger.exception(
                    "history_json_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": self.arquivo_historico_json},
                )
            except Exception:
                logger.exception(
                    "history_json_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": self.arquivo_historico_json},
                )

        if (
            not loaded_from_json
            and self.arquivo_historico
            and os.path.exists(self.arquivo_historico)
        ):
            try:
                try:
                    with open(self.arquivo_historico, "r", encoding="utf-8") as f:
                        linhas = f.readlines()
                except UnicodeDecodeError:
                    with open(self.arquivo_historico, "r", encoding="latin-1") as f:
                        linhas = f.readlines()
                for linha in linhas:
                    # Compatibilidade: aceita tanto "RESULTADO FINALIZADO: hh:mm:ss - ..."
                    # quanto o formato legado persistido em disco "hh:mm:ss - ..."
                    raw = str(linha or "").strip()
                    if not raw:
                        continue
                    if "Lucro:" not in raw:
                        continue
                    if raw.startswith("RESULTADO FINALIZADO:"):
                        raw = raw.replace("RESULTADO FINALIZADO:", "", 1).strip()
                    try:
                        partes = raw.split(" - ")
                        if len(partes) < 5:
                            continue
                        tag_hora = partes[0].strip()
                        ativo = partes[1].strip()
                        tipo = partes[2].strip()
                        resultado = partes[3].strip()
                        lucro_str = partes[4].replace("Lucro: ", "").strip()
                        lucro_val = float(lucro_str)

                        self.historico_operacoes.append(
                            {
                                "hora": tag_hora,
                                "ativo": ativo,
                                "tipo": tipo,
                                "resultado": resultado,
                                "lucro": lucro_val,
                                "pnl": lucro_val,
                                "pnl_pct": 0.0,
                                "profit": lucro_val,
                            }
                        )
                    except Exception:
                        logger.exception(
                            "history_line_parse_failed | ts=%s | ativo=%s | payload=%s",
                            int(time.time() * 1000),
                            "GLOBAL",
                            {"linha": linha},
                        )
                # Ficheiro vem cronológico (antigo → novo); inverter para coincidir com insert(0) do motor
                if self.historico_operacoes:
                    self.historico_operacoes.reverse()
            except Exception:
                logger.exception(
                    "history_file_read_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": self.arquivo_historico},
                )
        self._recalcular_desempenho_historico()
        if self.historico_operacoes and not loaded_from_json:
            try:
                self.persistir_historico_operacoes()
            except Exception:
                logger.exception("history_json_bootstrap_failed")

    def _recalcular_desempenho_historico(self):
        if not hasattr(self, "historico_operacoes") or self.historico_operacoes is None:
            self.historico_operacoes = []
        rows = []
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_profit_usd = 0.0
        self.total_loss_usd = 0.0
        self.historico_recente = []
        for item in list(self.historico_operacoes or []):
            if not isinstance(item, dict):
                continue
            row = dict(item)
            try:
                pnl = float(
                    row.get("pnl", row.get("profit", row.get("lucro", 0.0))) or 0.0
                )
            except (TypeError, ValueError):
                pnl = 0.0
            row["lucro"] = float(pnl)
            row["pnl"] = float(pnl)
            row["profit"] = float(pnl)
            try:
                row["pnl_pct"] = float(row.get("pnl_pct", 0.0) or 0.0)
            except (TypeError, ValueError):
                row["pnl_pct"] = 0.0
            rows.append(row)
            self.total_trades += 1
            if pnl > 0:
                self.total_wins += 1
                self.total_profit_usd += pnl
                self.historico_recente.append(1)
            else:
                self.total_losses += 1
                self.total_loss_usd += abs(pnl)
                self.historico_recente.append(0)
        if len(self.historico_recente) > 10:
            self.historico_recente = self.historico_recente[-10:]
        self.historico_operacoes = rows
        self.operacoes_finalizadas = list(rows)
        if hasattr(self, "_calcular_estatisticas"):
            try:
                self.desempenho_sessao = self._calcular_estatisticas()
            except Exception:
                self.desempenho_sessao = {
                    "total_trades": int(self.total_trades),
                    "wins": int(self.total_wins),
                    "losses": int(self.total_losses),
                    "winrate": 0.0,
                    "pnl_liquido": float(self.total_profit_usd - self.total_loss_usd),
                }
        try:
            saldo_base = float(getattr(self, "saldo_atual", 0.0) or 0.0)
            pnl_liquido = float(self.total_profit_usd - self.total_loss_usd)
            self._saldo_inicial_sessao = saldo_base - pnl_liquido
        except Exception:
            pass

    def persistir_historico_operacoes(self):
        path = getattr(self, "arquivo_historico_json", None)
        if not path:
            return
        snapshot = list(getattr(self, "historico_operacoes", []) or [])

        def _write():
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                safe = (
                    self._json_sanitize_cfg(snapshot)
                    if hasattr(self, "_json_sanitize_cfg")
                    else snapshot
                )
                tmp = f"{path}.tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(safe, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            except Exception:
                logger.exception(
                    "history_json_write_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": path},
                )

        if hasattr(self, "enqueue_disk_io"):
            self.enqueue_disk_io(_write)
        else:
            _write()

    def carregar_historico_sinais(self):
        self.sinais_historico = []
        path = getattr(self, "arquivo_sinais_historico", None)
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.sinais_historico = [x for x in data if isinstance(x, dict)]
        except (json.JSONDecodeError, TypeError, ValueError):
            logger.exception(
                "signals_json_read_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                "GLOBAL",
                {"arquivo": path},
            )
        except Exception:
            logger.exception(
                "signals_json_read_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                "GLOBAL",
                {"arquivo": path},
            )

    def persistir_historico_sinais(self):
        path = getattr(self, "arquivo_sinais_historico", None)
        if not path:
            return
        snapshot = list(getattr(self, "sinais_historico", []) or [])[-1000:]

        def _write():
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                safe = (
                    self._json_sanitize_cfg(snapshot)
                    if hasattr(self, "_json_sanitize_cfg")
                    else snapshot
                )
                tmp = f"{path}.tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(safe, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            except Exception:
                logger.exception(
                    "signals_json_write_failed | ts=%s | ativo=%s | payload=%s",
                    int(time.time() * 1000),
                    "GLOBAL",
                    {"arquivo": path},
                )

        if hasattr(self, "enqueue_disk_io"):
            self.enqueue_disk_io(_write)
        else:
            _write()

    def _historico_row_epoch_seconds(self, row: object) -> float:
        # Nexus HFT V90: Soft Cutoff de Sessão — instante de fecho (s) para filtro session_start_epoch.
        if not isinstance(row, dict):
            return 0.0
        for k in ("ts", "close_ts"):
            v = row.get(k)
            if v is None:
                continue
            try:
                f = float(v)
                return f / 1000.0 if f > 1e12 else f
            except (TypeError, ValueError):
                continue
        v = row.get("timestamp")
        if v is None:
            return 0.0
        try:
            if isinstance(v, (int, float)):
                f = float(v)
                return f / 1000.0 if f > 1e12 else f
            if isinstance(v, str) and v.strip():
                s = v.strip().replace("Z", "+00:00")
                try:
                    f = float(s)
                    return f / 1000.0 if f > 1e12 else f
                except ValueError:
                    pass
                try:
                    return float(datetime.fromisoformat(s).timestamp())
                except ValueError:
                    return 0.0
        except (TypeError, ValueError, OSError):
            return 0.0
        return 0.0

    def historico_filtrado_sessao(self, rows: list | None = None) -> list:
        # Nexus HFT V90: Soft Cutoff de Sessão — operações visíveis ao painel após ZERAR_STATS (SQLite intacto).
        src = list(rows if rows is not None else (getattr(self, "historico_operacoes", []) or []))
        ep = float(getattr(self, "session_start_epoch", 0.0) or 0.0)
        if ep <= 0.0:
            return src
        out: list = []
        for r in src:
            if not isinstance(r, dict):
                continue
            try:
                if self._historico_row_epoch_seconds(r) >= ep:
                    out.append(r)
            except Exception:
                continue
        return out

    def zerar_desempenho(self):
        """Reseta todos os contadores de desempenho (trades, wins, losses, PnL)."""
        # Nexus HFT V90: Soft Cutoff de Sessão — marco temporal; sem DELETE/TRUNCATE em historico_operacoes (SQLite).
        self.session_start_epoch = time.time()
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_profit_usd = 0.0
        self.total_loss_usd = 0.0
        # Nexus HFT V90: Soft Cutoff de Sessão — mantém Deep Memory em RAM; não esvaziar listas persistidas no cofre.
        self.operacoes_finalizadas = list(getattr(self, "historico_operacoes", []) or [])
        self.historico_recente = []
        self._saldo_inicial_sessao = self.saldo_atual
        self.log_history = []
        self.log_msg("🗑️ Contadores de desempenho zerados com sucesso.")

    def forcar_salvamento_dados(self):
        if not getattr(self, "workspace_raiz", None):
            return
        self.salvar_saldo()
        self.salvar_configuracoes_conta()
        if hasattr(self, "salvar_configuracoes_gerais"):
            try:
                self.salvar_configuracoes_gerais()
            except Exception:
                logger.exception("forcar_salvamento_sandbox_failed")
        if hasattr(self, "persistir_historico_operacoes"):
            try:
                self.persistir_historico_operacoes()
            except Exception:
                logger.exception("forcar_salvamento_historico_failed")

    async def _loop_noticias_cryptocompare_async(self):
        """
        Varredura NLP via CryptoCompare (JSON v2). Não altera execução de ordens.
        Intervalo fixo 300s (rate limit). Requisição isolada em try/except.
        """
        url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN"
        api_key = os.environ.get(
            "CRYPTOCOMPARE_API_KEY",
            "24d353412aa46233f1394099297d20d843c125ebda0ede5c5eb98aa8a4af1940",
        ).strip()
        headers = {"authorization": f"Apikey {api_key}"}

        euphoria_words = [
            "surge",
            "bull",
            "moon",
            "jump",
            "high",
            "adopt",
            "approve",
            "etf",
            "soar",
            "record",
            "buy",
            "long",
            "pump",
            "rally",
            "upgrade",
            "all-time",
            "target",
            "gain",
            "breakout",
            "lead",
        ]
        panic_words = [
            "crash",
            "dump",
            "hack",
            "scam",
            "sec",
            "sue",
            "ban",
            "drop",
            "plunge",
            "bear",
            "sell",
            "short",
            "liquidat",
            "drain",
            "fear",
            "lawsuit",
            "investigate",
            "down",
            "warning",
        ]

        timeout = aiohttp.ClientTimeout(total=25)
        while getattr(self, "is_app_alive", True):
            data = None
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            if hasattr(self, "log_msg"):
                                self.log_msg(
                                    f"⚠️ [NLP] CryptoCompare HTTP {resp.status} — mantém sentimento anterior. "
                                    f"ts={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                                )
                            await asyncio.sleep(300)
                            continue
                        try:
                            data = await resp.json(content_type=None)
                        except (
                            aiohttp.ContentTypeError,
                            json.JSONDecodeError,
                            TypeError,
                            ValueError,
                        ) as decode_ex:
                            if hasattr(self, "log_msg"):
                                self.log_msg(
                                    "[REST HTTP] Falha ao decodificar JSON: corretora retornou HTML"
                                )
                                self.log_msg(
                                    f"⚠️ [NLP] Payload inválido no CryptoCompare: {decode_ex!s} — mantém sentimento anterior. "
                                    f"ts={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                                )
                            data = {}
            except Exception as ex:
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        f"⚠️ [NLP] Falha na varredura CryptoCompare: {ex!s} — mantém sentimento anterior. "
                        f"ts={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )
                await asyncio.sleep(300)
                continue

            if isinstance(data, dict) and data.get("Data"):
                noticias_formatadas = []
                noticias = data.get("Data", [])
                
                for item in noticias:
                    if not isinstance(item, dict):
                        continue
                    title = item.get("title", "") or ""
                    si = item.get("source_info")
                    provider = si.get("name") if isinstance(si, dict) else str(item.get("source") or "CryptoCompare")

                    tsf = float(item.get("published_on") or 0)
                    if tsf > 1e12: tsf /= 1000.0
                    published_at = datetime.fromtimestamp(tsf).strftime("%H:%M") if tsf > 0 else ""

                    noticias_formatadas.append({"title": title, "provider": provider, "time": published_at})

                # [CORREÇÃO 2: BUFFER DE NLP EXPANDIDO P/ 100 NOTÍCIAS COM SOFT WEIGHTING]
                if getattr(self, "news_history", None) is None:
                    self.news_history = []
                
                # Funde as notícias novas na frente e remove itens redundantes pelo título
                buffer_completo = noticias_formatadas + self.news_history
                from collections import deque
                uniques = []
                seen = set()
                
                for n in buffer_completo:
                    if n["title"] not in seen:
                        seen.add(n["title"])
                        uniques.append(n)
                
                # Fatia o limite de rolagem estrito em 100 notícias
                self.news_history = list(deque(uniques, maxlen=100))
                # Mantém as primeiras 20 separadas pro Dashboard UI não enlouquecer
                self.noticias_recentes = self.news_history[:20]

                # Recálculo do Sentimento Macro com Média Ponderada Linear (Soft Weighting vs Lagging)
                N = len(self.news_history)
                score_ponderado = 0.0
                soma_pesos = 0.0
                
                for i, item in enumerate(self.news_history):
                    # Índice 0 (Mais recente) -> Peso = 2.0
                    # Índice N-1 (Mais antiga) -> Peso = 1.0
                    peso = 2.0 - (i / max(1, N - 1)) * 1.0
                    
                    item_score = 0.0
                    title_lower = item["title"].lower()
                    if any(w in title_lower for w in euphoria_words): item_score += 1.5
                    if any(w in title_lower for w in panic_words): item_score -= 1.5
                    
                    score_ponderado += item_score * peso
                    soma_pesos += peso
                
                # Média ponderada atenuada (-1.5 a +1.5 teóricos)
                media_ponderada = (score_ponderado / soma_pesos) if soma_pesos > 0 else 0.0
                
                # Projeta a média para a escala de densidade do motor original
                score_total = media_ponderada * 20.0

                self.nlp_score = max(-10.0, min(10.0, float(score_total)))
                self.pontuacao_sentimento_atual = float(self.nlp_score)
                self.log_msg(f"🧠 Sentimento NLP Ponderado (Lag-Free): {self.nlp_score:.2f} | Memória: {N} News")
                
                if hasattr(self, "request_ws_immediate_update"):
                    try:
                        self.request_ws_immediate_update()
                    except Exception:
                        pass
            else:
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        f"⚠️ [NLP] Resposta sem Data ou lista vazia — mantém sentimento anterior. "
                        f"ts={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )

            await asyncio.sleep(300)

    def loop_noticias_global(self):
        """Thread pool: executa o loop assíncrono CryptoCompare (aiohttp + sleep 300s)."""
        try:
            asyncio.run(self._loop_noticias_cryptocompare_async())
        except Exception:
            pass

    def init_deep_memory_db(self):
        """Inicializa o banco de dados SSD Deep Memory (SQLite)."""
        if not hasattr(self, "arquivo_db_memoria") or not self.arquivo_db_memoria:
            return
        try:
            # [E8] Context manager garante fechamento
            with sqlite3.connect(self.arquivo_db_memoria) as conn:
                cursor = conn.cursor()
                # [FIX 8] WAL Mode para concorrência sem lock
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                try:
                    cursor.execute("PRAGMA auto_vacuum=INCREMENTAL")
                except sqlite3.Error:
                    pass

                cursor.execute(
                    """
                CREATE TABLE IF NOT EXISTS replay_buffer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    ativo TEXT,
                    state BLOB,
                    action INTEGER,
                    reward REAL,
                    next_state BLOB,
                    done INTEGER,
                    agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'
                )
            """
                )
                # [V90 FINAL] Garantir coluna agent_id em bases legadas
                cols = [
                    c[1]
                    for c in cursor.execute(
                        "PRAGMA table_info(replay_buffer)"
                    ).fetchall()
                ]
                if "agent_id" not in cols:
                    cursor.execute(
                        "ALTER TABLE replay_buffer ADD COLUMN agent_id TEXT DEFAULT 'SNIPER_V90 FINAL'"
                    )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS llm_context_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        ativo TEXT,
                        narrativa_str TEXT,
                        prob_ia REAL
                    )
                """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS guardiao_shadow_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        ativo TEXT,
                        pnl_atual REAL,
                        vpin_atual REAL,
                        acao_sugerida TEXT,
                        motivo TEXT
                    )
                """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS historico_operacoes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        symbol TEXT,
                        type TEXT,
                        entry_price REAL,
                        exit_price REAL,
                        profit_usd REAL,
                        close_reason TEXT
                    )
                """
                )
                ho_cols = [
                    c[1]
                    for c in cursor.execute(
                        "PRAGMA table_info(historico_operacoes)"
                    ).fetchall()
                ]
                if "arena_regime" not in ho_cols:
                    cursor.execute(
                        "ALTER TABLE historico_operacoes ADD COLUMN arena_regime TEXT DEFAULT 'SNIPER'"
                    )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS punit_memory (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        ativo TEXT,
                        agent_id TEXT,
                        state TEXT,
                        action INTEGER,
                        reward REAL,
                        source TEXT DEFAULT 'punish'
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lhn_meta (
                        k TEXT PRIMARY KEY,
                        v TEXT
                    )
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS arena_reserva_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        regime TEXT,
                        ativo TEXT,
                        direcao TEXT,
                        preco_entrada REAL,
                        pnl_simulado REAL,
                        lucro_usd REAL,
                        shadow_uid TEXT UNIQUE,
                        status TEXT DEFAULT 'OPEN',
                        sl_price REAL,
                        tp_price REAL,
                        margem_sim REAL DEFAULT 10.0
                    )
                """
                )
                ar_cols = [
                    c[1]
                    for c in cursor.execute(
                        "PRAGMA table_info(arena_reserva_log)"
                    ).fetchall()
                ]
                for _col, _ddl in (
                    (
                        "shadow_uid",
                        "ALTER TABLE arena_reserva_log ADD COLUMN shadow_uid TEXT",
                    ),
                    (
                        "status",
                        "ALTER TABLE arena_reserva_log ADD COLUMN status TEXT DEFAULT 'OPEN'",
                    ),
                    (
                        "sl_price",
                        "ALTER TABLE arena_reserva_log ADD COLUMN sl_price REAL",
                    ),
                    (
                        "tp_price",
                        "ALTER TABLE arena_reserva_log ADD COLUMN tp_price REAL",
                    ),
                    (
                        "margem_sim",
                        "ALTER TABLE arena_reserva_log ADD COLUMN margem_sim REAL DEFAULT 10.0",
                    ),
                ):
                    if _col not in ar_cols:
                        try:
                            cursor.execute(_ddl)
                        except Exception:
                            pass
                conn.commit()

            self.log_msg(
                "🧠 SSD Deep Memory Iniciado. Cofre Neural de 50GB Armado "
                f"(zelador WORM: quota {self._cfg_cofre_quota_bytes() / (1024**3):.0f} GB)."
            )
            if not getattr(self, "_zelador_cofre_started", False):
                self._zelador_cofre_started = True
                self.submit_background_task(self.loop_zelador_cofre_neural)
        except Exception as e:
            self.erro_msg(f"Erro fatal ao montar Hipocampo SQLite: {e}")

    def bootstrap_memoria_persistida_sqlite(self):
        """Mescla `historico_operacoes` do SQLite na RAM e reidrata `replay_buffer` a partir de `punit_memory` (por id, sem duplicar a cada boot)."""
        n_sqlite_ops = 0
        n_punit_total = 0
        n_hydrated = 0
        db_path = getattr(self, "arquivo_db_memoria", None)
        if not db_path or not os.path.isfile(db_path):
            return 0, 0, 0
        try:
            with sqlite3.connect(db_path, timeout=15.0) as conn:
                conn.execute("PRAGMA busy_timeout=8000")
                try:
                    n_punit_total = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM punit_memory"
                        ).fetchone()[0]
                    )
                except Exception:
                    n_punit_total = 0

                cur = conn.execute(
                    """
                    SELECT timestamp, symbol, type, entry_price, exit_price,
                           profit_usd, close_reason, arena_regime
                    FROM historico_operacoes
                    ORDER BY id DESC
                    LIMIT 25000
                    """
                )
                sqlite_ops = []
                for row in cur.fetchall():
                    ts, sym, typ, ep, xp, profit, creason, reg = row
                    ts_str = str(ts or "")[:32]
                    pnl = float(profit or 0.0)
                    sqlite_ops.append(
                        {
                            "hora": ts_str,
                            "timestamp": ts_str,
                            "ativo": str(sym or ""),
                            "tipo": str(typ or ""),
                            "resultado": str(creason or ""),
                            "lucro": pnl,
                            "pnl": pnl,
                            "profit": pnl,
                            "pnl_pct": 0.0,
                            "arena_regime": str(reg or "SNIPER"),
                            "entry_price": float(ep or 0.0),
                            "exit_price": float(xp or 0.0),
                            "_fonte": "sqlite",
                        }
                    )
                n_sqlite_ops = len(sqlite_ops)
                if sqlite_ops:
                    ho = list(getattr(self, "historico_operacoes", []) or [])
                    seen = set()
                    for x in ho:
                        if not isinstance(x, dict):
                            continue
                        try:
                            pk = (
                                str(x.get("ativo", "")),
                                str(x.get("hora", x.get("timestamp", ""))),
                                round(
                                    float(
                                        x.get("pnl", x.get("lucro", x.get("profit", 0)))
                                        or 0.0
                                    ),
                                    6,
                                ),
                            )
                            seen.add(pk)
                        except (TypeError, ValueError):
                            continue
                    merged_head = []
                    for item in sqlite_ops:
                        try:
                            pk = (
                                str(item.get("ativo", "")),
                                str(item.get("hora", "")),
                                round(float(item.get("pnl", 0)), 6),
                            )
                        except (TypeError, ValueError):
                            continue
                        if pk in seen:
                            continue
                        seen.add(pk)
                        merged_head.append(item)
                    self.historico_operacoes = merged_head + ho
                    if len(self.historico_operacoes) > 8000:
                        self.historico_operacoes = self.historico_operacoes[:8000]
                    if hasattr(self, "_recalcular_desempenho_historico"):
                        self._recalcular_desempenho_historico()

                row_meta = conn.execute(
                    "SELECT v FROM lhn_meta WHERE k = ?",
                    ("punit_replay_upto_id",),
                ).fetchone()
                last_id = 0
                if row_meta and row_meta[0] is not None:
                    try:
                        last_id = int(str(row_meta[0]).strip())
                    except (TypeError, ValueError):
                        last_id = 0

                pm = conn.execute(
                    """
                    SELECT id, ativo, agent_id, state, action, reward
                    FROM punit_memory
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT 100000
                    """,
                    (last_id,),
                ).fetchall()

                max_h = last_id
                for rid, ativo, agent_id, state, action, reward in pm:
                    if state is None or not agent_id:
                        continue
                    st = state
                    if isinstance(st, bytes):
                        st = st.decode("utf-8", errors="ignore")
                    try:
                        rw = float(reward) if reward is not None else 0.0
                    except (TypeError, ValueError):
                        rw = 0.0
                    err_f = 2 if rw <= -0.25 else (1 if rw < 0 else 0)
                    conn.execute(
                        """
                        INSERT INTO replay_buffer (ativo, state, action, reward, error_flag, agent_id, done)
                        VALUES (?, ?, ?, ?, ?, ?, 1)
                        """,
                        (
                            str(ativo or ""),
                            st,
                            int(action) if action is not None else 0,
                            float(reward) if reward is not None else None,
                            err_f,
                            str(agent_id),
                        ),
                    )
                    n_hydrated += 1
                    max_h = max(max_h, int(rid))
                if max_h > last_id:
                    conn.execute(
                        "INSERT OR REPLACE INTO lhn_meta (k, v) VALUES (?, ?)",
                        ("punit_replay_upto_id", str(max_h)),
                    )
                conn.commit()
        except Exception:
            logger.exception("bootstrap_memoria_persistida_sqlite_failed")
            return n_sqlite_ops, n_punit_total, n_hydrated
        return n_sqlite_ops, n_punit_total, n_hydrated

    def espelhar_operacao_historico_sqlite(self, item: dict) -> None:
        """Grava fechamento na tabela `historico_operacoes` (SQLite) para persistência entre boots."""
        if not isinstance(item, dict):
            return
        db_path = getattr(self, "arquivo_db_memoria", None)
        if not db_path:
            return
        sym = str(item.get("ativo") or "").strip()
        if not sym:
            return
        try:
            typ = str(item.get("tipo") or "").strip()
            ep = float(
                item.get("preco_entrada")
                or item.get("entry_price")
                or item.get("entry")
                or 0.0
            )
            xp = float(
                item.get("close_price")
                or item.get("exit_price")
                or item.get("preco_saida")
                or 0.0
            )
            pnl = float(
                item.get("pnl")
                or item.get("lucro")
                or item.get("profit")
                or 0.0
            )
            reason = str(item.get("resultado") or "")[:512]
            reg = str(item.get("arena_regime") or "SNIPER")

            def _worker(conn):
                conn.execute("PRAGMA busy_timeout=6000")
                conn.execute(
                    """
                    INSERT INTO historico_operacoes (symbol, type, entry_price, exit_price, profit_usd, close_reason, arena_regime)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (sym, typ, ep, xp, pnl, reason, reg),
                )
                conn.commit()

            self.enqueue_deep_memory_write(
                _worker, max_attempts=6, base_delay=0.12, timeout=12.0
            )
        except Exception:
            logger.exception("espelhar_operacao_historico_sqlite_failed")

    def loop_leverage_brackets(self):
        """Nova Thread V87: Atualiza limites de alavancagem (Bybit linear) para Kelly Dinâmico."""
        while self.is_app_alive:
            try:
                client = self.get_bybit_client()
                if client:
                    rows = fetch_all_linear_instruments(client)
                    for inst in rows:
                        symbol = inst.get("symbol")
                        lf = inst.get("leverageFilter") or {}
                        mx = lf.get("maxLeverage") or "100"
                        try:
                            max_leverage = int(float(mx))
                        except (TypeError, ValueError):
                            max_leverage = 100
                        if symbol:
                            self.limites_alavancagem[symbol] = max_leverage
            except Exception:
                pass
            if hasattr(self, "_stop_event_leverage"):
                self._stop_event_leverage.wait(timeout=3600)
                self._stop_event_leverage.clear()
            else:
                time.sleep(3600)
