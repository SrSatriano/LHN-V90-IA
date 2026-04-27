import asyncio
import json
import logging
import math
import os
import sys
import threading

try:
    from lhn_secrets_persist import load_lhn_dotenv

    load_lhn_dotenv(override=False)
except Exception:
    pass
import time
import traceback
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import aiohttp
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from lhn_auth import (expected_api_key, verify_api_key,
                      websocket_subprotocol_token_ok)

# Configurar logging para evitar warnings do TensorFlow e PyBit (rate limit tratado via backoff)
logging.getLogger("tensorflow").setLevel(logging.ERROR)
logging.getLogger("pybit").setLevel(logging.CRITICAL)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

from ai_mixin import AIMixin
# Importando os Mixins (Componentes divididos)
from bybit_helpers import _call_with_retry
from core_mixin import CoreMixin
from engine_mixin import EngineMixin
from security_mixin import SecurityMixin
from config import REGIME_ADX_MIN
from services import NeuralNetworkPipeline, OrderService, RiskManager
from telegram_mixin import TelegramMixin

# Instância Global do Bot
lhn_bot = None
active_ws_clients = set()


class LHNSovereignV90Backend(
    CoreMixin, EngineMixin, AIMixin, SecurityMixin, TelegramMixin
):
    def __init__(self):
        CoreMixin.__init__(self)
        AIMixin.__init__(self)
        SecurityMixin.__init__(self)

        # Stub variables for isolated backend testing without full V87 payload
        self.precos_atuais = {"BTCUSDT": 0.0}
        self.precos_buffer = {}
        self.pontuacao_sentimento_atual = 5.0
        self.nlp_score = 5.0
        self.estrategia_rodando = False
        # Initialize self.cfg (formerly in bot.py)
        from config import DEFAULT_CFG

        self.cfg = DEFAULT_CFG.copy()
        self.cfg.update(
            {"escudo_dark": True, "idioma": "Português-Brasil", "tema_visual": "Dark"}
        )

        self.configurar_workspace_autonomo()
        self._sync_cfg_workspace_paths()
        self.risk_manager = RiskManager(self)
        self.order_service = OrderService(self)
        self.neural_pipeline = NeuralNetworkPipeline(self)

        # Iniciar blindagem C++ / Segurança nativa logo de cara
        if not hasattr(self, "shield_module"):
            self.shield_module = None
        try:
            if self.shield_module:
                self.blindar_janela_contra_espionagem()
                self.iniciar_sentinela_anti_hacker()
            if hasattr(self, "verificar_integridade_memoria"):
                self.verificar_integridade_memoria()
            if hasattr(self, "configurar_criptografia_chave"):
                self.configurar_criptografia_chave()
        except Exception as e:
            print(f"Erro ao inicializar segurança: {e}")
            # Continuar mesmo se segurança falhar

        self.inicializar_engine()
        if hasattr(self, "_bootstrap_precos_tickers_rest"):
            self._bootstrap_precos_tickers_rest()
        self.inicializar_ia()

    def inicializar_engine(self):
        """Prepara o motor: garante tickers válidos para WebSocket e radar."""
        if not getattr(self, "tickers", None) or len(self.tickers) == 0:
            self.tickers = list(getattr(self, "nomes_ativos_master", {}).keys())
            if self.tickers:
                self.log_msg(
                    "⚙️ Engine: tickers inicializados a partir do nomenclador master."
                )

    def inicializar_ia(self):
        """Preparação inicial da IA (caminhos, estado). O cérebro é ligado ao iniciar o motor."""
        # Garante que o estado da IA existe; ligar_cerebro_ia() é chamado ao dar START_ENGINE
        if not hasattr(self, "ia_treinada"):
            self.ia_treinada = False
        if not hasattr(self, "treinamento_concluido"):
            self.treinamento_concluido = bool(getattr(self, "ia_treinada", False))
        if not hasattr(self, "_loop_analise_started"):
            self._loop_analise_started = False

    def iniciar_servicos_background(self):
        """Inicia serviços em background com tratamento de erro"""
        try:
            self.loop = asyncio.new_event_loop()
            self.thread_async = threading.Thread(
                target=self._rodar_loop_async, daemon=True
            )
            self.thread_async.start()

            # Starts the Binance WebSocket Data Feed Streams inside the Engine Mixin
            if hasattr(self, "iniciar_motor_assincrono"):
                self.submit_background_task(self.iniciar_motor_assincrono)

            # [FIX 3] Inicia o loop de Notícias/NLP Global
            if hasattr(self, "loop_noticias_global"):
                self.submit_background_task(self.loop_noticias_global)

            # [FIX] Inicia o loop de atualização do Top 100 ativos (Binance) para popular tickers
            if hasattr(self, "loop_atualizar_top_ativos"):
                self.submit_background_task(self.loop_atualizar_top_ativos)

            if hasattr(self, "loop_leverage_brackets"):
                self.submit_background_task(self.loop_leverage_brackets)

            # V90 FINAL.4: Comitê Gestor Macro
            if hasattr(self, "loop_comite_gestor"):
                self.submit_background_task(self.loop_comite_gestor)

            # Watchdog: pulso do loop neural (evita congelamento silencioso)
            if hasattr(self, "_watchdog_loop_pulso_neural"):
                self.submit_background_task(self._watchdog_loop_pulso_neural)

            # V90 FINAL: Motor de Arbitragem Estatística (Pairs Trading)
            if hasattr(self, "loop_ia_arbitragem"):
                self.submit_background_task(self.loop_ia_arbitragem)

            # [FIX] Cold Boot / Standby: Inicia as threads mas mantém a execução PAUSADA
            self.estrategia_rodando = False
            self.is_searching = False
            self.log_msg(
                "🟡 Sistema em Standby. Aguardando comando de Ignição (Start) pelo Painel."
            )

            # As threads são lançadas para carregar o modelo na memória, mas os loops
            # (no engine e ai_mixin) vão iterar em sleep(1) até que is_searching vire True.
            if hasattr(self, "ligar_cerebro_ia"):
                self.submit_background_task(self.ligar_cerebro_ia)
            if hasattr(self, "iniciar_bot"):
                self.submit_background_task(self.iniciar_bot)

        except Exception as e:
            print(f"[ERRO] Falha ao iniciar serviços background: {e}")
            if hasattr(self, "log_msg"):
                self.log_msg(f"⚠️ Erro crítico ao iniciar serviços: {e}")

    def _rodar_loop_async(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _sync_cfg_workspace_paths(self):
        """Espelha caminhos absolutos do workspace em self.cfg após montagem."""
        self.inject_cfg_workspace_paths()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia ciclo de vida da aplicação com cleanup adequado"""
    global lhn_bot
    try:
        print("[+] Inicializando LHN Sovereign V90 FINAL Backend...")
        lhn_bot = LHNSovereignV90Backend()
        _server_loop = asyncio.get_running_loop()

        def _request_ws_immediate_update():
            try:
                asyncio.run_coroutine_threadsafe(
                    _broadcast_full_ws_status(lhn_bot), _server_loop
                )
            except Exception:
                pass

        # Hook consumido pelo motor (engine_mixin) após fechamento de ordem.
        setattr(lhn_bot, "request_ws_immediate_update", _request_ws_immediate_update)
        lhn_bot.iniciar_servicos_background()
        print("[OK] Backend inicializado com sucesso!")
        if not expected_api_key():
            print(
                "[AVISO] LHN_API_KEY nao definida — modo teste (rotas abertas). "
                "Defina a chave no ambiente ou no painel e reinicie para produção."
            )
        yield
    except Exception as e:
        print(f"[ERRO] Falha na inicialização: {e}")
        traceback.print_exc()
        raise
    finally:
        # Limpeza ao desligar
        print("[-] Desligando backend...")
        if lhn_bot:
            if hasattr(lhn_bot, "is_app_alive"):
                lhn_bot.is_app_alive = False
            if hasattr(lhn_bot, "estrategia_rodando"):
                lhn_bot.estrategia_rodando = False

            try:
                lhn_bot.salvar_configuracoes_gerais()
            except Exception as e:
                print(f"[ERRO] Falha ao salvar configurações gerais no shutdown: {e}")

            try:
                lhn_bot.salvar_configuracoes_conta()
            except Exception as e:
                print(f"[ERRO] Falha ao salvar configurações de conta no shutdown: {e}")

            try:
                lhn_bot.salvar_saldo()
            except Exception as e:
                print(f"[ERRO] Falha ao salvar saldo no shutdown: {e}")

            try:
                if hasattr(lhn_bot, "model") and lhn_bot.model:
                    await asyncio.to_thread(lhn_bot.model.save, lhn_bot.arquivo_cerebro)
            except Exception as e:
                print(f"[ERRO] Falha ao salvar cérebro neural no shutdown: {e}")
            try:
                if hasattr(lhn_bot, "_feature_executor") and lhn_bot._feature_executor:
                    lhn_bot._feature_executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                print(f"[ERRO] Falha ao encerrar pool de features: {e}")
            try:
                if hasattr(lhn_bot, "_train_executor") and lhn_bot._train_executor:
                    lhn_bot._train_executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                print(f"[ERRO] Falha ao encerrar pool de treino: {e}")
            try:
                if hasattr(lhn_bot, "_bg_executor") and lhn_bot._bg_executor:
                    lhn_bot._bg_executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                print(f"[ERRO] Falha ao encerrar pool global de background: {e}")


app = FastAPI(title="LHN Sovereign V90 FINAL Backend", lifespan=lifespan)

_fe = (os.environ.get("LHN_FRONTEND_URL") or "").strip()
_cors_raw = (os.environ.get("LHN_CORS_ORIGINS") or "").strip()
if _cors_raw:
    _cors_origins = [x.strip() for x in _cors_raw.split(",") if x.strip()]
elif _fe:
    _cors_origins = [_fe]
else:
    _cors_origins = ["http://localhost:3002", "http://127.0.0.1:3002"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["http://localhost:3002"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pydantic import BaseModel, ConfigDict


def _sanitize_for_json(obj):
    """
    Garante serialização JSON compatível com JSON.parse no browser (sem NaN/Infinity).
    """
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0.0
        return obj
    if isinstance(obj, Decimal):
        try:
            return float(obj)
        except Exception:
            return 0.0
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]
    return obj


def _symbol_payload_valid(symbol) -> bool:
    """Rejeita None, não-string, vazio e literais 'none'/'null' (case-insensitive)."""
    if symbol is None or not isinstance(symbol, str):
        return False
    s = symbol.strip()
    if not s or s.lower() in ("none", "null"):
        return False
    return True


def _precos_buffer_latest_for_ws(precos_buffer):
    """Converte buffer deque/float em dict {symbol: último preço} para o WebSocket."""
    if not isinstance(precos_buffer, dict):
        return {}
    out = {}
    for k, v in precos_buffer.items():
        if isinstance(v, deque) and len(v):
            try:
                out[k] = float(v[-1])
            except (TypeError, ValueError):
                continue
        elif isinstance(v, (int, float)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                continue
    return out


def _sqlite_chat_context_suffix(db_path: str) -> str:
    """Lê últimas linhas do contexto neural (SQLite). Executar via asyncio.to_thread."""
    import sqlite3

    if not db_path or not os.path.exists(db_path):
        return ""
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT narrativa_str FROM llm_context_logs ORDER BY id DESC LIMIT 3"
        )
        rows = cursor.fetchall()
        if rows:
            return "Últimas análises neurais: " + " | ".join([r[0] for r in rows])
    except Exception:
        logging.getLogger(__name__).exception(
            "chat_context_db_read_failed | ts=%s | ativo=%s | payload=%s",
            int(time.time() * 1000),
            "GLOBAL",
            {"db_path": db_path},
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return ""


class CommandRequest(BaseModel):
    command: str


class SaldoRequest(BaseModel):
    saldo: float


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "lhn-sovereign-backend"}


@app.post("/api/command", dependencies=[Depends(verify_api_key)])
async def handle_command(req: CommandRequest):
    global lhn_bot
    if req.command and req.command.upper().startswith("CLOSE_"):
        symbol = req.command.replace("CLOSE_", "").replace("close_", "").strip().upper()
        if not _symbol_payload_valid(symbol):
            raise HTTPException(
                status_code=400,
                detail="Symbol inválido ou ausente para encerramento manual.",
            )
        if lhn_bot and hasattr(lhn_bot, "encerrar_operacao_manual"):
            await lhn_bot.encerrar_operacao_manual(symbol)
        return {"status": "success", "command": req.command}

    cmd_norm = (req.command or "").strip().upper()
    if cmd_norm in ("START_ENGINE", "START"):
        wr = (getattr(lhn_bot, "workspace_raiz", None) or "").strip()
        workspace_missing = (not wr) or (not os.path.isdir(wr))
        not_ok = not getattr(lhn_bot, "_workspace_ok", False)
        if workspace_missing or not_ok:
            lhn_bot.configurar_workspace_autonomo()
            lhn_bot._sync_cfg_workspace_paths()
            if hasattr(lhn_bot, "iniciar_bot"):
                lhn_bot.submit_background_task(lhn_bot.iniciar_bot)
        else:
            lhn_bot._sync_cfg_workspace_paths()

        if not getattr(lhn_bot, "_workspace_ok", False) or not getattr(
            lhn_bot, "workspace_raiz", None
        ):
            return {
                "status": "error",
                "command": req.command,
                "detail": "Workspace não montado. Verifique o boot e a pasta Workspace_LHN.",
            }
        if not os.path.isdir(lhn_bot.workspace_raiz):
            return {
                "status": "error",
                "command": req.command,
                "detail": "Pasta workspace inexistente no disco.",
            }
        lhn_bot.estrategia_rodando = True
        lhn_bot.is_searching = True
        lhn_bot._saldo_inicial_sessao = lhn_bot.saldo_atual
        lhn_bot.log_msg("🚀 Motor de Trading INICIADO via Painel de Controle.")
    elif req.command == "STOP_ENGINE":
        lhn_bot.estrategia_rodando = False
        lhn_bot.is_searching = False
        if hasattr(lhn_bot, "pausar_bot"):
            lhn_bot.pausar_bot()
        lhn_bot.log_msg("🛑 Motor de Trading PAUSADO via Painel de Controle.")
    elif req.command == "FORCAR_TREINO":
        lhn_bot.log_msg("⚠️ Sinal de Retreino Forçado recebido...")
        lhn_bot.submit_background_task(
            getattr(lhn_bot, "forcar_retreino", lambda: None)
        )
    elif cmd_norm == "ZERAR_STATS":
        if hasattr(lhn_bot, "zerar_desempenho"):
            lhn_bot.zerar_desempenho()
        lhn_bot.log_msg("🗑️ Desempenho zerado via Painel.")
        await _broadcast_fast_ws_status(lhn_bot)

    return {"status": "success", "command": req.command}


@app.post("/api/saldo", dependencies=[Depends(verify_api_key)])
async def update_saldo(req: SaldoRequest):
    global lhn_bot
    novo_saldo = float(_safe_float(req.saldo, 0.0))
    if novo_saldo < 0:
        novo_saldo = 0.0

    # Atualização estrita do saldo de simulação (separado do saldo real).
    if hasattr(lhn_bot, "set_saldo_simulado_manual"):
        lhn_bot.set_saldo_simulado_manual(float(novo_saldo))
    else:
        lhn_bot.saldo_simulacao = float(novo_saldo)
        lhn_bot.saldo_atual = float(novo_saldo)
        lhn_bot._saldo_inicial_sessao = float(novo_saldo)
    lhn_bot.log_msg(
        f"Saldo de SIMULAÇÃO redefinido manualmente para US$ {lhn_bot.saldo_atual}"
    )

    if not _is_real_mode_bot(lhn_bot):
        if hasattr(lhn_bot, "cfg") and isinstance(lhn_bot.cfg, dict):
            lhn_bot.cfg["saldo_simulacao_inicial"] = float(novo_saldo)
            lhn_bot.cfg["conta_real"] = False
            lhn_bot.cfg["modo_real"] = False
        lhn_bot.modo_real = False
        if hasattr(lhn_bot, "_saldo_manual_override_until"):
            # Janela longa: evita que sync/config/API key ressincronize o capital para o valor antigo do JSON.
            lhn_bot._saldo_manual_override_until = time.time() + 86400.0
        if hasattr(lhn_bot, "salvar_saldo"):
            lhn_bot.salvar_saldo()
        if hasattr(lhn_bot, "salvar_configuracoes_gerais"):
            try:
                lhn_bot.salvar_configuracoes_gerais()
            except Exception:
                pass

    await _broadcast_fast_ws_status(lhn_bot)
    return {"status": "success", "saldo": _saldo_runtime_bot(lhn_bot)}


@app.get("/api/history/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_history(symbol: str, interval: str = Query(default="1m")):
    global lhn_bot
    try:
        interval_norm = (
            interval if interval in {"1m", "5m", "15m", "1h", "4h", "1d"} else "1m"
        )
        from bybit_helpers import (get_kline_throttled, map_interval,
                                   normalize_klines_result)

        client = lhn_bot.get_bybit_client()
        res = await asyncio.to_thread(
            lambda: get_kline_throttled(
                client,
                category="linear",
                symbol=symbol.upper(),
                interval=map_interval(interval_norm),
                limit=100,
            )
        )
        klines = normalize_klines_result(res)
        formatted_klines = []
        for k in klines:
            formatted_klines.append(
                {
                    "time": int(k[0] / 1000),  # Timestamp em segundos
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                }
            )
        return formatted_klines
    except Exception as e:
        if lhn_bot and hasattr(lhn_bot, "erro_msg"):
            lhn_bot.erro_msg(f"Erro ao carregar histórico de {symbol}: {e}")
        return []


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    api_key: str = ""
    api_secret: str = ""
    modo_real: bool = False
    tema_visual: str = "Dark"
    idioma: str = "pt-BR"
    margem_entrada: float = 10.0
    alavancagem: int = 20
    winrate_minimo: float = 50.0
    # [FIX 4] Parâmetros da IA Neural (espelho do DEFAULT_CFG)
    rsi_p: int = 14
    rsi_ob: int = 70
    rsi_os: int = 30
    ema_count: int = 2
    adx_p: int = 14
    adx_thresh: float = REGIME_ADX_MIN
    confluencia_min: int = 2
    use_kelly: bool = True
    ai_certainty_threshold: float = 85.0
    use_trailing_stop: bool = True
    escudo_btc: bool = True
    adx_regime_minimo: float = REGIME_ADX_MIN
    nlp_sentimento_minimo: float = -3.0
    l2_imbalance_corte: float = 0.0
    use_rsi: bool = True
    use_ema: bool = True
    use_macd: bool = True
    use_bb: bool = True
    use_adx: bool = True
    use_stoch: bool = False
    use_vol: bool = False
    use_mtf: bool = True
    macd_f: int = 12
    macd_s: int = 26
    macd_sig: int = 9
    bb_p: int = 20
    bb_std: float = 2.0
    stoch_k: int = 14
    stoch_d: int = 3
    vol_sma: int = 20
    tipo_execucao: str = "TAKER"
    use_async_engine: bool = True
    use_ws: bool = True
    use_l2_depth: bool = True
    use_backtest: bool = True
    use_binance_vision: bool = True
    use_ws_orders: bool = False
    use_mtf_neural: Optional[bool] = False
    use_institutional_microstructure: bool = True
    use_breakeven: bool = True
    smart_exit: bool = True
    use_trailing_tp: bool = True
    escudo_dark: bool = True
    fuso_horario: str = "America/Sao_Paulo"
    telegram_token: str = ""
    telegram_chat_id: str = ""
    saldo_simulacao_inicial: float | None = 50.0
    # Filtros institucionais / motor (espelho DEFAULT_CFG — expostos à UI)
    use_funding_filter: bool = False
    use_oi_filter: bool = False
    use_regime_filter: bool = False
    hard_sl_pct: float = 0.015
    engine_param_1: float = 0.5
    engine_param_2: int = 100
    use_lateral_smart_money_filters: bool = True
    lateral_oi_min_delta_pct: float = 0.0
    lateral_funding_bias_eps: float = 0.0001
    lateral_funding_bias_certeza_pts: float = 3.0
    use_reinforcement_lateral_training: bool = False
    use_scale_out: bool = True
    scale_out_pct: float = 0.50
    alvo_lucro_base: float = 0.008
    breakeven_trigger: float = 0.003
    trailing_tp_activation: float = 0.005
    trailing_tp_callback: float = 0.0015
    trailing_fee_buffer_pct: float = 0.0012
    trailing_min_profit_lock_pct: float = 0.0025
    # Governança (persistido em cfg; risk_threshold lido em executar_ordem_real)
    trading_enabled: Optional[bool] = True
    risk_threshold: Optional[float] = 0.02
    # Córtex / layout neural (65D) e replay
    use_65d_layout: Optional[bool] = False
    replay_buffer_purge_on_layout_upgrade: Optional[bool] = False
    # Sensores HFT (Túnel 2 spoof)
    spoof_evaporation_pct: Optional[float] = 0.4
    spoof_price_flat_eps_pct: Optional[float] = 0.06
    # Arbitragem estatística (pairs)
    arb_zscore_entry: Optional[float] = 2.0
    arb_zscore_exit: Optional[float] = 0.5
    arb_loop_interval_sec: Optional[float] = 5.0
    # Deep Memory / cofre neural (WORM)
    cofre_quota_gb: Optional[float] = 45.0
    cofre_worm_profit_usd: Optional[float] = 15.0
    # Chave HTTP painel→backend (`X-API-Key`); persistida em `.env` como `LHN_API_KEY`
    lhn_api_key: str | None = None


# Defaults para chaves ausentes / null no ficheiro de cfg do utilizador (GET /api/config).
_GOVERNANCE_CFG_DEFAULTS: dict[str, object] = {
    "trading_enabled": True,
    "risk_threshold": 0.02,
    "use_65d_layout": False,
    "replay_buffer_purge_on_layout_upgrade": False,
    "spoof_evaporation_pct": 0.4,
    "spoof_price_flat_eps_pct": 0.06,
    "arb_zscore_entry": 2.0,
    "arb_zscore_exit": 0.5,
    "arb_loop_interval_sec": 5.0,
    "cofre_quota_gb": 45.0,
    "cofre_worm_profit_usd": 15.0,
}


def _as_bool(value, default: bool = False) -> bool:
    """Coerção segura (evita bool('false') == True em strings vindas de JSON/YAML)."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "on", "sim"):
        return True
    if s in ("false", "0", "no", "off", "nao", "não", "none", ""):
        return False
    return default


@app.get("/api/config", dependencies=[Depends(verify_api_key)])
async def get_config():
    global lhn_bot
    if lhn_bot is None:
        raise HTTPException(
            status_code=503,
            detail="Motor LHN não inicializado.",
        )
    _raw_cfg = getattr(lhn_bot, "cfg", None)
    cfg: dict = dict(_raw_cfg) if isinstance(_raw_cfg, dict) else {}
    for _gk, _gv in _GOVERNANCE_CFG_DEFAULTS.items():
        if _gk not in cfg or cfg.get(_gk) is None:
            cfg[_gk] = _gv

    _sec = getattr(lhn_bot, "api_secret", None)
    api_secret_masked = (
        "***MASCARADO***" if (_sec is not None and str(_sec).strip()) else ""
    )
    return {
        "api_key": getattr(lhn_bot, "api_key", ""),
        "api_secret": api_secret_masked,
        "modo_real": getattr(lhn_bot, "modo_real", False),
        "tema_visual": cfg.get("tema_visual", "Dark"),
        "idioma": cfg.get("idioma", "pt-BR"),
        "margem_entrada": _safe_float(cfg.get("margem_entrada"), 10.0),
        "alavancagem": int(_safe_float(cfg.get("alavancagem"), 20)),
        "winrate_minimo": _safe_float(cfg.get("winrate_minimo"), 50.0),
        # [FIX 4] Parâmetros da IA
        "rsi_p": int(_safe_float(cfg.get("rsi_p"), 14)),
        "rsi_ob": int(_safe_float(cfg.get("rsi_ob"), 70)),
        "rsi_os": int(_safe_float(cfg.get("rsi_os"), 30)),
        "ema_count": int(_safe_float(cfg.get("ema_count"), 2)),
        "adx_p": int(_safe_float(cfg.get("adx_p"), 14)),
        "adx_thresh": _safe_float(cfg.get("adx_thresh"), REGIME_ADX_MIN),
        "confluencia_min": int(_safe_float(cfg.get("confluencia_min"), 2)),
        "use_kelly": _as_bool(cfg.get("use_kelly"), True),
        "ai_certainty_threshold": _safe_float(cfg.get("ai_certainty_threshold"), 85.0),
        "use_trailing_stop": _as_bool(cfg.get("use_trailing_stop"), True),
        "escudo_btc": _as_bool(cfg.get("escudo_btc"), True),
        "adx_regime_minimo": _safe_float(cfg.get("adx_regime_minimo"), REGIME_ADX_MIN),
        "nlp_sentimento_minimo": _safe_float(
            cfg.get("nlp_sentimento_minimo"), -3.0
        ),
        "l2_imbalance_corte": _safe_float(cfg.get("l2_imbalance_corte"), 0.0),
        "use_rsi": _as_bool(cfg.get("use_rsi"), True),
        "use_ema": _as_bool(cfg.get("use_ema"), True),
        "use_macd": _as_bool(cfg.get("use_macd"), True),
        "use_bb": _as_bool(cfg.get("use_bb"), True),
        "use_adx": _as_bool(cfg.get("use_adx"), True),
        "use_stoch": _as_bool(cfg.get("use_stoch"), False),
        "use_vol": _as_bool(cfg.get("use_vol"), False),
        "use_mtf": _as_bool(cfg.get("use_mtf"), True),
        "macd_f": int(_safe_float(cfg.get("macd_f"), 12)),
        "macd_s": int(_safe_float(cfg.get("macd_s"), 26)),
        "macd_sig": int(_safe_float(cfg.get("macd_sig"), 9)),
        "bb_p": int(_safe_float(cfg.get("bb_p"), 20)),
        "bb_std": _safe_float(cfg.get("bb_std"), 2.0),
        "stoch_k": int(_safe_float(cfg.get("stoch_k"), 14)),
        "stoch_d": int(_safe_float(cfg.get("stoch_d"), 3)),
        "vol_sma": int(_safe_float(cfg.get("vol_sma"), 20)),
        "tipo_execucao": str(cfg.get("tipo_execucao") or "TAKER"),
        "use_async_engine": _as_bool(cfg.get("use_async_engine"), True),
        "use_ws": _as_bool(cfg.get("use_ws"), True),
        "use_l2_depth": _as_bool(cfg.get("use_l2_depth"), True),
        "use_backtest": _as_bool(cfg.get("use_backtest"), True),
        "use_binance_vision": _as_bool(cfg.get("use_binance_vision"), True),
        "use_ws_orders": _as_bool(cfg.get("use_ws_orders"), False),
        "use_mtf_neural": _as_bool(cfg.get("use_mtf_neural"), False),
        "use_institutional_microstructure": _as_bool(
            cfg.get("use_institutional_microstructure"), True
        ),
        "use_breakeven": _as_bool(cfg.get("use_breakeven"), True),
        "smart_exit": _as_bool(cfg.get("smart_exit"), True),
        "use_trailing_tp": _as_bool(cfg.get("use_trailing_tp"), True),
        "escudo_dark": _as_bool(cfg.get("escudo_dark"), True),
        "fuso_horario": str(cfg.get("fuso_horario") or "America/Sao_Paulo"),
        "telegram_token": str(cfg.get("telegram_token") or ""),
        "telegram_chat_id": str(cfg.get("telegram_chat_id") or ""),
        "saldo_simulacao_inicial": _safe_float(
            cfg.get("saldo_simulacao_inicial"), 50.0
        ),
        "use_funding_filter": _as_bool(cfg.get("use_funding_filter"), False),
        "use_oi_filter": _as_bool(cfg.get("use_oi_filter"), False),
        "use_regime_filter": _as_bool(cfg.get("use_regime_filter"), False),
        "hard_sl_pct": _safe_float(cfg.get("hard_sl_pct"), 0.015),
        "engine_param_1": _safe_float(cfg.get("engine_param_1"), 0.5),
        "engine_param_2": int(_safe_float(cfg.get("engine_param_2"), 100)),
        "use_lateral_smart_money_filters": _as_bool(
            cfg.get("use_lateral_smart_money_filters"), True
        ),
        "lateral_oi_min_delta_pct": _safe_float(
            cfg.get("lateral_oi_min_delta_pct"), 0.0
        ),
        "lateral_funding_bias_eps": _safe_float(
            cfg.get("lateral_funding_bias_eps"), 0.0001
        ),
        "lateral_funding_bias_certeza_pts": _safe_float(
            cfg.get("lateral_funding_bias_certeza_pts"), 3.0
        ),
        "use_reinforcement_lateral_training": _as_bool(
            cfg.get("use_reinforcement_lateral_training"), False
        ),
        "use_scale_out": _as_bool(cfg.get("use_scale_out"), True),
        "scale_out_pct": _safe_float(cfg.get("scale_out_pct"), 0.50),
        "alvo_lucro_base": _safe_float(cfg.get("alvo_lucro_base"), 0.008),
        "breakeven_trigger": _safe_float(cfg.get("breakeven_trigger"), 0.003),
        "trailing_tp_activation": _safe_float(
            cfg.get("trailing_tp_activation"), 0.005
        ),
        "trailing_tp_callback": _safe_float(
            cfg.get("trailing_tp_callback"), 0.0015
        ),
        "trailing_fee_buffer_pct": _safe_float(
            cfg.get("trailing_fee_buffer_pct"), 0.0012
        ),
        "trailing_min_profit_lock_pct": _safe_float(
            cfg.get("trailing_min_profit_lock_pct"), 0.0025
        ),
        "trading_enabled": _as_bool(cfg.get("trading_enabled"), True),
        "risk_threshold": _safe_float(cfg.get("risk_threshold"), 0.02),
        "use_65d_layout": _as_bool(cfg.get("use_65d_layout"), False),
        "replay_buffer_purge_on_layout_upgrade": _as_bool(
            cfg.get("replay_buffer_purge_on_layout_upgrade"), False
        ),
        "spoof_evaporation_pct": _safe_float(
            cfg.get("spoof_evaporation_pct"), 0.4
        ),
        "spoof_price_flat_eps_pct": _safe_float(
            cfg.get("spoof_price_flat_eps_pct"), 0.06
        ),
        "arb_zscore_entry": _safe_float(cfg.get("arb_zscore_entry"), 2.0),
        "arb_zscore_exit": _safe_float(cfg.get("arb_zscore_exit"), 0.5),
        "arb_loop_interval_sec": _safe_float(
            cfg.get("arb_loop_interval_sec"), 5.0
        ),
        "cofre_quota_gb": _safe_float(cfg.get("cofre_quota_gb"), 45.0),
        "cofre_worm_profit_usd": _safe_float(
            cfg.get("cofre_worm_profit_usd"), 15.0
        ),
    }


@app.post("/api/config", dependencies=[Depends(verify_api_key)])
async def update_config(cfg: ConfigModel):
    global lhn_bot
    try:
        partial = cfg.model_dump(exclude_unset=True)
        if not partial:
            return {"status": "success", "applied": []}

        # Campos Optional: JSON `null` não deve sobrescrever cfg com None.
        for _nk in (
            "trading_enabled",
            "risk_threshold",
            "use_mtf_neural",
            "use_65d_layout",
            "replay_buffer_purge_on_layout_upgrade",
            "spoof_evaporation_pct",
            "spoof_price_flat_eps_pct",
            "arb_zscore_entry",
            "arb_zscore_exit",
            "arb_loop_interval_sec",
            "cofre_quota_gb",
            "cofre_worm_profit_usd",
        ):
            if partial.get(_nk) is None:
                partial.pop(_nk, None)

        saldo_cfg_changed = "saldo_simulacao_inicial" in partial
        modo_real_changed = "modo_real" in partial
        _prev_modo_real = bool(getattr(lhn_bot, "modo_real", False))

        if "lhn_api_key" in partial:
            try:
                from lhn_secrets_persist import persist_lhn_app_api_key_dotenv

                persist_lhn_app_api_key_dotenv(str(partial.get("lhn_api_key") or ""))
            except Exception:
                logging.exception("persist_lhn_app_api_key_dotenv_failed")

        if "api_key" in partial:
            lhn_bot.api_key = partial["api_key"]
        if "api_secret" in partial:
            _incoming = partial["api_secret"]
            if isinstance(_incoming, str) and _incoming.strip() == "***MASCARADO***":
                pass
            else:
                lhn_bot.api_secret = _incoming
        if "modo_real" in partial:
            _new_modo_real = bool(partial["modo_real"])
            if _new_modo_real != _prev_modo_real:
                if _new_modo_real:
                    lhn_bot.log_msg(
                        "🔐 Modo CONTA REAL ativado — consultando saldo USDT na Bybit (aguarde sincronização)..."
                    )
                else:
                    lhn_bot.log_msg(
                        "🎮 Modo SIMULAÇÃO ativado — o saldo exibido seguirá o valor de simulação da interface."
                    )
            lhn_bot.modo_real = _new_modo_real
            if not _new_modo_real:
                sim_cur = _safe_float(
                    getattr(
                        lhn_bot, "saldo_simulacao", getattr(lhn_bot, "saldo_atual", 0.0)
                    ),
                    0.0,
                )
                lhn_bot.saldo_atual = sim_cur
            # JSON antigo pode ter conta_real=false enquanto modo_real=true no atributo — is_real_account_mode()
            # exige os dois alinhados; sem isso o sync de saldo cai no ramo de SIMULAÇÃO (ex.: US$ 50).
            bot_cfg_dict = getattr(lhn_bot, "cfg", None)
            if isinstance(bot_cfg_dict, dict):
                bot_cfg_dict["modo_real"] = _new_modo_real
                bot_cfg_dict["conta_real"] = _new_modo_real

        for key, value in partial.items():
            if key in ("api_key", "api_secret", "modo_real", "lhn_api_key"):
                continue
            if key == "saldo_simulacao_inicial":
                lhn_bot.cfg["saldo_simulacao_inicial"] = (
                    float(value) if value is not None else 50.0
                )
                continue
            lhn_bot.cfg[key] = value

        if "api_key" in partial or "api_secret" in partial:
            if hasattr(lhn_bot, "invalidar_client_cache"):
                lhn_bot.invalidar_client_cache()
            if hasattr(lhn_bot, "reiniciar_websocket"):
                try:
                    lhn_bot.reiniciar_websocket()
                except Exception:
                    logging.exception("reiniciar_websocket_after_keys_failed")
        elif modo_real_changed and hasattr(lhn_bot, "invalidar_client_cache"):
            _new_mr = bool(partial.get("modo_real", _prev_modo_real))
            if _new_mr != _prev_modo_real:
                # Novo cliente HTTP ao alternar real/simulação (evita cliente velho com timestamp defasado).
                lhn_bot.invalidar_client_cache()

        if hasattr(lhn_bot, "salvar_configuracoes_conta"):
            lhn_bot.salvar_configuracoes_conta()
        if hasattr(lhn_bot, "salvar_configuracoes_gerais"):
            lhn_bot.salvar_configuracoes_gerais()

        # [SSOT - FINAL ITERATION 3] Gravação síncrona com Single Source of Truth
        try:
            cfg_path = os.path.join(getattr(lhn_bot, "workspace_raiz", "./"), "LHN_CONFIG_MASTER.json")
            import json
            with open(cfg_path, "w", encoding="utf-8") as f:
                json.dump(lhn_bot.cfg, f, indent=4)
        except Exception as e:
            logging.exception(f"Erro SSOT dump LHN_CONFIG_MASTER: {e}")

        if hasattr(lhn_bot, "_reconcile_neural_feature_stack"):
            lhn_bot._reconcile_neural_feature_stack()

        try:
            if saldo_cfg_changed:
                sim_val = float(lhn_bot.cfg.get("saldo_simulacao_inicial", 50.0))
                if hasattr(lhn_bot, "set_saldo_simulado_manual"):
                    lhn_bot.set_saldo_simulado_manual(sim_val)
                else:
                    lhn_bot.saldo_simulacao = sim_val
                    if not _is_real_mode_bot(lhn_bot):
                        lhn_bot.saldo_atual = sim_val
                        lhn_bot._saldo_inicial_sessao = sim_val
                    if hasattr(lhn_bot, "salvar_saldo"):
                        lhn_bot.salvar_saldo()
            elif modo_real_changed and not bool(partial.get("modo_real", True)):
                # Retorno à simulação: restaurar saldo acumulado da simulação, sem reset para default.
                sim_cur = _safe_float(
                    getattr(
                        lhn_bot, "saldo_simulacao", getattr(lhn_bot, "saldo_atual", 0)
                    ),
                    0.0,
                )
                lhn_bot.saldo_atual = sim_cur
                lhn_bot._saldo_inicial_sessao = sim_cur
        except Exception:
            pass

        def sync_saldo_apos_config():
            try:
                if _is_real_mode_bot(lhn_bot):
                    from bybit_helpers import get_usdt_available_balance

                    key = (getattr(lhn_bot, "api_key", "") or "").strip()
                    secret = (getattr(lhn_bot, "api_secret", "") or "").strip()
                    if not key or not secret:
                        lhn_bot.saldo_real = 0.0
                        lhn_bot.saldo_atual = 0.0
                        lhn_bot._saldo_inicial_sessao = 0.0
                        lhn_bot._saldo_real_confirmado = False
                        lhn_bot.log_msg(
                            "⚠️ Modo real sem chaves API — saldo exibido como US$ 0.00 "
                            "(configure API Key/Secret na Configuração Mestra)."
                        )
                        if hasattr(lhn_bot, "salvar_saldo"):
                            lhn_bot.salvar_saldo()
                        return

                    client = lhn_bot.get_bybit_client()
                    if client:
                        saldo_livre = float(get_usdt_available_balance(client))
                        lhn_bot.saldo_real = saldo_livre
                        lhn_bot.saldo_atual = saldo_livre
                        lhn_bot._saldo_inicial_sessao = saldo_livre
                        lhn_bot._saldo_real_confirmado = saldo_livre > 0.0
                        lhn_bot.log_msg(
                            f"💰 Saldo REAL sincronizado: US$ {saldo_livre:.2f}"
                        )
                        if hasattr(lhn_bot, "salvar_saldo"):
                            lhn_bot.salvar_saldo()
                else:
                    # Simulação: preservar saldo acumulado em memória/disco; nunca resetar por toggle.
                    sim_cur = _safe_float(
                        getattr(
                            lhn_bot,
                            "saldo_simulacao",
                            getattr(
                                lhn_bot,
                                "carregar_saldo",
                                lambda: _safe_float(
                                    getattr(lhn_bot, "saldo_atual", 0.0), 0.0
                                ),
                            )(),
                        ),
                        0.0,
                    )
                    lhn_bot.saldo_simulacao = sim_cur
                    lhn_bot.saldo_atual = sim_cur
                    lhn_bot._saldo_inicial_sessao = sim_cur
                    if modo_real_changed and not _is_real_mode_bot(lhn_bot):
                        lhn_bot.log_msg(
                            f"🎮 Modo SIMULAÇÃO restaurado com saldo acumulado: US$ {sim_cur:.2f}"
                        )
            except Exception as e:
                err_s = str(e)
                if "10002" in err_s or "timestamp" in err_s.lower():
                    lhn_bot.log_msg(
                        "⚠️ Bybit recusou a sincronização do saldo (timestamp/relógio). "
                        "Ative 'Definir hora automaticamente' no Windows e salve de novo; recv_window já está no máximo (60s)."
                    )
                else:
                    lhn_bot.log_msg(f"⚠️ Erro ao puxar saldo da Bybit: {err_s[:280]}")

        if (
            "modo_real" in partial
            or "saldo_simulacao_inicial" in partial
            or "api_key" in partial
            or "api_secret" in partial
            or "lhn_api_key" in partial
        ):
            lhn_bot.submit_background_task(sync_saldo_apos_config)

        lhn_bot.log_msg(
            "⚙️ Configurações e Parâmetros (Margem, Alav, IA) atualizados via Web UI."
        )
        return {"status": "success", "applied": list(partial.keys())}
    except Exception as exc:
        logging.exception("update_config_failed")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "detail": str(exc),
                "trace": traceback.format_exc()[-4000:],
            },
        )


@app.get("/api/historico-operacoes", dependencies=[Depends(verify_api_key)])
@app.get("/api/historico_operacoes", dependencies=[Depends(verify_api_key)])
async def get_historico_operacoes():
    """
    Lista fechamentos para o painel (paridade com o WebSocket).
    Útil quando o cliente precisa ressincronizar sem depender só do stream.
    """
    global lhn_bot
    if not lhn_bot:
        return {"historico": []}
    # Nexus HFT V90: Soft Cutoff de Sessão — respeita session_start_epoch (painel sem apagar SQLite).
    raw = list(getattr(lhn_bot, "historico_operacoes", []) or [])
    if hasattr(lhn_bot, "historico_filtrado_sessao"):
        raw = lhn_bot.historico_filtrado_sessao(raw)
    raw = raw[:200]
    safe = []
    for row in raw:
        try:
            if isinstance(row, dict):
                safe.append(_sanitize_for_json(dict(row)))
        except Exception:
            continue
    return {"historico": safe, "total": len(safe)}


class ChatMessage(BaseModel):
    """Compatível com o painel: `message` (EN) ou `mensagem` (PT) + contexto opcional."""

    model_config = ConfigDict(extra="ignore")

    message: str | None = None
    mensagem: str | None = None
    contexto: dict | None = None


class SafeShutdownRequest(BaseModel):
    reason: str = "browser_unload"
    confirmed_exit: bool = True
    audit: dict = {}


def _nexus_sidecar_base() -> str:
    """URL do processo NEXUS (Qwen), separado do motor de trading — sem torch no 9002."""
    return (os.environ.get("LHN_NEXUS_URL") or "http://127.0.0.1:9001").rstrip("/")


@app.post("/api/chat", dependencies=[Depends(verify_api_key)])
async def chat_with_nexus(msg: ChatMessage):
    """
    Encaminha o chat ao sidecar NEXUS (python nexus_chat.py, porta 9001).
    O modelo NÃO roda neste processo — evita OOM/crash no backend HFT (ECONNRESET).
    Com Next.js, /api/chat costuma ir direto ao 9001 (rewrite); esta rota cobre chamadas à API no 9002.
    """
    global lhn_bot
    raw = (msg.message or msg.mensagem or "").strip()
    if not raw:
        raise HTTPException(
            status_code=400,
            detail="Envie o campo 'message' ou 'mensagem' com o texto do chat.",
        )

    def _snapshot_operacoes_nexus():
        lock_ops = getattr(lhn_bot, "_ops_lock", None)
        operacoes = []
        if lock_ops:
            with lock_ops:
                _oa = getattr(lhn_bot, "operacoes_abertas", {}) or {}
                for sym, op in _oa.items():
                    if not isinstance(op, dict) or op.get("_pending"):
                        continue
                    operacoes.append(
                        {
                            "symbol": sym,
                            "tipo": str(op.get("tipo") or op.get("direcao") or "LONG"),
                        }
                    )
                return operacoes, list(_oa.keys())
        _oa = getattr(lhn_bot, "operacoes_abertas", {}) or {}
        for sym, op in _oa.items():
            if not isinstance(op, dict) or op.get("_pending"):
                continue
            operacoes.append(
                {
                    "symbol": sym,
                    "tipo": str(op.get("tipo") or op.get("direcao") or "LONG"),
                }
            )
        return operacoes, list(_oa.keys())

    # Snapshot de estado com lock de thread fora do event loop principal.
    operacoes_nexus, ativos_abertos = await asyncio.to_thread(_snapshot_operacoes_nexus)

    context_str = f"Ativos abertos: {ativos_abertos}. "
    context_str += f"Saldo: {_saldo_runtime_bot(lhn_bot):.2f}. "
    context_str += f"Taxa de Acerto: {(getattr(lhn_bot, 'total_wins', 0) / max(1, getattr(lhn_bot, 'total_trades', 1))):.1%}. "

    db_path = getattr(lhn_bot, "arquivo_db_memoria", None)
    if lhn_bot and db_path and os.path.exists(db_path):
        suffix = await asyncio.to_thread(_sqlite_chat_context_suffix, db_path)
        if suffix:
            context_str += suffix

    ctx = dict(msg.contexto or {})
    ctx["operacoes"] = operacoes_nexus
    ctx["nlp_score"] = _safe_float(
        getattr(lhn_bot, "nlp_score", None),
        _safe_float(getattr(lhn_bot, "pontuacao_sentimento_atual", None), 0.0),
    )
    ctx["contexto_extra"] = context_str

    url = f"{_nexus_sidecar_base()}/api/chat"
    nexus_timeout_s = _safe_float(os.environ.get("LHN_NEXUS_TIMEOUT_SEC"), 600.0)
    # Mantém a rota assíncrona e permite respostas longas do LLM sem reset prematuro.
    timeout = aiohttp.ClientTimeout(
        total=max(60.0, nexus_timeout_s),
        connect=30.0,
        sock_connect=30.0,
        sock_read=max(60.0, nexus_timeout_s),
    )
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                url,
                json={"mensagem": raw, "contexto": ctx},
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Nexus sidecar respondeu {resp.status}: {text[:800]}",
                    )
                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, TypeError, ValueError):
                    raise HTTPException(
                        status_code=502,
                        detail="Resposta inválida do Nexus (não é JSON).",
                    )
                reply = data.get("resposta") or data.get("reply") or ""
                return {"status": "success", "reply": reply, "resposta": reply}
    except HTTPException:
        raise
    except aiohttp.ClientError as e:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Nexus LLM offline ou inacessível em {url}. "
                f"Inicie: python nexus_chat.py (porta 9001). Erro: {e}"
            ),
        ) from e


def _safe_float(value, default=0.0):
    """Converte valor para float seguro. Retorna default se None, NaN ou não-numérico."""
    if value is None:
        return default
    try:
        f = float(value)
        if f != f:  # NaN check
            return default
        return f
    except (TypeError, ValueError):
        return default


def _is_real_mode_bot(bot) -> bool:
    checker = getattr(bot, "is_real_account_mode", None)
    if callable(checker):
        try:
            return bool(checker())
        except Exception:
            return bool(getattr(bot, "modo_real", False))
    return bool(getattr(bot, "modo_real", False))


def _saldo_runtime_bot(bot) -> float:
    """Saldo exibido conforme ambiente ativo, preservando separação real/simulação."""
    if _is_real_mode_bot(bot):
        return _safe_float(
            getattr(bot, "saldo_real", getattr(bot, "saldo_atual", None)), 0.0
        )
    return _safe_float(
        getattr(bot, "saldo_simulacao", getattr(bot, "saldo_atual", None)), 1000.0
    )


def _clean_neural_scan_ts(raw):
    """Timestamp Unix (s) para WS: float arredondado; None se ainda não houve varredura neural real."""
    if raw is None:
        return None
    try:
        t = float(raw)
        if t != t or t <= 0:
            return None
        if t > 1e12:
            t = t / 1000.0
        now = time.time()
        if t > now + 120.0 or t < now - 86400.0 * 30:
            return None
        return round(t, 6)
    except (TypeError, ValueError):
        return None


def _build_fast_ws_status_snapshot(bot):
    """Payload enxuto para confirmar update de saldo sem esperar o próximo tick."""
    payload = _build_ws_status_payload(bot, include_logs=False)
    payload["ack_saldo_update"] = True
    return payload


async def _broadcast_fast_ws_status(bot):
    if not active_ws_clients:
        return
    payload = json.dumps(_build_fast_ws_status_snapshot(bot))
    dead = []
    for ws in list(active_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        active_ws_clients.discard(ws)


async def _broadcast_full_ws_status(bot):
    """Broadcast completo para UI refletir fecho de ordens sem esperar o loop periódico."""
    if not active_ws_clients:
        return
    payload_obj = _build_ws_status_payload(bot, include_logs=True)
    try:
        payload = json.dumps(payload_obj)
    except Exception:
        payload = json.dumps(_sanitize_for_json(payload_obj))
    dead = []
    for ws in list(active_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        active_ws_clients.discard(ws)


def _panic_flatten_open_positions(lhn_bot, operacoes_snapshot: dict) -> list:
    """
    Modo real: cancel_all_orders + fechar_ordem_real por símbolo.
    Simulação: remove entradas de operacoes_abertas (sem chamada à corretora).
    """
    errs = []
    if not operacoes_snapshot:
        return errs
    lock_ops = getattr(lhn_bot, "_ops_lock", None)
    if _is_real_mode_bot(lhn_bot):
        try:
            client = lhn_bot.get_bybit_client()
        except Exception as e:
            return [f"get_bybit_client: {e}"]
        for sym_raw, op in list(operacoes_snapshot.items()):
            if not _symbol_payload_valid(str(sym_raw or "")):
                continue
            sym = str(sym_raw).strip().upper()
            try:
                _call_with_retry(
                    lambda s=sym: client.cancel_all_orders(category="linear", symbol=s)
                )
            except Exception as e:
                errs.append(f"{sym} cancel_all_orders: {e}")
            qty = _safe_float(op.get("qty_real"), 0.0)
            tipo = op.get("tipo") or op.get("direcao") or "LONG"
            if qty > 0 and hasattr(lhn_bot, "fechar_ordem_real"):
                try:
                    ok = lhn_bot.fechar_ordem_real(sym, tipo, qty)
                    if not ok:
                        errs.append(f"{sym}: fechar_ordem_real=False")
                except Exception as e:
                    errs.append(f"{sym} fechar_ordem_real: {e}")
            if lock_ops:
                with lock_ops:
                    oa = getattr(lhn_bot, "operacoes_abertas", {})
                    if sym in oa:
                        del oa[sym]
            else:
                oa = getattr(lhn_bot, "operacoes_abertas", {})
                if sym in oa:
                    del oa[sym]
    else:
        if lock_ops:
            with lock_ops:
                oa = getattr(lhn_bot, "operacoes_abertas", {})
                for sym in list(operacoes_snapshot.keys()):
                    if sym in oa:
                        del oa[sym]
        else:
            oa = getattr(lhn_bot, "operacoes_abertas", {})
            for sym in list(operacoes_snapshot.keys()):
                if sym in oa:
                    del oa[sym]
    return errs


@app.post("/api/safe-shutdown", dependencies=[Depends(verify_api_key)])
async def safe_shutdown(req: SafeShutdownRequest):
    global lhn_bot
    if not lhn_bot:
        return {"status": "noop", "reason": "bot_not_initialized"}

    risks = []
    lock_ops = getattr(lhn_bot, "_ops_lock", None)
    operacoes = {}
    if lock_ops:
        with lock_ops:
            operacoes = dict(getattr(lhn_bot, "operacoes_abertas", {}).copy())
    else:
        operacoes = dict(getattr(lhn_bot, "operacoes_abertas", {}).copy())
    for symbol, op in operacoes.items():
        sl_val = _safe_float(op.get("sl"), 0.0)
        if sl_val <= 0:
            risks.append(f"{symbol}: sem Stop Loss configurado")

    trailing_on = bool(getattr(lhn_bot, "cfg", {}).get("use_trailing_stop", False))
    ws_online = bool((req.audit or {}).get("websocket_online", False))
    if trailing_on and operacoes and not ws_online:
        risks.append("Trailing Stop ativo com WebSocket de rastreio inativo")

    panic_errs = _panic_flatten_open_positions(lhn_bot, operacoes)
    if panic_errs and hasattr(lhn_bot, "erro_msg"):
        lhn_bot.erro_msg("SAFE-SHUTDOWN (flatten): " + " | ".join(panic_errs[:12]))
    all_risks = risks + panic_errs

    def _sync_shutdown_persist():
        if hasattr(lhn_bot, "forcar_salvamento_dados"):
            lhn_bot.forcar_salvamento_dados()
        else:
            if hasattr(lhn_bot, "salvar_saldo"):
                lhn_bot.salvar_saldo()
            if hasattr(lhn_bot, "salvar_configuracoes_conta"):
                lhn_bot.salvar_configuracoes_conta()
            if hasattr(lhn_bot, "salvar_configuracoes_gerais"):
                lhn_bot.salvar_configuracoes_gerais()

        if (
            hasattr(lhn_bot, "model")
            and lhn_bot.model
            and hasattr(lhn_bot, "arquivo_cerebro")
        ):
            lhn_bot.model.save(lhn_bot.arquivo_cerebro)

    try:
        await asyncio.to_thread(_sync_shutdown_persist)
    except Exception as e:
        if hasattr(lhn_bot, "erro_msg"):
            lhn_bot.erro_msg(f"Falha no safe-shutdown: {e}")
        return {
            "status": "error",
            "error": str(e),
            "critical_risk": len(all_risks) > 0,
            "risks": all_risks,
            "panic_errors": panic_errs,
        }

    if risks and hasattr(lhn_bot, "erro_msg"):
        lhn_bot.erro_msg("ALERTA CRITICO SAFE-SHUTDOWN: " + " | ".join(risks))

    return {
        "status": "saved",
        "critical_risk": len(all_risks) > 0,
        "risks": all_risks,
        "panic_errors": panic_errs,
    }


def _mean_ia_confidence_from_cache(bot):
    """Média das probabilidades no cache do radar. None se ainda não houve varredura (evita 50% falso no painel)."""
    cache = getattr(bot, "ia_cache_probs", None)
    if isinstance(cache, dict) and cache:
        vals = []
        for v in cache.values():
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                continue
        if vals:
            return float(sum(vals) / len(vals))
    return None


def _get_ia_confidence_map(bot):
    # Nexus HFT V90: telemetria UI — mapa bruto prob. IA por ativo (JSON-safe); não substitui a média global.
    raw = getattr(bot, "ia_cache_probs", None)
    if not isinstance(raw, dict) or not raw:
        return {}
    out = {}
    for k, v in raw.items():
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if not (fv == fv):  # NaN
            continue
        out[str(k)] = float(fv)
    return out


def _safe_ops_list(operacoes_dict):
    """Serializa operações abertas com blindagem multichave contra None."""
    result = []
    for k, v in operacoes_dict.items():
        try:
            ts = (
                v.get("ts_abertura")
                or v.get("timestamp")
                or (v.get("_ts") if isinstance(v, dict) else None)
                or time.time()
            )
            hora_str = time.strftime("%H:%M:%S", time.localtime(ts))
            preco_entrada = _safe_float(
                v.get("preco_entrada") or v.get("entry_price") or v.get("preco")
            )
            margem = _safe_float(
                v.get("margem_gasta")
                or v.get("margem_alocada")
                or v.get("margem")
                or v.get("margin")
            )
            alavancagem = int(v.get("alavancagem") or v.get("alav") or 20)
            certeza_val = _safe_float(
                v.get("certeza") or v.get("ia_prob") or v.get("confidence"),
                50.0,
            )
            entry = {
                "symbol": k,
                "alavancagem": alavancagem,
                "hora": hora_str,
                "tipo": v.get("tipo") or v.get("direcao") or "TRADE",
                "preco_entrada": preco_entrada,
                "margem": margem,
                "sl": _safe_float(v.get("sl") or v.get("stop_loss")),
                "tp": _safe_float(v.get("tp") or v.get("take_profit")),
                "certeza": certeza_val,
                "ia_prob": _safe_float(v.get("ia_prob"), certeza_val),
            }
            if isinstance(v, dict) and v.get("_pending"):
                entry["_pending"] = True
            result.append(entry)
        except Exception as e:
            print(f"Erro serializando op: {e}")
    return result


def _aggregate_closed_stats_from_rows(rows: list | None) -> tuple[float, float, float, float, float, float]:
    # Nexus HFT V90: Soft Cutoff de Sessão — agrega wins/losses/PnL bruto a partir de linhas já filtradas.
    ct = 0
    wins = 0.0
    losses = 0.0
    tprof = 0.0
    tloss = 0.0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        raw = r.get("pnl")
        if raw is None:
            raw = r.get("profit")
        if raw is None:
            raw = r.get("lucro")
        try:
            pl = float(raw or 0.0)
        except (TypeError, ValueError):
            pl = 0.0
        ct += 1
        if pl > 0:
            wins += 1.0
            tprof += pl
        else:
            losses += 1.0
            tloss += abs(pl)
    wr = (wins / float(ct) * 100.0) if ct else 0.0
    return float(ct), wins, losses, tprof, tloss, wr


def _build_ws_status_payload(bot, include_logs: bool = True, last_log_ts_ref=None):
    """Monta snapshot global do bot para stream em tempo real (100% JSON-safe)."""
    _hist_full = (getattr(bot, "historico_operacoes", []) or []).copy()
    _hist_filt = (
        bot.historico_filtrado_sessao(_hist_full)
        if hasattr(bot, "historico_filtrado_sessao")
        else _hist_full
    )
    _ep_hist = float(getattr(bot, "session_start_epoch", 0.0) or 0.0)
    if _ep_hist > 0.0 and hasattr(bot, "_historico_row_epoch_seconds"):
        # Hard cutoff WS: só fechos com instante >= marco de sessão (paridade com timestamp do trade).
        _hist_filt = [
            r
            for r in _hist_filt
            if isinstance(r, dict) and bot._historico_row_epoch_seconds(r) >= _ep_hist
        ]
    if _ep_hist > 0.0:
        # Nexus HFT V90: Soft Cutoff de Sessão — totais e win rate do stream alinhados ao histórico visível.
        total_trades, total_wins, total_losses, total_profit_usd, total_loss_usd, wr = (
            _aggregate_closed_stats_from_rows(_hist_filt)
        )
    else:
        wr = 0.0
        total_trades = _safe_float(getattr(bot, "total_trades", 0))
        total_wins = _safe_float(getattr(bot, "total_wins", 0))
        total_losses = _safe_float(getattr(bot, "total_losses", 0))
        total_profit_usd = _safe_float(getattr(bot, "total_profit_usd", None))
        total_loss_usd = _safe_float(getattr(bot, "total_loss_usd", None))
        if total_trades > 0:
            wr = (total_wins / total_trades) * 100.0

    saldo_final = _saldo_runtime_bot(bot)
    saldo_ini_raw = getattr(bot, "_saldo_inicial_sessao", None)
    saldo_ini = (
        saldo_final
        if saldo_ini_raw is None
        else _safe_float(saldo_ini_raw, saldo_final)
    )
    pnl = saldo_final - saldo_ini

    lock_ops = getattr(bot, "_ops_lock", None)
    if lock_ops:
        with lock_ops:
            _ops = getattr(bot, "operacoes_abertas", {}) or {}
            operacoes = _ops.copy()
    else:
        _ops = getattr(bot, "operacoes_abertas", {}) or {}
        operacoes = _ops.copy()

    margem_alocada_total = sum(
        _safe_float(op.get("margem"))
        for op in operacoes.values()
        if not (isinstance(op, dict) and op.get("_pending"))
    )

    pb_raw = getattr(bot, "precos_buffer", None)
    current_prices = _precos_buffer_latest_for_ws(pb_raw)
    if not current_prices:
        fallback = getattr(bot, "precos_atuais", None)
        current_prices = fallback.copy() if isinstance(fallback, dict) else {}
    elif isinstance(current_prices, dict):
        current_prices = current_prices.copy()

    sentiment_raw = getattr(bot, "nlp_score", None)
    sentiment_val = _safe_float(
        sentiment_raw,
        _safe_float(getattr(bot, "pontuacao_sentimento_atual", None), 0.0),
    )
    ia_conf_val = _mean_ia_confidence_from_cache(bot)
    ia_conf_map = _get_ia_confidence_map(bot)  # Nexus HFT V90: mapa por símbolo para UI (vs. média ia_confidence).
    dyn_lev_raw = getattr(bot, "alavancagem_dinamica", None)
    dyn_lev_val = int(_safe_float(dyn_lev_raw, 1))

    pnl_flutuante_total = 0.0
    for sym, op in operacoes.items():
        try:
            if isinstance(op, dict) and op.get("_pending"):
                continue
            entry = _safe_float(
                op.get("preco") or op.get("preco_entrada") or op.get("entry_price"), 0.0
            )
            if entry <= 0:
                continue
            p_now = _safe_float(current_prices.get(sym), 0.0)
            if p_now <= 0:
                continue
            margin = _safe_float(
                op.get("margem") or op.get("margem_gasta") or op.get("margem_alocada"),
                0.0,
            )
            alav = _safe_float(op.get("alav") or op.get("alavancagem"), 1.0)
            side = str(op.get("tipo") or op.get("direcao") or "LONG").upper()
            pct = (p_now - entry) / entry if side == "LONG" else (entry - p_now) / entry
            pnl_flutuante_total += margin * (pct * alav)
        except Exception:
            continue
    equity_total = saldo_final + pnl_flutuante_total

    _scan_ts = getattr(bot, "_ultima_varredura_ts", None)
    # Nexus HFT V90: Soft Cutoff de Sessão — reutiliza lista já filtrada para o payload WS.
    historico_finalizadas = _hist_filt[:50]
    estatisticas = {
        "saldo": saldo_final,
        "pnl_liquido": pnl,
        "winrate": wr,
        "total_trades": int(total_trades),
        "wins": int(total_wins),
        "losses": int(total_losses),
        "lucro_bruto_usd": total_profit_usd,
        "perda_bruta_usd": total_loss_usd,
        "pnl_flutuante_total": pnl_flutuante_total,
        "equity_total": equity_total,
    }
    try:
        if hasattr(bot, "_calcular_estatisticas"):
            estatisticas["consolidado_finalizadas"] = bot._calcular_estatisticas()
    except Exception:
        estatisticas["consolidado_finalizadas"] = {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "winrate": 0.0,
            "pnl_liquido": 0.0,
        }

    # Alinha contadores do snapshot WS com a lista real (histórico) quando o contador do bot atrasar.
    # Não altera motor — apenas evita divergência entre cards e consolidado_finalizadas.
    _cf = estatisticas.get("consolidado_finalizadas")
    if isinstance(_cf, dict):
        _ct = int(_safe_float(_cf.get("total_trades"), 0))
        _et = int(estatisticas.get("total_trades") or 0)
        if _ct > _et:
            estatisticas["total_trades"] = _ct
            estatisticas["wins"] = int(_safe_float(_cf.get("wins"), 0))
            estatisticas["losses"] = int(_safe_float(_cf.get("losses"), 0))
            estatisticas["winrate"] = _safe_float(_cf.get("winrate"), 0.0)

    status_data = {
        "prices": current_prices,
        "websocket_online": bool(getattr(bot, "websocket_online", True)),
        "monitoramento_apenas": bool(
            getattr(bot, "monitoramento_apenas_por_ws", False)
        ),
        "sentiment_nlp": sentiment_val,
        "nlp_score": sentiment_val,
        "last_neural_scan_ts": _clean_neural_scan_ts(_scan_ts),
        "is_running": bool(getattr(bot, "estrategia_rodando", False)),
        "saldo": saldo_final,
        "pnl_liquido": pnl,
        "pnl_flutuante_total": pnl_flutuante_total,
        "equity_total": equity_total,
        "margem_alocada_total": margem_alocada_total,
        "win_rate": wr,
        "total_trades": int(total_trades),
        "total_wins": int(total_wins),
        "total_losses": int(total_losses),
        "total_profit_usd": total_profit_usd,
        "total_loss_usd": total_loss_usd,
        "operacoes": _safe_ops_list(operacoes),
        "historico": historico_finalizadas,
        "operacoes_finalizadas": historico_finalizadas,
        "estatisticas": estatisticas,
        "news": (getattr(bot, "news_history", []) or []).copy(),
        "ia_confidence": ia_conf_val,
        "ia_confidence_map": ia_conf_map,  # Nexus HFT V90: contrato WS — prob. por ativo para o gráfico selecionado.
        "dynamic_leverage": dyn_lev_val,
        "config": {
            "lang": bot.cfg.get("idioma", "Português-Brasil"),
            "theme": bot.cfg.get("tema_visual", "Dark"),
            "margem_entrada": _safe_float(bot.cfg.get("margem_entrada"), 10.0),
            "alavancagem": int(_safe_float(bot.cfg.get("alavancagem"), 20)),
            "winrate_minimo": _safe_float(bot.cfg.get("winrate_minimo"), 80.0),
            "engine_param_1": _safe_float(bot.cfg.get("engine_param_1"), 0.5),
            "engine_param_2": int(_safe_float(bot.cfg.get("engine_param_2"), 100)),
        },
    }
    if include_logs:
        log_history = getattr(bot, "log_history", []) or []
        if log_history:
            now = time.time()
            allow = True
            if last_log_ts_ref is not None:
                allow = now - float(last_log_ts_ref[0]) >= 1.0
            if allow:
                if last_log_ts_ref is not None:
                    last_log_ts_ref[0] = now
                log_tail = log_history[-50:].copy()
                status_data["logs"] = log_tail
                status_data["log_history"] = log_tail
    return _sanitize_for_json(status_data)


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket):
    ok, sub = websocket_subprotocol_token_ok(websocket.headers)
    if not ok:
        await websocket.close(code=1008)
        return
    if sub is not None:
        await websocket.accept(subprotocol=sub)
    else:
        await websocket.accept()
    active_ws_clients.add(websocket)
    _ws_none_warned = False
    _last_log_broadcast = [0.0]
    try:
        while True:
            try:
                status_data = _build_ws_status_payload(
                    lhn_bot, include_logs=True, last_log_ts_ref=_last_log_broadcast
                )
                if (
                    getattr(lhn_bot, "_saldo_inicial_sessao", None) is None
                    and not _ws_none_warned
                ):
                    print(
                        "[WS] Saldo inicial da sessão ainda não definido. Usando saldo atual como referência."
                    )
                    _ws_none_warned = True
                try:
                    try:
                        payload = json.dumps(status_data)
                    except Exception:
                        payload = json.dumps(_sanitize_for_json(status_data))
                    await asyncio.wait_for(
                        websocket.send_text(payload),
                        timeout=2.0,
                    )
                except Exception:
                    break
            except ConnectionError:
                break
            except Exception as e:
                err_str = str(e).lower()
                if any(
                    x in err_str
                    for x in [
                        "close",
                        "disconnect",
                        "1001",
                        "1005",
                        "1006",
                        "10054",
                        "going away",
                        "was forced",
                    ]
                ):
                    break
                print(f"[WS] Erro menor no ciclo: {e}")
                await asyncio.sleep(1)
                continue

            await asyncio.sleep(0.5)
    except Exception as e:
        if "close" not in str(e).lower():
            print(f"[ERRO] Conexão WebSocket perdida: {e}")
    finally:
        active_ws_clients.discard(websocket)


@app.get("/api/market/tickers", dependencies=[Depends(verify_api_key)])
async def proxy_bybit_tickers(category: str = Query("linear")):
    """
    Proxy server-side para tickers Bybit (evita CORS no browser ao chamar api.bybit.com).
    """
    if category not in ("linear", "spot", "inverse", "option"):
        category = "linear"
    url = f"https://api.bybit.com/v5/market/tickers?category={category}"
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                text = await resp.text()
                if resp.status != 200:
                    return JSONResponse(
                        status_code=502,
                        content={
                            "retCode": -1,
                            "retMsg": f"Bybit HTTP {resp.status}",
                            "result": {"list": []},
                        },
                    )
                try:
                    data = json.loads(text)
                except (json.JSONDecodeError, TypeError, ValueError):
                    return JSONResponse(
                        status_code=502,
                        content={
                            "retCode": -1,
                            "retMsg": "Internal Server Error upstream",
                            "result": {"list": []},
                        },
                    )
                return _sanitize_for_json(data)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=502,
            content={
                "retCode": -1,
                "retMsg": str(e),
                "result": {"list": []},
            },
        )


@app.get("/api/signals", dependencies=[Depends(verify_api_key)])
async def get_signals():
    """
    [V90] Retorna o histórico de sinais gerados pela IA.
    Lê do buffer em memória (sinais_historico) ou retorna lista vazia se não iniciado.
    """
    global lhn_bot
    if not lhn_bot:
        return {"signals": []}

    sinais = list(getattr(lhn_bot, "sinais_historico", []) or [])
    # Serialização segura
    resultado = []
    for s in sinais[-500:]:  # Máximo 500 sinais
        try:
            acao_raw = str(s.get("acao") or s.get("tipo") or "").strip()
            if not acao_raw:
                acao_raw = str(s.get("direcao") or "").strip()
            resultado.append(
                {
                    "id": s.get("id"),
                    "timestamp": s.get("timestamp", ""),
                    "par": str(s.get("par") or s.get("symbol") or ""),
                    "acao": acao_raw,
                    "preco_entrada": _safe_float(
                        s.get("preco_entrada") or s.get("preco")
                    ),
                    "tp1": _safe_float(s.get("tp1") or s.get("tp")),
                    "tp2": _safe_float(s.get("tp2")),
                    "tp3": _safe_float(s.get("tp3")),
                    "sl": _safe_float(s.get("sl")),
                    "certeza": _safe_float(
                        s.get("certeza") or s.get("confidence"), 0.0
                    ),
                    "fluxo": _safe_float(s.get("fluxo") or s.get("vpin")),
                    "status": s.get("status"),
                }
            )
        except Exception:
            pass
    return {"signals": resultado, "total": len(resultado)}


@app.get("/api/config/transmission", dependencies=[Depends(verify_api_key)])
async def get_transmission_config():
    """Retorna a configuração do bot de transmissão de sinais (isolada do Telegram pessoal)."""
    global lhn_bot
    if not lhn_bot:
        return {
            "transmissao_ativa": False,
            "transmissao_token": "",
            "transmissao_chat_id": "",
        }
    if hasattr(lhn_bot, "_get_transmissao_cfg"):
        return lhn_bot._get_transmissao_cfg()
    return {
        "transmissao_ativa": False,
        "transmissao_token": "",
        "transmissao_chat_id": "",
    }


@app.post("/api/config/transmission", dependencies=[Depends(verify_api_key)])
async def save_transmission_config(payload: dict):
    """Salva a configuração do bot de transmissão de sinais."""
    global lhn_bot
    if not lhn_bot:
        raise HTTPException(status_code=503, detail="Bot não inicializado.")

    cfg = {
        "transmissao_ativa": bool(payload.get("transmissao_ativa", False)),
        "transmissao_token": str(payload.get("transmissao_token", "") or "").strip(),
        "transmissao_chat_id": str(
            payload.get("transmissao_chat_id", "") or ""
        ).strip(),
    }
    if hasattr(lhn_bot, "salvar_transmissao_cfg"):
        await asyncio.to_thread(lhn_bot.salvar_transmissao_cfg, cfg)
    lhn_bot.log_msg(
        f"📡 [TRANSMISSÃO] Config atualizada. Ativo: {cfg['transmissao_ativa']}"
    )
    return {"status": "ok", "saved": cfg}


@app.post("/api/config/transmission/test", dependencies=[Depends(verify_api_key)])
async def test_transmission():
    """Envia uma mensagem de teste via bot de transmissão de sinais."""
    global lhn_bot
    if not lhn_bot:
        raise HTTPException(status_code=503, detail="Bot não inicializado.")
    if not hasattr(lhn_bot, "enviar_teste_transmissao"):
        raise HTTPException(status_code=501, detail="Função de teste não disponível.")

    result = await lhn_bot.enviar_teste_transmissao()
    if result.get("ok"):
        lhn_bot.log_msg("📡 [TRANSMISSÃO] Mensagem de teste enviada com sucesso.")
        return {"status": "ok", "message": "Mensagem de teste enviada!"}
    err = result.get("error", "Falha no envio do teste.")
    raise HTTPException(status_code=400, detail=err)


def _executar_snapshot_neural_checkpoint_sync(bot, log_prefix: str = "📸 [SNAPSHOT]"):
    """
    Mesma lógica do botão 'Snapshot Neural': persiste .keras, sandbox e configs.
    Retorna (relatorio, erros).
    """
    relatorio: list = []
    erros: list = []

    # ── 1. Salvar pesos das Redes Neurais ──────────────────────────────────
    modelos_salvos = []
    _arena_pairs = [
        ("model_sniper_titular", "arq_sniper_titular"),
        ("model_sniper_reserva", "arq_sniper_reserva"),
        ("model_lateral_titular", "arq_lateral_titular"),
        ("model_lateral_reserva", "arq_lateral_reserva"),
        ("model_sniper", "arquivo_cerebro_sniper"),
        ("model_lateral", "arquivo_cerebro_lateral"),
    ]
    _seen_paths: set[str] = set()
    for attr_modelo, attr_path in _arena_pairs:
        modelo = getattr(bot, attr_modelo, None)
        path = getattr(bot, attr_path, None)
        if modelo is not None and path and path not in _seen_paths:
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                modelo.save(path)
                _seen_paths.add(path)
                modelos_salvos.append(os.path.basename(path))
            except Exception as e:
                erros.append(f"Modelo {attr_modelo}: {e}")
    _wr = getattr(bot, "workspace_raiz", None)
    _gp = (
        os.path.join(_wr, "modelos", "guardiao_v90.keras")
        if _wr
        else None
    )
    modelo_g = getattr(bot, "modelo_guardiao", None)
    if modelo_g is not None and _gp and _gp not in _seen_paths:
        try:
            os.makedirs(os.path.dirname(_gp) or ".", exist_ok=True)
            modelo_g.save(_gp)
            _seen_paths.add(_gp)
            modelos_salvos.append(os.path.basename(_gp))
        except Exception as e:
            erros.append(f"Modelo modelo_guardiao: {e}")
    for attr_modelo, attr_path in [
        ("modelo_sniper", "arquivo_cerebro_sniper"),
        ("modelo_lateral", "arquivo_cerebro_lateral"),
        ("modelo_guardiao", "arquivo_cerebro_guardiao"),
    ]:
        modelo = getattr(bot, attr_modelo, None)
        path = getattr(bot, attr_path, None)
        if modelo is not None and path and path not in _seen_paths:
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                modelo.save(path)
                _seen_paths.add(path)
                modelos_salvos.append(os.path.basename(path))
            except Exception as e:
                erros.append(f"Modelo {attr_modelo}: {e}")
    modelo_leg = getattr(bot, "model", None)
    path_leg = getattr(bot, "arquivo_cerebro", None)
    if (
        modelo_leg is not None
        and path_leg
        and os.path.basename(path_leg) not in modelos_salvos
    ):
        try:
            modelo_leg.save(path_leg)
            modelos_salvos.append(os.path.basename(path_leg))
        except Exception as e:
            erros.append(f"Modelo legado: {e}")
    if modelos_salvos:
        relatorio.append(f"🧠 Redes salvas: {', '.join(modelos_salvos)}")

    sandbox_path = getattr(bot, "arquivo_sandbox", None) or (
        getattr(bot, "cfg", {}) or {}
    ).get("arquivo_sandbox")
    if sandbox_path:
        try:
            lock_ops = getattr(bot, "_ops_lock", None)
            if lock_ops:
                with lock_ops:
                    ops_snap = dict(getattr(bot, "operacoes_abertas", {}) or {})
            else:
                ops_snap = dict(getattr(bot, "operacoes_abertas", {}) or {})

            os.makedirs(os.path.dirname(sandbox_path) or ".", exist_ok=True)
            with open(sandbox_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "operacoes_abertas": ops_snap,
                        "saldo": _safe_float(getattr(bot, "saldo_atual", 0)),
                        "snapshot_ts": time.time(),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            relatorio.append(f"📦 Sandbox salvo: {len(ops_snap)} posição(ões)")
        except Exception as e:
            erros.append(f"Sandbox: {e}")

    try:
        if hasattr(bot, "salvar_saldo"):
            bot.salvar_saldo()
        if hasattr(bot, "salvar_configuracoes_conta"):
            bot.salvar_configuracoes_conta()
        if hasattr(bot, "salvar_configuracoes_gerais"):
            bot.salvar_configuracoes_gerais()
        relatorio.append("💾 Configs e saldo persistidos")
    except Exception as e:
        erros.append(f"Config/saldo: {e}")

    if hasattr(bot, "log_msg"):
        bot.log_msg(f"{log_prefix} Checkpoint salvo. {' | '.join(relatorio)}")

    return relatorio, erros


@app.post("/api/snapshot", dependencies=[Depends(verify_api_key)])
async def criar_snapshot_neural():
    """
    [V90] Checkpoint de Memória: salva em disco o estado completo da IA e das posições abertas.
    Chamado pelo botão 'Snapshot Neural' do painel.
    """
    global lhn_bot
    if not lhn_bot:
        raise HTTPException(status_code=503, detail="Bot não inicializado.")

    try:
        relatorio, erros = await asyncio.to_thread(
            _executar_snapshot_neural_checkpoint_sync, lhn_bot, "📸 [SNAPSHOT]"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao executar snapshot: {e}")

    if erros and not relatorio:
        raise HTTPException(
            status_code=500, detail="Falha total no snapshot: " + " | ".join(erros)
        )

    return {
        "status": "ok",
        "message": (
            " | ".join(relatorio)
            if relatorio
            else "Snapshot concluído (sem modelos carregados ainda)."
        ),
        "detalhes": relatorio,
        "erros": erros,
        "timestamp": time.time(),
    }


def _graceful_shutdown_body_sync() -> None:
    """Checkpoint neural + SQLite WAL + sandbox; não mata o processo (o caller agenda exit)."""
    global lhn_bot
    import sqlite3

    bot = lhn_bot
    if not bot:
        return
    try:
        bot.is_app_alive = False
    except Exception:
        pass
    for _evname in (
        "_stop_event_noticias",
        "_stop_event_leverage",
        "_stop_event_top_ativos",
    ):
        _ev = getattr(bot, _evname, None)
        if _ev is not None:
            try:
                _ev.set()
            except Exception:
                pass
    try:
        if hasattr(bot, "reiniciar_websocket"):
            bot.reiniciar_websocket()
    except Exception:
        logging.exception("graceful_shutdown_ws_signal_failed")

    for _dbp in (
        getattr(bot, "arquivo_db_memoria", None),
    ):
        if _dbp and isinstance(_dbp, str) and os.path.isfile(_dbp):
            try:
                with sqlite3.connect(_dbp, timeout=10.0) as cx:
                    cx.execute("PRAGMA busy_timeout=8000")
                    cx.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                logging.exception("graceful_shutdown_sqlite_checkpoint_failed")

    try:
        _executar_snapshot_neural_checkpoint_sync(bot, "🔻 [SHUTDOWN]")
    except Exception:
        logging.exception("graceful_shutdown_snapshot_failed")
    try:
        if hasattr(bot, "forcar_salvamento_dados"):
            bot.forcar_salvamento_dados()
    except Exception:
        logging.exception("graceful_shutdown_forcar_salvamento_failed")


@app.post("/api/shutdown", dependencies=[Depends(verify_api_key)])
async def api_shutdown_graceful():
    """
    Desligamento limpo: fecha WebSockets do painel, checkpoint WAL/SQLite, snapshot .keras,
    sandbox e configs; depois encerra o processo FastAPI (sem taskkill externo).
    """
    global active_ws_clients
    wss = list(active_ws_clients)
    for ws in wss:
        try:
            await ws.close(code=1001, reason="LHN graceful shutdown")
        except Exception:
            pass
    active_ws_clients.clear()

    try:
        await asyncio.to_thread(_graceful_shutdown_body_sync)
    except Exception:
        logging.exception("graceful_shutdown_thread_failed")

    def _exit_clean():
        time.sleep(0.25)
        os._exit(0)

    threading.Thread(target=_exit_clean, daemon=True).start()
    return JSONResponse(
        content={"status": "shutting_down", "message": "Checkpoint concluído; processo a encerrar."},
        status_code=200,
    )


@app.post("/api/close", dependencies=[Depends(verify_api_key)])
async def close_position(payload: dict):
    """Encerra manualmente uma operação aberta via painel (HFT-safe)."""
    global lhn_bot
    symbol = payload.get("symbol")
    print(f"🚀 [SINAL] Comando de encerramento recebido para: {symbol}")
    if not _symbol_payload_valid(symbol):
        raise HTTPException(status_code=400, detail="Symbol inválido ou ausente.")
    if not lhn_bot:
        raise HTTPException(status_code=500, detail="Bot não inicializado.")
    try:
        if hasattr(lhn_bot, "encerrar_operacao_manual"):
            await lhn_bot.encerrar_operacao_manual(symbol.upper())
        else:
            raise HTTPException(
                status_code=500, detail="Função de encerramento manual indisponível."
            )
    except HTTPException:
        raise
    except Exception as e:
        if hasattr(lhn_bot, "erro_msg"):
            lhn_bot.erro_msg(f"Falha ao encerrar operação manual em {symbol}: {e}")
        raise HTTPException(status_code=500, detail="Erro ao encerrar operação manual.")
    return {"status": "ok"}


if __name__ == "__main__":
    try:
        print("[+] Iniciando LHN Sovereign V90 FINAL Backend...")
        uvicorn.run(
            "server:app",
            host="0.0.0.0",
            port=9002,  # Rotação tática de porta (9000 em uso)
            reload=False,
            log_level="info",
        )
    except KeyboardInterrupt:
        print("\n[!] Servidor interrompido pelo usuário.")
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar servidor: {e}")
        sys.exit(1)
