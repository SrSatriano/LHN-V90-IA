# EngineMixin auto-extracted
import concurrent.futures
import contextlib
import copy
import os
import sys
from collections import deque

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # Silenciar logs Keras/Adam
import asyncio
import json
import logging
import math
import re
import socket
import threading
import time
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import websockets
from bybit_helpers import (bybit_side_from_binance, format_bybit_exception,
                           get_linear_tickers_list_safe,
                           ws_kline_candle_to_row)
from config import *
from order_execution_gate import round_qty_to_step, validate_linear_open_order
from services.risk_limits import MAX_OPERACOES_SIMULTANEAS, obter_limites_risco

try:
    from pybit.exceptions import FailedRequestError
except ImportError:
    FailedRequestError = Exception  # type: ignore

logger = logging.getLogger(__name__)
try:
    from tensorflow.keras import backend as K
except Exception:
    K = None

# Bybit V5: mais de 10 args num único subscribe é descartado silenciosamente → chunk estrito.
_BYBIT_WS_SUBSCRIBE_MAX_ARGS = 10
_BYBIT_WS_SUB_CHUNK = _BYBIT_WS_SUBSCRIBE_MAX_ARGS
# Máximo de conexões WebSocket paralelas (Túnel 1) — anti–connection limit / ban de IP.
_BYBIT_WS_SHARD_MAX = 4
_BYBIT_WS_SYMBOLS_PER_SHARD = 25
MIN_TRADES_FOR_EVALUATION = 5
# Produção — linear público (não usar URL alternativa por engano no Túnel 1).
BYBIT_PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"

# Linear USDT: base 2–15 alfanum. (OPUSDT, BTCUSDT, 1000PEPEUSDT); 1 char base bloqueado; denylist reforço.
_BYBIT_LINEAR_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,15}USDT$")
_BYBIT_WS_SYMBOL_EXTRA_DENY = frozenset({"4USDT", "1USDT"})


def _install_uvloop_if_linux(log_fn=None) -> bool:
    """Ativa uvloop só em Linux; Windows mantém o event loop padrão."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        if log_fn is not None:
            log_fn("⚡ [Asyncio] uvloop ativado para ambiente Linux.")
        return True
    except Exception as e:
        if log_fn is not None:
            log_fn(f"⚠️ [Asyncio] uvloop indisponível; usando asyncio padrão: {e}")
        return False


def _is_transient_ws_error(exc: BaseException) -> bool:
    err = f"{type(exc).__name__} {exc}".lower()
    return any(
        token in err
        for token in (
            "winerror 121",
            "121",
            "semáforo",
            "semaphore",
            "connectionclosed",
            "connection close",
            "connection reset",
            "connection aborted",
            "timeout",
            "timed out",
            "keepalive",
            "1006",
        )
    )


def _ws_backoff_seconds(owner, attr: str, exc: BaseException, *, cap: float = 120.0) -> float:
    streak = int(getattr(owner, attr, 0) or 0) + 1
    setattr(owner, attr, streak)
    base = 1.0 if _is_transient_ws_error(exc) else 3.0
    return min(cap, base * (2 ** min(streak - 1, 7)))


def _safe_ws_json_dict(raw: str):
    """Parse JSON de frame WS; nunca levanta — retorna dict ou None."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _split_tickers_into_ws_shards(raw, max_shards=None, target_per_shard=None):
    """
    Divide tickers em até max_shards conexões (~target_per_shard símbolos por shard).
    Cap global = max_shards * target_per_shard (ex.: 100) para não saturar uma única WS.
    """
    if max_shards is None:
        max_shards = _BYBIT_WS_SHARD_MAX
    if target_per_shard is None:
        target_per_shard = _BYBIT_WS_SYMBOLS_PER_SHARD
    if not raw:
        return [["BTCUSDT"]]
    cap = max_shards * target_per_shard
    if len(raw) > cap:
        raw = raw[:cap]
    n = len(raw)
    need = max(1, (n + target_per_shard - 1) // target_per_shard)
    num_shards = min(max_shards, need)
    chunk = (n + num_shards - 1) // num_shards
    partitions = [raw[i : i + chunk] for i in range(0, n, chunk)]
    return partitions if partitions else [["BTCUSDT"]]


def _filter_valid_bybit_linear_symbols(tickers, log_queue=None):
    """Mantém só símbolos linear USDT plausíveis para subscribe WS; ordem preservada, sem duplicatas."""
    out = []
    seen = set()

    def _log_invalid(sym: str) -> None:
        if log_queue is not None:
            try:
                log_queue.put("⚠️ [Filtro] Símbolo inválido removido: " + sym)
            except Exception:
                logger.warning("Símbolo inválido removido: %s", sym)
        else:
            logger.warning("Símbolo inválido removido: %s", sym)

    for t in tickers or []:
        u = str(t).strip().upper()
        if not u:
            continue
        if not _BYBIT_LINEAR_SYMBOL_RE.match(u):
            _log_invalid(u)
            continue
        if u in _BYBIT_WS_SYMBOL_EXTRA_DENY:
            _log_invalid(u)
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out if out else ["BTCUSDT"]


def _log_bybit_subscribe_rejection(log_fn, data: dict) -> None:
    """Log quando a corretora devolve subscribe com success=false."""
    if not isinstance(data, dict):
        return
    if data.get("op") != "subscribe":
        return
    if data.get("success") is not False:
        return
    ret = data.get("ret_msg") or data.get("retMsg") or ""
    args = data.get("args")
    log_fn(f"⛔ [WS Erro] Subscrição rejeitada: ret_msg={ret!r} | args={args!r}")


def _normalize_bybit_linear_symbol(sym: str) -> str:
    """Bybit V5 tickers.{SYMBOL}: símbolo linear em maiúsculas (formato público linear)."""
    s = str(sym or "").strip().upper()
    return s if s else "BTCUSDT"


async def _bybit_app_ping_loop(ws):
    """Ping de aplicação Bybit V5 (obrigatório para manter pushes; além do ping WebSocket RFC)."""
    while True:
        try:
            await ws.send(json.dumps({"req_id": "heartbeat", "op": "ping"}))
            await asyncio.sleep(20)
        except Exception:
            break


def _log_subscribe_first_frame(log_fn, raw, lot: int) -> None:
    """Depuração: primeiro frame após subscribe — success vs erro Bybit."""
    if not raw:
        return
    rstrip = raw.strip()
    try:
        data = json.loads(rstrip)
    except Exception:
        if "success" in rstrip.lower():
            log_fn(f"✅ [WS] Subscrição confirmada para lote {lot} (success textual)")
        return
    if not isinstance(data, dict):
        return
    if data.get("success") is True:
        log_fn(f"✅ [WS] Subscrição confirmada para lote {lot}")
        return
    code = data.get("ret_code") or data.get("retCode") or data.get("code")
    msg = data.get("ret_msg") or data.get("retMsg") or data.get("msg")
    if data.get("success") is False or code not in (None, 0, "0"):
        log_fn(f"⛔ [WS] Bybit erro no lote {lot}: ret_code={code!r} ret_msg={msg!r}")


async def _bybit_ws_subscribe_chunked(
    ws,
    topic_args,
    log_fn,
    *,
    inter_chunk_delay: float = 0.5,
    chunk_size: int = _BYBIT_WS_SUB_CHUNK,
    ack_timeout: float = 12.0,
):
    """Subscribe em chunks (máx. 10 args por payload); após cada envio lê um frame, loga (ack) e responde ping se necessário."""
    n = len(topic_args)
    if n == 0:
        return
    await ws.send(json.dumps({"op": "ping"}))
    await asyncio.sleep(0.5)
    chunk_size = min(int(chunk_size), _BYBIT_WS_SUBSCRIBE_MAX_ARGS)
    if chunk_size < 1:
        chunk_size = _BYBIT_WS_SUBSCRIBE_MAX_ARGS
    for i in range(0, n, chunk_size):
        chunk = topic_args[i : i + chunk_size]
        sub_msg = json.dumps({"op": "subscribe", "args": chunk})
        log_fn(f"📤 [WS Outbound] enviando: {sub_msg}")
        await ws.send(sub_msg)
        await asyncio.sleep(inter_chunk_delay)
        lot = i // chunk_size + 1
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=ack_timeout)
        except asyncio.TimeoutError:
            log_fn(
                f"⚠️ [Bybit subscribe] timeout aguardando frame após lote {lot} "
                f"({len(chunk)} tópicos)"
            )
            continue
        _log_subscribe_first_frame(log_fn, raw, lot)
        log_fn(
            f"📬 [Bybit subscribe ack] lote {lot}/{max(1, (n + chunk_size - 1) // chunk_size)}: {raw[:1600]}"
        )
        data = _safe_ws_json_dict(raw)
        if data is None:
            continue
        _log_bybit_subscribe_rejection(log_fn, data)
        if data.get("op") == "ping":
            await ws.send(json.dumps({"op": "pong"}))
            try:
                raw2 = await asyncio.wait_for(ws.recv(), timeout=8.0)
                log_fn(
                    f"📬 [Bybit subscribe ack] lote {lot} (após pong): {raw2[:1600]}"
                )
            except asyncio.TimeoutError:
                log_fn(f"⚠️ [Bybit subscribe] sem 2º frame após pong (lote {lot})")


def run_isolated_ws_process(tickers, url_pub, price_queue, log_queue):
    """Processo blindado para WebSocket (Túnel 1). Imune ao GIL do Motor Neural."""
    import asyncio
    import contextlib
    import json
    import os
    import time

    import websockets

    try:
        import psutil

        _p = psutil.Process(os.getpid())
        if os.name == "nt":
            _p.nice(psutil.HIGH_PRIORITY_CLASS)
        else:
            _p.nice(-10)
    except Exception:
        pass

    # MATA O PING NATIVO DO PYTHON (Obrigatório para Bybit V5)
    _WS_CONNECT_KW = {
        "ping_interval": None,
        "ping_timeout": None,
        "close_timeout": 30,
        "open_timeout": 30,
        # Windows + DNS Bybit devolve IPv6 primeiro; forçamos IPv4 para evitar open_timeout fantasma.
        "family": socket.AF_INET,
    }

    def _ts():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def _ingest_ticker_row(data):
        topic = data.get("topic") or ""
        if not topic.startswith("tickers."):
            return
        d = data.get("data")
        if not isinstance(d, dict):
            return
        sym = d.get("symbol")
        lp = d.get("lastPrice") or d.get("markPrice")
        if sym and lp is not None:
            ts = int(data.get("ts") or d.get("timestamp") or (time.time() * 1000))
            price_queue.put({"sym": sym, "lp": float(lp), "ts": ts})

    def _iso_log(msg):
        log_queue.put(f"[{_ts()}] {msg}")

    async def _ws_loop(chunk_syms, ws_url):
        log_queue.put(
            f"🌐 [WS Isolado] Shard {len(chunk_syms)} símb.: {','.join(chunk_syms[:5])}..."
        )
        try:
            async with websockets.connect(ws_url, **_WS_CONNECT_KW) as ws:

                # 1. QUEBRA-GELO (Icebreaker Ping) - Força a Bybit a abrir o túnel
                icebreaker_msg = json.dumps({"req_id": "icebreaker", "op": "ping"})
                await ws.send(icebreaker_msg)
                try:
                    pong_inicial = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    log_queue.put(
                        f"🧊 [WS Handshake] Bybit respondeu: {pong_inicial[:100]}"
                    )
                except Exception as e:
                    log_queue.put(f"❌ [WS Handshake] Falha no Icebreaker: {e}")
                    return

                # 2. HEARTBEAT DE APLICAÇÃO (A cada 20s)
                async def _bybit_app_ping():
                    while True:
                        try:
                            await asyncio.sleep(20)
                            await ws.send(
                                json.dumps({"req_id": "heartbeat", "op": "ping"})
                            )
                        except Exception:
                            break

                hb_task = asyncio.create_task(_bybit_app_ping())

                try:
                    # 3. SUBSCRIÇÃO FRACIONADA (Max 10 tópicos) COM PREFIXO "tickers."
                    for i in range(0, len(chunk_syms), 10):
                        sub_chunk = chunk_syms[i : i + 10]
                        topics_prefixed = [f"tickers.{s}" for s in sub_chunk]
                        sub_payload = json.dumps(
                            {"op": "subscribe", "args": topics_prefixed}
                        )

                        log_queue.put(f"📤 [WS Sub] Enviando: {sub_payload}")
                        await ws.send(sub_payload)
                        await asyncio.sleep(0.5)

                    # 4. LOOP DE RECEPÇÃO (COM TIMEOUT DE 60s)
                    while True:
                        data_str = await asyncio.wait_for(ws.recv(), timeout=60.0)

                        if "pong" in data_str.lower():
                            # Spam HFT: só em nível debug (terminal/UI não inunda).
                            logger.debug(
                                "[WS] Pong da Aplicação recebido (Túnel Ativo)"
                            )
                            continue

                        if (
                            "success" in data_str.lower()
                            and "subscribe" in data_str.lower()
                        ):
                            log_queue.put(f"✅ [WS] Lote Confirmado: {data_str[:100]}")
                            continue

                        # Processa os preços e envia para a fila HFT
                        try:
                            parsed = json.loads(data_str)
                        except (json.JSONDecodeError, TypeError, ValueError) as ex:
                            log_queue.put(
                                "⚠️ [WS JSON] Falha ao decodificar JSON: payload inválido/HTML descartado"
                            )
                            log_queue.put(
                                f"⚠️ [WS Isolado] Payload inválido descartado: {ex!s} | raw={data_str[:120]}"
                            )
                            continue
                        if "topic" in parsed and parsed["topic"].startswith("tickers."):
                            price_queue.put(("TICKER", parsed))

                finally:
                    hb_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await hb_task

        except asyncio.TimeoutError:
            log_queue.put("⚠️ [WS Isolado] Timeout recv (shard). Reconectando...")
        except Exception as e:
            log_queue.put(f"⚠️ [WS Isolado] Erro na shard: {e}")

    async def _shard_ws_loop(symbols_shard, ws_url):
        """Wrapper de reconexão para uma shard do Túnel 1."""
        chunk_syms = _filter_valid_bybit_linear_symbols(
            symbols_shard, log_queue=log_queue
        )
        if not chunk_syms:
            chunk_syms = ["BTCUSDT"]
        ws_fails_tunnel_1 = 0
        while True:
            await _ws_loop(chunk_syms, ws_url)
            ws_fails_tunnel_1 += 1
            b_sleep = min(300, 2**ws_fails_tunnel_1)
            log_queue.put(
                f"[{_ts()}] ⚠️ [WS Isolado] Reconectando shard em {b_sleep}s..."
            )
            await asyncio.sleep(b_sleep)

    async def _ws_multi_shards():
        ws_url = BYBIT_PUBLIC_LINEAR_WS
        if str(url_pub or "").rstrip("/") != ws_url.rstrip("/"):
            log_queue.put(
                f"[{_ts()}] ⚠️ [WS Isolado] URL forçada para produção linear V5: {ws_url!r} (parâmetro era {url_pub!r})"
            )
        raw_in = [t for t in (tickers or []) if t] or ["BTCUSDT"]
        tickers_limpos = _filter_valid_bybit_linear_symbols(raw_in, log_queue=log_queue)
        if len(tickers_limpos) < len(raw_in):
            log_queue.put(
                f"[{_ts()}] ⚠️ [WS Isolado] Símbolos filtrados (RegEx V5 + denylist): "
                f"{len(raw_in)} → {len(tickers_limpos)}."
            )
        raw = tickers_limpos
        shards = _split_tickers_into_ws_shards(raw)
        log_queue.put(
            f"[{_ts()}] 🌐 [WS Isolado] {len(shards)} shard(s) (máx. {_BYBIT_WS_SHARD_MAX}), "
            f"{len(raw)} tickers válidos, subscribe_max_args={_BYBIT_WS_SUBSCRIBE_MAX_ARGS}, url={ws_url}."
        )
        tasks = []
        for i, sh in enumerate(shards):
            if i > 0:
                await asyncio.sleep(2.0)
            tasks.append(asyncio.create_task(_shard_ws_loop(sh, ws_url)))
        await asyncio.gather(*tasks)

    _install_uvloop_if_linux(lambda msg: log_queue.put(f"[{_ts()}] {msg}"))
    asyncio.run(_ws_multi_shards())


def _preco_ultimo_precos_buffer(precos_buffer, ativo, default=0.0):
    """Último preço por ativo (deque maxlen=100 ou float legado)."""
    if not precos_buffer:
        return default
    v = precos_buffer.get(ativo)
    if v is None:
        return default
    if isinstance(v, deque):
        return float(v[-1]) if len(v) else default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# Túneis 2–4: sem ping binário RFC; keepalive via _bybit_app_ping_loop {"op":"ping"}.
_WS_CONNECT_KW = {
    "ping_interval": None,
    "ping_timeout": None,
    "close_timeout": 60,
    "open_timeout": 60,
    # Mitiga timeout de conexão quando o host resolve primeiro para IPv6 e a rota local está quebrada.
    "family": socket.AF_INET,
}


class EngineMixin:
    def _abortar_se_erro_autenticacao_api(self, e: BaseException) -> bool:
        erro_str = str(e).lower()
        if (
            "api secret required" in erro_str
            or "invalid api-key" in erro_str
            or "-2015" in erro_str
        ):
            self.erro_msg(
                "FALHA CRÍTICA: Chaves da API ausentes ou inválidas. Abortando ação para evitar loop infinito."
            )
            return True
        return False

    def _append_historico_scale_out(self, ativo: str, op: dict) -> None:
        """Regista realização parcial no histórico exibido no painel (não altera total_trades)."""
        if not hasattr(self, "historico_operacoes"):
            self.historico_operacoes = []
        hora = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime())
        tipo = str(op.get("tipo", "LONG")).upper()
        self.historico_operacoes.insert(
            0,
            {
                "hora": hora,
                "ativo": ativo,
                "tipo": tipo,
                "resultado": "SCALE_OUT (realização parcial)",
                "lucro": 0.0,
                "pnl": 0.0,
                "pnl_pct": 0.0,
                "profit": 0.0,
                "margem_gasta": float(op.get("margem", 0.0) or 0.0),
                "alavancagem": int(op.get("alav") or op.get("alavancagem") or 1),
                "certeza": float(op.get("certeza", 0.0) or 0.0),
                "ts": time.time(),
                "arena_regime": op.get("arena_regime", "SNIPER"),
            },
        )
        if len(self.historico_operacoes) > 1000:
            self.historico_operacoes.pop()
        self.operacoes_finalizadas = list(self.historico_operacoes)
        if hasattr(self, "persistir_historico_operacoes"):
            try:
                self.persistir_historico_operacoes()
            except Exception:
                logger.exception("persist_historico_scale_out_failed")
        if hasattr(self, "request_ws_immediate_update"):
            try:
                self.request_ws_immediate_update()
            except Exception:
                pass

    def _ensure_close_retry_state(self) -> None:
        if not hasattr(self, "_close_retry_queue") or self._close_retry_queue is None:
            self._close_retry_queue = deque()
        if not hasattr(self, "_close_retry_lock") or self._close_retry_lock is None:
            self._close_retry_lock = threading.Lock()

    def _limpar_acumuladores_virtuais_pos_forja(self) -> None:
        """Remove perdas temporárias geradas por forja/replay sem apagar histórico real."""
        if hasattr(self, "historico_recente"):
            self.historico_recente = []

    def _treinamento_governanca_concluido(self) -> bool:
        return bool(
            getattr(
                self,
                "treinamento_concluido",
                getattr(self, "ia_treinada", False),
            )
        )

    def _drawdown_sessao_pode_avaliar(self) -> bool:
        if not self._treinamento_governanca_concluido():
            return False
        if not getattr(self, "is_real_account_mode", lambda: False)():
            return True
        if not bool(getattr(self, "_saldo_real_confirmado", False)):
            return False
        if not bool(getattr(self, "_ws_price_feed_confirmed", False)):
            return False
        return float(getattr(self, "saldo_real", 0.0) or 0.0) > 0.0

    def _enqueue_close_retry(
        self,
        *,
        ativo: str,
        lado_original: str,
        qty: float,
        motivo: str,
        attempts: int = 0,
        next_retry_ts: float | None = None,
    ) -> None:
        self._ensure_close_retry_state()
        if next_retry_ts is None:
            next_retry_ts = time.time() + min(60.0, 5.0 * (2**attempts))
        item = {
            "ativo": str(ativo).strip().upper(),
            "lado_original": str(lado_original).strip().upper(),
            "qty": float(qty or 0.0),
            "attempts": int(attempts),
            "next_retry_ts": float(next_retry_ts),
            "motivo": str(motivo or ""),
        }
        with self._close_retry_lock:
            filtered = deque(
                [
                    q
                    for q in self._close_retry_queue
                    if str(q.get("ativo")) != item["ativo"]
                ],
                maxlen=256,
            )
            filtered.append(item)
            self._close_retry_queue = filtered
        self.erro_msg(
            f"⚠️ [CLOSE RETRY] {item['ativo']} enfileirado para retentativa #{item['attempts'] + 1}: {item['motivo']}"
        )

    def _formatar_alerta_fechamento_operacao(
        self, ativo: str, pnl_usd: float, pnl_pct: float, motivo: str
    ) -> str:
        pnl_val = float(pnl_usd or 0.0)
        pnl_pct_val = float(pnl_pct or 0.0)
        if pnl_val > 0:
            return (
                f"🟢 [OPERAÇÃO ENCERRADA - WIN]\n"
                f"Ativo: {ativo}\n"
                f"Lucro: +{pnl_val:.2f} USDT (+{pnl_pct_val:.2f}%)\n"
                f"Motivo: {motivo}"
            )
        return (
            f"🔴 [OPERAÇÃO ENCERRADA - LOSS]\n"
            f"Ativo: {ativo}\n"
            f"Prejuízo: -{abs(pnl_val):.2f} USDT ({pnl_pct_val:.2f}%)\n"
            f"Motivo: {motivo}"
        )

    def _registrar_fechamento_sessao(
        self,
        *,
        ativo: str,
        op: dict,
        resultado: str,
        pnl_hist: float,
        pnl_pct_hist: float,
        preco_saida: float,
        origem_label: str,
        op_tipo: str | None = None,
        margem_usada: float | None = None,
        alav: float | None = None,
        certeza: float | None = None,
        arena_regime: str | None = None,
        prob_ia: float | None = None,
        atr_op: float | None = None,
        registrar_replay: bool = True,
    ) -> None:
        hora = datetime.now().strftime("%H:%M:%S")
        ativo = str(ativo or "").strip().upper()
        op_tipo = str(op_tipo or op.get("tipo", "LONG")).upper()
        margem_usada = float(
            margem_usada if margem_usada is not None else op.get("margem", 0.0) or 0.0
        )
        alav = float(
            alav
            if alav is not None
            else op.get("alav", op.get("alavancagem", self.cfg.get("alavancagem", 20)))
            or 20.0
        )
        certeza = float(
            certeza if certeza is not None else op.get("certeza", 0.0) or 0.0
        )
        arena_regime = str(arena_regime or op.get("arena_regime", "SNIPER"))
        prob_ia = float(prob_ia if prob_ia is not None else op.get("ia_prob", 0.0) or 0.0)
        atr_op = float(atr_op if atr_op is not None else op.get("atr_dca", 1.0) or 1.0)
        lucro = float(pnl_hist)

        arena_u = str(arena_regime or "").upper()
        t_open = float(op.get("ts_abertura") or 0.0)
        if t_open <= 0.0 and op.get("timestamp") is not None:
            try:
                t_open = float(op["timestamp"]) / 1000.0
            except (TypeError, ValueError):
                t_open = 0.0
        dur_sec = max(0.0, time.time() - t_open) if t_open > 0.0 else 0.0
        res_u = str(resultado or "").upper()
        _ej = "EJEÇÃO" in res_u or "GUARDIÃO" in res_u or "PANIC" in res_u
        _skip_tg_saida = arena_u == "ARBITRAGEM" or (_ej and dur_sec < 60.0)
        if _ej and dur_sec < 60.0:
            _m = getattr(self, "_telegram_vip_suppress_until_symbol", None)
            if not isinstance(_m, dict):
                _m = {}
                self._telegram_vip_suppress_until_symbol = _m
            _m[ativo] = time.time() + 60.0

        try:
            if hasattr(self, "formatar_alerta_fechamento_operacao"):
                msg_saida = self.formatar_alerta_fechamento_operacao(
                    ativo, lucro, float(pnl_pct_hist), resultado
                )
            else:
                msg_saida = self._formatar_alerta_fechamento_operacao(
                    ativo, lucro, float(pnl_pct_hist), resultado
                )
            if (
                not _skip_tg_saida
                and hasattr(self, "loop")
                and self.loop
            ):
                asyncio.run_coroutine_threadsafe(
                    self.enviar_alerta_telegram(msg_saida), self.loop
                )
        except Exception as e:
            print(f"❌ [TELEGRAM ERROR] {e}")

        if not hasattr(self, "historico_operacoes") or self.historico_operacoes is None:
            self.historico_operacoes = []
        _pe = float(
            op.get("preco_entrada")
            or op.get("entry_price")
            or op.get("entry")
            or 0.0
        )
        entry = {
            "hora": hora,
            "ativo": ativo,
            "tipo": op_tipo,
            "resultado": resultado,
            "lucro": float(lucro),
            "pnl": float(lucro),
            "pnl_pct": float(pnl_pct_hist),
            "profit": float(lucro),
            "margem_gasta": float(margem_usada),
            "alavancagem": int(alav),
            "certeza": float(certeza),
            "ts": time.time(),
            "arena_regime": arena_regime,
            "preco_entrada": _pe,
            **self._metadados_fechamento_posicao(float(preco_saida or 0.0)),
        }
        self.historico_operacoes.insert(0, entry)
        if len(self.historico_operacoes) > 1000:
            self.historico_operacoes.pop()
        self.operacoes_finalizadas = list(self.historico_operacoes)
        if hasattr(self, "_calcular_estatisticas"):
            try:
                self.desempenho_sessao = self._calcular_estatisticas()
            except Exception:
                pass

        self.total_trades += 1
        if lucro > 0:
            self.total_wins += 1
            self.total_profit_usd += lucro
        else:
            self.total_losses += 1
            self.total_loss_usd += abs(lucro)

        if not hasattr(self, "historico_recente") or self.historico_recente is None:
            self.historico_recente = []
        self.historico_recente.append(1 if lucro > 0 else 0)
        if len(self.historico_recente) > 10:
            self.historico_recente.pop(0)
        if len(self.historico_recente) < MIN_TRADES_FOR_EVALUATION:
            winrate_rec = None
        else:
            winrate_rec = sum(self.historico_recente) / len(self.historico_recente)
        if (
            len(self.historico_recente) >= MIN_TRADES_FOR_EVALUATION
            and winrate_rec is not None
            and winrate_rec <= 0.4
        ):
            self.log_msg(
                f"⚕️ SISTEMA DE AUTOCURA ATIVADO: Winrate recente caiu para {winrate_rec*100:.1f}%..."
            )
            self.historico_recente.clear()
            self.is_searching = False
            self.ia_treinada = False
            self.treinamento_concluido = False
            if self.arquivo_cerebro and os.path.exists(self.arquivo_cerebro):
                if K is not None:
                    K.clear_session()
                try:
                    os.remove(self.arquivo_cerebro)
                except OSError:
                    pass
            if hasattr(self, "forjar_ia_sniper_nexus"):
                self.forjar_ia_sniper_nexus()
            elif hasattr(self, "submit_background_task"):
                self.submit_background_task(self.treinar_ia)

        lucro_rl = float(lucro)
        if self.cfg.get("use_reward_shaping", True) and atr_op > 0:
            lucro_rl = max(-3.0, min(3.0, lucro / atr_op))

        self._radar_enqueue_close_disk_io(
            {
                "hora": hora,
                "ativo": ativo,
                "op_tipo": op_tipo,
                "resultado": resultado,
                "lucro": lucro,
                "prob_ia": prob_ia,
                "lucro_rl": lucro_rl,
                "res_simplificado": 1 if lucro > 0 else 0,
                "arquivo_historico": getattr(self, "arquivo_historico", None),
                "workspace_raiz": getattr(self, "workspace_raiz", None),
                "registrar_replay": registrar_replay,
            }
        )

        if hasattr(self, "persistir_historico_operacoes"):
            try:
                self.persistir_historico_operacoes()
            except Exception:
                logger.exception("persist_historico_operacoes_failed")

        if hasattr(self, "espelhar_operacao_historico_sqlite"):
            try:
                self.espelhar_operacao_historico_sqlite(entry)
            except Exception:
                logger.exception("espelhar_operacao_historico_sqlite_failed")

        self.log_msg(
            f"RESULTADO FINALIZADO: {hora} - {ativo} - {op_tipo} - {resultado} - Lucro: {lucro:+.2f}"
        )
        modo = "REAL" if getattr(
            self,
            "is_real_account_mode",
            lambda: bool(getattr(self, "modo_real", False)),
        )() else "SIMULAÇÃO"
        self.log_msg(
            f"🏁 ORDEM ENCERRADA ({modo} / {origem_label}): {ativo} | {resultado} | Lucro: {lucro:+.2f}"
        )
        if hasattr(self, "request_ws_immediate_update"):
            try:
                self.request_ws_immediate_update()
            except Exception:
                pass

    async def _loop_retry_close_orders(self):
        self._ensure_close_retry_state()
        while getattr(self, "is_app_alive", True):
            try:
                await asyncio.sleep(3.0)
                if not getattr(
                    self,
                    "is_real_account_mode",
                    lambda: bool(getattr(self, "modo_real", False)),
                )():
                    continue
                now = time.time()
                ready = []
                with self._close_retry_lock:
                    pending = deque(maxlen=256)
                    while self._close_retry_queue:
                        item = self._close_retry_queue.popleft()
                        if float(item.get("next_retry_ts", 0.0) or 0.0) <= now:
                            ready.append(item)
                        else:
                            pending.append(item)
                    self._close_retry_queue = pending
                for item in ready:
                    ok = await asyncio.to_thread(
                        self.fechar_ordem_real,
                        item.get("ativo"),
                        item.get("lado_original"),
                        item.get("qty", 0.0),
                        False,
                    )
                    if ok:
                        self.log_msg(
                            f"✅ [CLOSE RETRY] Fechamento reenviado com sucesso: {item.get('ativo')}"
                        )
                        continue
                    attempts = int(item.get("attempts", 0) or 0) + 1
                    if attempts >= 5:
                        self.erro_msg(
                            f"❌ [CLOSE RETRY] Abandonando retentativa de fechamento para {item.get('ativo')} após {attempts} falhas."
                        )
                        continue
                    self._enqueue_close_retry(
                        ativo=item.get("ativo", ""),
                        lado_original=item.get("lado_original", ""),
                        qty=float(item.get("qty", 0.0) or 0.0),
                        motivo=item.get("motivo", "close_retry"),
                        attempts=attempts,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("loop_retry_close_orders")

    def iniciar_motor_assincrono(self):
        self.log_msg(
            "⚡ [Motor] Iniciando loop asyncio dedicado (HFT) — WebSockets + watchdogs..."
        )
        _install_uvloop_if_linux(getattr(self, "log_msg", None))
        self._ws_price_last_tick_ts = time.time()
        self._ws_rest_fallback_last_log = 0.0
        self._ws_orderbook_fail_streak = 0
        self._ws_vpin_fail_streak = 0
        self._ensure_close_retry_state()
        self.loop_async = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop_async)
        self._ws_orders = None
        self._ws_orders_lock = asyncio.Lock()
        self.log_msg(
            "⚡ [Motor] Event loop criado — entrando no orchestrator_assincrono()."
        )
        self.loop_async.run_until_complete(self.orchestrator_assincrono())

    async def _orchestrator_supervised(self, name: str, coro_fn):
        """Reinicia automaticamente uma corrotina do motor se falhar isoladamente."""
        while getattr(self, "is_app_alive", True):
            try:
                await coro_fn()
                logger.warning(
                    "orchestrator_task_exited_cleanly name=%s — restarting in 2s", name
                )
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "orchestrator_task_crashed name=%s — restarting in 3s", name
                )
                await asyncio.sleep(3.0)

    async def orchestrator_assincrono(self):
        self.log_msg("⚡ [Orchestrator] Motor Assíncrono Ativado (Asyncio + WS).")
        self.log_msg(
            "⚡ [Orchestrator] Etapa 1/2: garantindo canal de ordens / estado REST..."
        )
        await self._ensure_ws_orders_connection()
        self.log_msg(
            "⚡ [Orchestrator] Etapa 2/2: despachando tarefas paralelas (WS + relatório + guardião + tribunal + REST)."
        )
        _specs = [
            ("websocket", self.loop_websocket_nativo),
            ("relatorio_diario", self._loop_relatorio_diario),
            ("guardiao_shadow", self._loop_guardiao_shadow),
            ("tribunal_quantitativo", self._loop_tribunal_quantitativo),
            ("rest_price_fallback", self._loop_rest_price_fallback),
            ("retry_close_orders", self._loop_retry_close_orders),
            ("binance_oracle_tunnel5", self._loop_binance_oracle_tunnel5),
            ("onchain_tracker_tunnel6", self._loop_onchain_tracker_tunnel6),
            ("evolucao_neural_1h", self._ciclo_evolucao_neural_1h),
            ("experience_replay", self._loop_experience_replay),
        ]
        tarefas = [
            asyncio.create_task(self._orchestrator_supervised(n, fn))
            for n, fn in _specs
        ]
        try:
            results = await asyncio.gather(*tarefas, return_exceptions=True)
            for (n, _), r in zip(_specs, results, strict=True):
                if isinstance(r, Exception):
                    logger.error(
                        "orchestrator_gather_residual name=%s err=%s",
                        n,
                        r,
                        exc_info=r,
                    )
        finally:
            await self._close_ws_orders_connection()

    async def _loop_binance_oracle_tunnel5(self):
        """
        Túnel 5: Binance Futures bookTicker (BTCUSDT) — read-only, paralelo aos WS Bybit.
        Não envia ordens; não toca no córtex; estado em services.binance_oracle.
        """
        try:
            from services.binance_oracle import run_binance_oracle_forever

            await run_binance_oracle_forever(
                should_run=lambda: getattr(self, "is_app_alive", True)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("binance_oracle_tunnel5_fatal")

    async def _loop_onchain_tracker_tunnel6(self):
        """
        Túnel 6: polling on-chain mock (aiohttp) — read-only, não bloqueia WS Bybit.
        Estado em services.onchain_tracker.
        """
        try:
            from services.onchain_tracker import run_onchain_tracker_forever

            await run_onchain_tracker_forever(
                should_run=lambda: getattr(self, "is_app_alive", True)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("onchain_tracker_tunnel6_fatal")

    async def _ciclo_evolucao_neural_1h(self):
        """[V90] Aprendizado Contínuo de 1 Hora (Experience Replay)"""
        while getattr(self, "is_app_alive", True):
            try:
                # Aguarda exatos 60 minutos
                await asyncio.sleep(3600)

                if getattr(self, "estrategia_rodando", False) and hasattr(
                    self, "forjar_memoria_recente"
                ):
                    self.log_msg(
                        "🧠 [NEXUS OMNISCIENCE] Iniciando Ciclo de Experiência Replay (1H)..."
                    )
                    # Chama a forja em thread separada para não bloquear o radar
                    await asyncio.to_thread(self.forjar_memoria_recente)
                    self._limpar_acumuladores_virtuais_pos_forja()
                    try:
                        import gc

                        await asyncio.to_thread(gc.collect)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.erro_msg(f"Falha no Ciclo de Evolução Neural: {e}")
                await asyncio.sleep(60)

    async def _loop_experience_replay(self):
        """Replay Buffer SQLite (últimos 5000 com reward) → fit só reservas; desfasado 30 min do ciclo Parquet."""
        await asyncio.sleep(1800)
        while getattr(self, "is_app_alive", True):
            try:
                await asyncio.sleep(3600)
                if getattr(self, "estrategia_rodando", False) and getattr(
                    self, "ia_treinada", False
                ):
                    if hasattr(self, "_replay_buffer_experience_reservas"):
                        self.log_msg(
                            "🧬 [XP_REPLAY] Ciclo SQLite (arena reservas, até 5000 linhas)..."
                        )
                        await asyncio.to_thread(self._replay_buffer_experience_reservas)
                        self._limpar_acumuladores_virtuais_pos_forja()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.erro_msg(f"[XP_REPLAY] loop: {e}")
                await asyncio.sleep(60)

    async def _ensure_ws_orders_connection(self):
        """Bybit V5: execução de ordens via REST (pybit). Sem WS-FAPI Binance."""
        return None

    async def _close_ws_orders_connection(self):
        self._ws_orders = None

    async def _loop_relatorio_diario(self):
        while getattr(self, "is_app_alive", True):
            try:
                now = datetime.now()
                tomorrow = now.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ) + timedelta(days=1)
                segundos = (tomorrow - now).total_seconds()
                if segundos <= 0:
                    segundos = 1
                await asyncio.sleep(segundos)

                saldo_disponivel = float(getattr(self, "saldo_atual", 0.0))
                saldo_inicial = float(
                    getattr(
                        self,
                        "_saldo_inicial_sessao",
                        saldo_disponivel if saldo_disponivel != 0 else 1.0,
                    )
                )
                if saldo_inicial <= 0:
                    crescimento_percentual = 0.0
                else:
                    crescimento_percentual = (
                        (saldo_disponivel - saldo_inicial) / saldo_inicial
                    ) * 100.0

                vitorias = int(getattr(self, "total_wins", 0))
                derrotas = int(getattr(self, "total_losses", 0))
                total_ops = vitorias + derrotas
                winrate = (vitorias / total_ops * 100) if total_ops > 0 else 0
                msg_diaria = f"📊 **Relatório Diário (Todo dia às 00h):**\n\n📈 RESUMO V90 FINAL\nSaldo Atual: ${saldo_disponivel:.2f}\nCrescimento: {crescimento_percentual:+.2f}%\nWinrate do Dia: {winrate:.0f}%\nOperações Realizadas: {total_ops}"
                if hasattr(self, "loop") and self.loop:
                    asyncio.run_coroutine_threadsafe(
                        self.enviar_alerta_telegram(msg_diaria), self.loop
                    )
            except Exception as e:
                print(f"❌ [TELEGRAM ERROR] {e}")
                # Nunca deve derrubar o motor assíncrono
                await asyncio.sleep(60)

    async def _loop_guardiao_shadow(self):
        """[V90] Guardião em Modo Sombra: Avalia saídas sem executar."""
        self.log_msg(
            "🛡️ Guardião Sombra (V90) ativado. Observando posições do banco de reservas..."
        )
        # [FIX] Throttle por símbolo: evita flood de logs para a mesma condição
        _shadow_last_log: dict = {}
        _SHADOW_LOG_COOLDOWN = 30  # segundos
        while getattr(self, "is_app_alive", True):
            try:
                await asyncio.sleep(3)
                with self._ops_lock:
                    abertas = dict(getattr(self, "operacoes_abertas", {}))

                if not abertas:
                    continue

                for ativo, op in abertas.items():
                    p_entrada = float(op.get("preco", 0))
                    p_atual = getattr(self, "precos_atuais", {}).get(ativo, p_entrada)
                    if p_atual == 0 or p_entrada == 0:
                        continue

                    direcao = op.get("tipo", "LONG")
                    if direcao == "LONG":
                        pnl_percent = ((p_atual - p_entrada) / p_entrada) * 100
                    else:
                        pnl_percent = ((p_entrada - p_atual) / p_entrada) * 100

                    if op.get("_breakeven_protegido") and pnl_percent > 0:
                        continue

                    hard_stop_hit = self._guardian_hard_stop_hit(
                        op, pnl_percent, p_atual
                    )
                    if self._guardian_grace_active(op) and not hard_stop_hit:
                        continue

                    vd = getattr(self, "vpin_data", {}).copy().get(ativo, {})
                    bv = float(vd.get("buy_vol", 0.0))
                    sv = float(vd.get("sell_vol", 0.0))
                    vpin_atual = (100.0 * bv / (bv + sv)) if (bv + sv) > 0 else 50.0
                    vpin_comprador_pct = (bv / (bv + sv)) if (bv + sv) > 0 else 0.5
                    vpin_vendedor_pct = (sv / (bv + sv)) if (bv + sv) > 0 else 0.5

                    acao_sugerida = "HOLD"
                    motivo = "Tendência estável"
                    usou_neural = False

                    # [Fase 3] Gatilho de Sobrevivência (Reflexo Medular)
                    acionou_reflexo = False
                    if hard_stop_hit:
                        acao_sugerida = "PANIC_SELL"
                        motivo = "Hard Stop Máximo atingido durante grace period"
                        acionou_reflexo = True
                        usou_neural = True
                    elif direcao == "LONG" and vpin_vendedor_pct >= 0.85:
                        acao_sugerida = "SCALE_OUT"
                        motivo = "🛡️ GUARDIÃO REFLEXO: Pico de Toxicidade VPIN (>85%) detectado contra a posição LONG. SCALE-OUT DE EMERGÊNCIA (50%) executado para blindagem de capital."
                        acionou_reflexo = True
                    elif direcao == "SHORT" and vpin_comprador_pct >= 0.85:
                        acao_sugerida = "SCALE_OUT"
                        motivo = "🛡️ GUARDIÃO REFLEXO: Pico de Toxicidade VPIN (>85%) detectado contra a posição SHORT. SCALE-OUT DE EMERGÊNCIA (50%) executado para blindagem de capital."
                        acionou_reflexo = True

                    # [V90] Custo de oportunidade (tempo) — scalping: liberta capital de posições "zumbis"
                    if not acionou_reflexo and self._guardiao_custo_oportunidade(op, p_atual, pnl_percent):
                        acao_sugerida = "PANIC_SELL"
                        motivo = (
                            "Rede Neural (Exaustão de Tempo / Custo de Oportunidade)"
                        )
                        usou_neural = True

                    # [V90] Integração do Córtex Neural Guardião (Controlo Real)
                    elif (
                        hasattr(self, "modelo_guardiao")
                        and getattr(self, "modelo_guardiao", None) is not None
                    ):
                        import time

                        import numpy as np

                        try:
                            adx_val = 30.0
                            vol_val = 1.0
                            if (
                                hasattr(self, "indicadores_cache")
                                and ativo in self.indicadores_cache
                            ):
                                ind = self.indicadores_cache[ativo]
                                adx_val = ind.get("ADX", 30.0)
                                vol_val = ind.get("ATR", 1.0)

                            tempo_aberto = float(self._minutos_em_operacao(op))

                            estado_tensor = np.array(
                                [
                                    [
                                        float(pnl_percent),
                                        float(vpin_atual),
                                        float(adx_val),
                                        float(tempo_aberto),
                                        float(vol_val),
                                    ]
                                ]
                            )

                            if getattr(self, "_ai_process_pool", None) is None:
                                from concurrent.futures import ProcessPoolExecutor
                                self._ai_process_pool = ProcessPoolExecutor(max_workers=2)
                            
                            from workers.ai_worker import predict_guardiao_isolated
                            arquivo_g = getattr(self, "arquivo_cerebro_guardiao", None)

                            q_values = await asyncio.get_running_loop().run_in_executor(
                                self._ai_process_pool,
                                predict_guardiao_isolated,
                                arquivo_g,
                                estado_tensor
                            )
                            max_idx = np.argmax(q_values)

                            if max_idx == 1:
                                acao_sugerida = "SCALE_OUT"
                                motivo = "Rede Neural (Realização Parcial)"
                            elif max_idx == 2:
                                acao_sugerida = "PANIC_SELL"
                                motivo = "Rede Neural (Ejeção Imediata)"
                            else:
                                acao_sugerida = "HOLD"
                                motivo = "Rede Neural (Manter Posição)"

                            # Nexus HFT V90: Zona de Tolerância de Spread (ignorar ruído do tensor).
                            if acao_sugerida in ["PANIC_SELL", "SCALE_OUT"] and abs(
                                pnl_percent
                            ) < 0.15:
                                acao_sugerida = "HOLD"
                                motivo = (
                                    f"Zona de Ruído de Spread ({pnl_percent:.3f}%). "
                                    "Tensor ignorado."
                                )

                            usou_neural = True
                        except Exception as e:
                            self.log_msg(f"⚠️ [GUARDIÃO] Erro na inferência neural: {e}")

                    if not usou_neural:
                        # Fallback estático
                        if (
                            pnl_percent > 1.0
                            and vpin_atual > 98.0
                            and direcao == "LONG"
                        ):
                            acao_sugerida = "HOLD_AGRESSIVO"
                            motivo = "Surfando Pump Institucional"
                        elif (
                            pnl_percent > 0.5
                            and vpin_atual < 20.0
                            and direcao == "LONG"
                        ):
                            acao_sugerida = "PANIC_SELL"
                            motivo = "Reversão de Fluxo detectada (DUMP iminente)"
                        elif (
                            pnl_percent < -1.0
                            and vpin_atual < 30.0
                            and direcao == "LONG"
                        ):
                            acao_sugerida = "PANIC_SELL"
                            motivo = "Corte de Perdas Dinâmico"

                    if acao_sugerida in ["PANIC_SELL", "SCALE_OUT"] and not self._guardian_survival_allows_panic(
                        op, pnl_percent, motivo
                    ):
                        acao_sugerida = "HOLD"
                        motivo = (
                            "Sobrevivência (grace); micro-oscilação ignorada "
                            "(PANIC_SELL/SCALE_OUT)"
                        )

                    if acao_sugerida != "HOLD":
                        try:
                            arquivo_bd = getattr(self, "arquivo_db_memoria", None)
                            if not arquivo_bd:
                                continue

                            def _persistir_shadow(conn):
                                cursor = conn.cursor()
                                cursor.execute("PRAGMA journal_mode=WAL;")
                                cursor.execute("PRAGMA synchronous=NORMAL;")
                                cursor.execute(
                                    """
                                        INSERT INTO guardiao_shadow_log (ativo, pnl_atual, vpin_atual, acao_sugerida, motivo)
                                        VALUES (?, ?, ?, ?, ?)
                                    """,
                                    (
                                        ativo,
                                        pnl_percent,
                                        vpin_atual,
                                        acao_sugerida,
                                        motivo,
                                    ),
                                )
                                conn.commit()

                            self.enqueue_deep_memory_write(
                                _persistir_shadow,
                                max_attempts=6,
                                base_delay=0.12,
                                timeout=15.0,
                            )

                            if acao_sugerida in [
                                "PANIC_SELL",
                                "SCALE_OUT",
                                "CLOSE_100",
                            ]:
                                shadow_only = bool(
                                    self.cfg.get("guardian_shadow_only", False)
                                )
                                if not shadow_only:
                                    if acao_sugerida != "SCALE_OUT":
                                        self.log_msg(
                                            f"🛡️ [GUARDIÃO EXECUTOR] Tensor disparou {acao_sugerida} em {ativo} (PnL: {pnl_percent:.2f}%). Motivo: {motivo}"
                                        )
                                    with self._ops_lock:
                                        if ativo in getattr(
                                            self, "operacoes_abertas", {}
                                        ):
                                            if acao_sugerida in [
                                                "PANIC_SELL",
                                                "CLOSE_100",
                                            ]:
                                                self.operacoes_abertas[ativo][
                                                    "fechar_agora"
                                                ] = f"EJEÇÃO NEURAL (GUARDIÃO - {motivo})"
                                                if "Custo de Oportunidade" in str(
                                                    motivo
                                                ):
                                                    self.operacoes_abertas[ativo][
                                                        "custo_oportunidade_feito"
                                                    ] = True
                                            elif (
                                                acao_sugerida == "SCALE_OUT"
                                                and not self.operacoes_abertas[
                                                    ativo
                                                ].get("scale_out_guardian_feito")
                                            ):
                                                self.log_msg(
                                                    f"🛡️ [GUARDIÃO EXECUTOR] Tensor disparou {acao_sugerida} em {ativo} (PnL: {pnl_percent:.2f}%). Motivo: {motivo}"
                                                )
                                                self.operacoes_abertas[ativo][
                                                    "scale_out_guardian_feito"
                                                ] = True
                                                direcao_op = self.operacoes_abertas[
                                                    ativo
                                                ].get("tipo", "LONG")

                                                def _run_scale_g():
                                                    try:
                                                        if not getattr(
                                                            self,
                                                            "is_real_account_mode",
                                                            lambda: False,
                                                        )():
                                                            return
                                                        client = getattr(
                                                            self,
                                                            "get_bybit_client",
                                                            lambda: None,
                                                        )()
                                                        if not client:
                                                            return
                                                        qty_atual = float(
                                                            self.operacoes_abertas[
                                                                ativo
                                                            ]["qtd"]
                                                        )
                                                        qty_fechar = str(
                                                            round(qty_atual * 0.5, 4)
                                                        )
                                                        side_close = (
                                                            "Sell"
                                                            if direcao_op == "LONG"
                                                            else "Buy"
                                                        )
                                                        client.place_order(
                                                            category="linear",
                                                            symbol=ativo,
                                                            side=side_close,
                                                            orderType="Market",
                                                            qty=qty_fechar,
                                                            reduceOnly=True,
                                                        )
                                                        self.log_msg(
                                                            f"💸 [GUARDIÃO EXECUTOR] Scale-Out disparado na Bybit! {qty_fechar} contratos."
                                                        )
                                                    except Exception as e:
                                                        self.erro_msg(
                                                            f"Erro Scale-Out Guardião na Bybit: {e}"
                                                        )

                                                if hasattr(self, "enqueue_disk_io"):
                                                    self.enqueue_disk_io(_run_scale_g)

                                                self.operacoes_abertas[ativo]["qtd"] = (
                                                    str(
                                                        float(
                                                            self.operacoes_abertas[
                                                                ativo
                                                            ]["qtd"]
                                                        )
                                                        * 0.5
                                                    )
                                                )
                                else:
                                    # [FIX] Throttle: só loga se passaram >30s desde o último log para este ativo
                                    import time as _time

                                    _agora = _time.monotonic()
                                    if (
                                        _agora - _shadow_last_log.get(ativo, 0)
                                        >= _SHADOW_LOG_COOLDOWN
                                    ):
                                        _shadow_last_log[ativo] = _agora
                                        self.log_msg(
                                            f"👻 [SHADOW GUARDIÃO] Teria disparado {acao_sugerida} (PnL: {pnl_percent:.2f}%). Motivo: {motivo}"
                                        )
                        except Exception:
                            pass
            except Exception:
                pass

    async def _loop_tribunal_quantitativo(self):
        """[V90] Tribunal do Presidente: Avaliação Semanal Campeão vs Desafiante"""
        from datetime import datetime

        self.log_msg(
            "🏛️ Tribunal Quantitativo Armado. O Presidente avaliará o Guardião aos Domingos."
        )
        ultimo_julgamento = None

        while getattr(self, "is_app_alive", True):
            try:
                await asyncio.sleep(60)
                agora = datetime.now()

                if agora.weekday() == 6 and agora.hour == 23 and agora.minute >= 50:
                    if ultimo_julgamento and ultimo_julgamento.date() == agora.date():
                        continue

                    self.log_msg(
                        "⚖️ [TRIBUNAL] Iniciando Julgamento Semanal: Guardião Sombra vs Trailing Fixo..."
                    )
                    arquivo_bd = getattr(self, "arquivo_db_memoria", None)
                    if not arquivo_bd:
                        ultimo_julgamento = agora
                        continue

                    def _worker(conn):
                        cursor = conn.cursor()
                        pnl_campeao = 0.0
                        _cut = time.time() - 7 * 86400
                        for h in getattr(self, "historico_operacoes", []) or []:
                            ts = float(h.get("ts", 0) or 0)
                            if ts <= 0 or ts < _cut:
                                continue
                            pnl_campeao += float(h.get("lucro", 0) or 0)

                        cursor.execute(
                            "SELECT SUM(pnl_atual) FROM guardiao_shadow_log WHERE timestamp >= date('now', '-7 days') AND (acao_sugerida LIKE '%CLOSE%' OR acao_sugerida LIKE '%PANIC%')"
                        )
                        res_sombra = cursor.fetchone()
                        pnl_guardiao = (
                            float(res_sombra[0])
                            if res_sombra and res_sombra[0]
                            else 0.0
                        )

                        self.log_msg(
                            f"📊 [VEREDITO] Fixo: US${pnl_campeao:.2f} | Guardião Sombra: ~{pnl_guardiao:.2f}%"
                        )

                        win_streak = int(self.cfg.get("guardian_win_streak", 0))
                        if pnl_guardiao > pnl_campeao:
                            win_streak += 1
                            self.cfg["guardian_win_streak"] = win_streak
                            if win_streak >= 2:
                                self.log_msg(
                                    f"🏆 [PROMOÇÃO] O Guardião esmagou a matemática por {win_streak} semanas seguidas! Assumindo o controle total amanhã."
                                )
                                self.cfg["active_exit_strategy"] = "GUARDIAN_AI"
                                cursor.execute(
                                    "INSERT INTO replay_buffer (agent_id, state, action, reward) VALUES ('GUARDIAN_V90', '{\"status\": \"promoted\"}', 'TAKEOVER', 50000.0)"
                                )
                            else:
                                self.log_msg(
                                    f"🟡 [AVISO] Guardião venceu o Fixo! (Vitórias: {win_streak}/2). Precisa vencer a próxima semana para sair do banco."
                                )
                                cursor.execute(
                                    "INSERT INTO replay_buffer (agent_id, state, action, reward) VALUES ('GUARDIAN_V90', '{\"status\": \"weekly_win\"}', 'HOLD_BENCH', 10000.0)"
                                )
                        else:
                            self.cfg["guardian_win_streak"] = 0
                            self.log_msg(
                                "❌ [DERROTA] O Guardião falhou contra o Trailing Stop. Streak zerado. Retornando ao treinamento."
                            )
                            self.cfg["active_exit_strategy"] = "STATIC_TRAILING"
                            self.log_msg(
                                "⚡ [PUNIÇÃO] Choque Neural aplicado (-10.000 pts). Forçando re-treinamento das sinapses de saída."
                            )
                            cursor.execute(
                                "INSERT INTO replay_buffer (agent_id, state, action, reward) VALUES ('GUARDIAN_V90', '{\"status\": \"weekly_loss\"}', 'BENCHED', -10000.0)"
                            )

                        self._julgar_arena_presidente(cursor)

                        if hasattr(self, "salvar_configuracoes_gerais"):
                            self.salvar_configuracoes_gerais()
                        conn.commit()

                    self.enqueue_deep_memory_write(
                        _worker, max_attempts=8, base_delay=0.15, timeout=20.0
                    )
                    ultimo_julgamento = agora
            except Exception:
                pass

    async def _loop_rest_price_fallback(self):
        """Se o WS de tickers ficar >10s sem atualizar preço, força get_tickers (evita BTC em 0.0000)."""
        loop = asyncio.get_running_loop()
        while getattr(self, "is_app_alive", True):
            try:
                await asyncio.sleep(2.0)
                if not self.cfg.get("use_ws", True):
                    continue
                last = float(getattr(self, "_ws_price_last_tick_ts", 0) or 0)
                if time.time() - last < 10.0:
                    continue
                # Mínimo 60s entre chamadas REST completas (rate limit Bybit / varredura ampla).
                if (
                    time.time()
                    - float(getattr(self, "_last_rest_fallback_fetch_ts", 0) or 0)
                    < 60.0
                ):
                    continue

                def _fetch_tickers_rest():
                    try:
                        client = self.get_bybit_client()
                    except Exception:
                        return {"_kind": "err"}
                    kind, data = get_linear_tickers_list_safe(client)
                    if kind == "unstable":
                        return {"_kind": "unstable", "code": data}
                    if kind != "ok":
                        return {"_kind": "empty"}
                    lst = data
                    out = {}
                    for t in lst:
                        if not isinstance(t, dict):
                            continue
                        sym = t.get("symbol")
                        lp = t.get("lastPrice") or t.get("markPrice")
                        if sym in getattr(self, "tickers", []) and lp is not None:
                            out[sym] = float(lp)
                    return {"_kind": "ok", "precos": out}

                precos = await loop.run_in_executor(None, _fetch_tickers_rest)
                if isinstance(precos, dict) and precos.get("_kind") == "unstable":
                    code = precos.get("code", 0)
                    self.log_msg(
                        f"[REST HTTP {code}] Corretora instável, aguardando 10s..."
                    )
                    await asyncio.sleep(10)
                    continue
                self._last_rest_fallback_fetch_ts = time.time()
                if (
                    not isinstance(precos, dict)
                    or precos.get("_kind") != "ok"
                    or not precos.get("precos")
                ):
                    continue
                precos = precos["precos"]
                if precos:
                    self.processar_precos_radar(precos, tick_ts_map=None)
                    self._ws_price_last_tick_ts = time.time()
                    now = time.time()
                    if (
                        now - float(getattr(self, "_ws_rest_fallback_last_log", 0) or 0)
                        > 45.0
                    ):
                        self._ws_rest_fallback_last_log = now
                        self.log_msg(
                            "📡 [REST FALLBACK] Preços atualizados via get_tickers (sem tick WS >10s)."
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(5.0)

    def _merge_kline_ws_row(self, symbol: str, row: list) -> None:
        """Atualiza kline_cache com uma vela vinda do WebSocket (formato 12 colunas)."""
        import pandas as pd

        cols = [
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
        if len(row) < 12:
            return
        lock = getattr(self, "_ops_lock", None)
        if lock is None:
            return
        sym = str(symbol).strip().upper()
        t_open = int(row[0])
        with lock:
            df = self.kline_cache.get(sym)
            if df is None or len(df) < 1:
                return
            last_t = int(df["t"].iloc[-1])
            nr = pd.DataFrame([row], columns=cols)
            if t_open == last_t:
                out = pd.concat([df.iloc[:-1], nr], ignore_index=True)
            elif t_open > last_t:
                out = pd.concat([df, nr], ignore_index=True)
            else:
                return
            if len(out) > 400:
                out = out.iloc[-400:].reset_index(drop=True)
            self.kline_cache[sym] = out
            self.ultima_vela_t[sym] = float(out["t"].iloc[-1])
        if getattr(self, "_kline_ws_last_ts", None) is None:
            self._kline_ws_last_ts = {}
        self._kline_ws_last_ts[sym] = time.time()

    async def loop_websocket_nativo(self):
        # Bybit V5 public linear — URL fixa de produção (mesmo endpoint do stream público linear).
        url_pub = BYBIT_PUBLIC_LINEAR_WS
        while getattr(self, "is_app_alive", True):
            self.log_msg(
                "⚡ [WS] Novo ciclo: Túnel 1 (preços), 2 (orderbook), 3 (VPIN), 4 (kline 15m)..."
            )
            stop_evt = asyncio.Event()
            self._ws_cycle_stop_event = stop_evt

            # [FIX 2] O consumidor nativo agora atua apenas como Roteador de Queue (Bypass do GIL)
            async def _consumer_tickers(stop_evt=stop_evt):
                import multiprocessing as mp
                import queue

                if not hasattr(self, "_ws_price_queue"):
                    self._ws_price_queue = mp.Queue(maxsize=100000)
                    self._ws_log_queue = mp.Queue(maxsize=10000)

                # Esvazia resíduos do ciclo anterior
                while not self._ws_log_queue.empty():
                    try:
                        self._ws_log_queue.get_nowait()
                    except:
                        break
                while not self._ws_price_queue.empty():
                    try:
                        self._ws_price_queue.get_nowait()
                    except:
                        break

                tickers_sub = _filter_valid_bybit_linear_symbols(
                    [t for t in getattr(self, "tickers", ["BTCUSDT"]) if t]
                )

                self._ws_process = mp.Process(
                    target=run_isolated_ws_process,
                    args=(
                        tickers_sub,
                        url_pub,
                        self._ws_price_queue,
                        self._ws_log_queue,
                    ),
                    daemon=True,
                )
                self._ws_process.start()

                try:
                    while getattr(self, "is_app_alive", True) and not stop_evt.is_set():
                        if not self.cfg.get("use_ws", True):
                            await asyncio.sleep(2)
                            continue

                        while not self._ws_log_queue.empty():
                            try:
                                msg = self._ws_log_queue.get_nowait()
                                self.log_msg(msg)
                            except queue.Empty:
                                break
                            except Exception:
                                break

                        extracted = []
                        while not self._ws_price_queue.empty():
                            try:
                                extracted.append(self._ws_price_queue.get_nowait())
                            except queue.Empty:
                                break
                            except Exception:
                                break

                        if extracted:
                            map_precos = {}
                            map_ts = {}
                            for item in extracted:
                                if isinstance(item, tuple) and len(item) == 2:
                                    kind, parsed = item
                                    if kind != "TICKER" or not isinstance(parsed, dict):
                                        continue
                                    topic = str(parsed.get("topic") or "")
                                    if not topic.startswith("tickers."):
                                        continue
                                    payload = parsed.get("data")
                                    if not isinstance(payload, dict):
                                        continue
                                    sym = (
                                        payload.get("symbol") or topic.split(".", 1)[-1]
                                    )
                                    lp = payload.get("lastPrice") or payload.get(
                                        "markPrice"
                                    )
                                    if not sym or lp is None:
                                        continue
                                    try:
                                        fv = float(lp)
                                    except (TypeError, ValueError):
                                        continue
                                    try:
                                        ts = int(
                                            parsed.get("ts")
                                            or payload.get("timestamp")
                                            or (time.time() * 1000)
                                        )
                                    except (TypeError, ValueError):
                                        ts = int(time.time() * 1000)
                                    sym = str(sym).strip().upper()
                                    map_precos[sym] = fv
                                    map_ts[sym] = ts
                                    continue
                                if not isinstance(item, dict):
                                    continue
                                sym = item.get("sym")
                                lp = item.get("lp")
                                ts = item.get("ts")
                                if not sym or lp is None or ts is None:
                                    continue
                                map_precos[sym] = lp
                                map_ts[sym] = ts

                            if map_precos:
                                self.processar_precos_radar(
                                    map_precos, tick_ts_map=map_ts
                                )
                                self._ws_price_last_tick_ts = time.time()
                                if not getattr(self, "_ws_price_feed_confirmed", False):
                                    self._ws_price_feed_confirmed = True
                                    ev = getattr(self, "_ws_price_operational_ev", None)
                                    if ev is not None:
                                        ev.set()

                        # Pausa levíssima para aliviar a thread do event loop principal e ceder a execução
                        await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass
                finally:
                    if hasattr(self, "_ws_process") and self._ws_process.is_alive():
                        self._ws_process.terminate()
                        self._ws_process.join(timeout=1)

            async def _consumer_orderbook(stop_evt=stop_evt):
                if not getattr(self, "l2_books", None):
                    self.l2_books = {}
                if not getattr(self, "spoof_cache", None):
                    self.spoof_cache = {}
                if not getattr(self, "spoofing_detectado", None):
                    self.spoofing_detectado = {}
                if not getattr(self, "liquidity_walls", None):
                    self.liquidity_walls = {}
                if not getattr(self, "realtime_data", None):
                    self.realtime_data = {}

                while getattr(self, "is_app_alive", True):
                    if stop_evt.is_set():
                        return
                    if not self.cfg.get("use_ws", True) or not self.cfg.get(
                        "use_l2_depth", True
                    ):
                        await asyncio.sleep(2)
                        continue
                    await asyncio.sleep(5.0)
                    try:
                        self.log_msg(
                            "🌐 Conectando Túnel 2 (Orderbook / L2 Bybit V5)..."
                        )
                        tickers_alvo = getattr(self, "tickers", ["BTCUSDT"])[:50]
                        elite = [
                            s
                            for s in getattr(self, "ativos_elite_top10", []) or []
                            if s in tickers_alvo
                        ]
                        rest = [t for t in tickers_alvo if t not in elite]
                        tickers_ord = _filter_valid_bybit_linear_symbols(elite + rest)
                        depth_args = [f"orderbook.50.{s}" for s in tickers_ord]

                        async with websockets.connect(url_pub, **_WS_CONNECT_KW) as ws:
                            self._ws_orderbook_fail_streak = 0
                            await _bybit_ws_subscribe_chunked(
                                ws,
                                depth_args,
                                self.log_msg,
                                inter_chunk_delay=0.5,
                                chunk_size=_BYBIT_WS_SUB_CHUNK,
                            )
                            self.log_msg(
                                "✅ Túnel 2 (Orderbook) Estabelecido — L2 orderbook.50 ativo."
                            )
                            hb_task = asyncio.create_task(_bybit_app_ping_loop(ws))
                            try:
                                while (
                                    getattr(self, "is_app_alive", True)
                                    and self.cfg.get("use_ws", True)
                                    and not stop_evt.is_set()
                                ):
                                    msg = await self._recv_one_ws_message(ws, stop_evt)
                                    if msg is None:
                                        break
                                    payload = _safe_ws_json_dict(msg)
                                    if payload is None:
                                        continue
                                    op = payload.get("op")
                                    if op == "ping":
                                        await ws.send(json.dumps({"op": "pong"}))
                                        continue
                                    if op == "pong":
                                        continue
                                    if op == "subscribe":
                                        _log_bybit_subscribe_rejection(
                                            self.log_msg, payload
                                        )
                                        continue
                                    topic = payload.get("topic") or ""
                                    if not topic.startswith("orderbook."):
                                        continue
                                    data = payload.get("data")
                                    if not isinstance(data, dict):
                                        continue
                                    s = data.get("s")
                                    bids = data.get("b", [])
                                    asks = data.get("a", [])

                                    if bids and asks and s:
                                        # [V90 FINAL.2] Identificação do Muro (Wall) de Liquidez
                                        best_bid_wall = max(
                                            bids, key=lambda x: float(x[1])
                                        )
                                        best_ask_wall = max(
                                            asks, key=lambda x: float(x[1])
                                        )

                                        self.liquidity_walls[s] = {
                                            "bid_wall_px": float(best_bid_wall[0]),
                                            "bid_wall_vol": float(best_bid_wall[1]),
                                            "ask_wall_px": float(best_ask_wall[0]),
                                            "ask_wall_vol": float(best_ask_wall[1]),
                                        }

                                        # Mantém a retrocompatibilidade do Imbalance (L2 Imbalance)
                                        bid_qty = sum(float(b[1]) for b in bids)
                                        ask_qty = sum(float(a[1]) for a in asks)
                                        total = bid_qty + ask_qty
                                        
                                        # [FASE 1] Abstração do OFI (Order Flow Imbalance) - Pressão dos primeiros 10 níveis
                                        ofi_ponderado = 0.0
                                        for i in range(min(10, len(bids))):
                                            ofi_ponderado += float(bids[i][1]) * (1.0 / (i + 1))
                                        for i in range(min(10, len(asks))):
                                            ofi_ponderado -= float(asks[i][1]) * (1.0 / (i + 1))
                                        self.realtime_data.setdefault(s, {})["ofi_vetor"] = float(ofi_ponderado)

                                        # --- Spoof catcher (O(1)): evaporação vs tick anterior + preço “plano” ---
                                        evap = float(
                                            self.cfg.get("spoof_evaporation_pct", 0.4)
                                            or 0.4
                                        )
                                        evap = max(0.05, min(0.95, evap))
                                        eps = float(
                                            self.cfg.get(
                                                "spoof_price_flat_eps_pct", 0.06
                                            )
                                            or 0.06
                                        )
                                        px = float(
                                            getattr(self, "precos_atuais", {}).get(
                                                s, 0.0
                                            )
                                            or 0.0
                                        )
                                        spoofing_signal = 0
                                        if s not in self.spoof_cache:
                                            self.spoof_cache[s] = {
                                                "B": bid_qty,
                                                "A": ask_qty,
                                                "px": px,
                                            }
                                            if not hasattr(self, "spoof_strike"):
                                                self.spoof_strike = {}
                                            self.spoof_strike[s] = 0
                                        else:
                                            prev = self.spoof_cache[s]
                                            prev_b = float(prev.get("B", 0.0) or 0.0)
                                            prev_a = float(prev.get("A", 0.0) or 0.0)
                                            prev_px = float(
                                                prev.get("px", 0.0) or 0.0
                                            )
                                            d_pct = (
                                                ((px - prev_px) / prev_px) * 100.0
                                                if prev_px > 1e-12 and px > 1e-12
                                                else 0.0
                                            )
                                            ask_evap = (
                                                (prev_a - ask_qty) / prev_a
                                                if prev_a > 1e-12
                                                else 0.0
                                            )
                                            bid_evap = (
                                                (prev_b - bid_qty) / prev_b
                                                if prev_b > 1e-12
                                                else 0.0
                                            )
                                            # +1: muro ask evaporou sem rally (spoof venda)
                                            ask_trig = ask_evap >= evap and d_pct <= eps
                                            # -1: muro bid evaporou sem queda (spoof compra)
                                            bid_trig = bid_evap >= evap and d_pct >= -eps
                                            if ask_trig and bid_trig:
                                                spoofing_signal = (
                                                    1 if ask_evap >= bid_evap else -1
                                                )
                                            elif ask_trig:
                                                spoofing_signal = 1
                                            elif bid_trig:
                                                spoofing_signal = -1
                                            if not hasattr(self, "spoof_strike"):
                                                self.spoof_strike = {}
                                            if spoofing_signal != 0:
                                                self.spoof_strike[s] = (
                                                    self.spoof_strike.get(s, 0) + 1
                                                )
                                                if self.spoof_strike[s] >= 3:
                                                    self.spoofing_detectado[s] = (
                                                        time.time()
                                                    )
                                                    self.spoof_strike[s] = 0
                                            else:
                                                self.spoof_strike[s] = 0
                                            self.spoof_cache[s] = {
                                                "B": bid_qty,
                                                "A": ask_qty,
                                                "px": px,
                                            }
                                        self.realtime_data.setdefault(s, {})[
                                            "spoofing_signal"
                                        ] = int(spoofing_signal)

                                        if total > 0:
                                            self.l2_books[s] = (
                                                (bid_qty - ask_qty) / total
                                            ) * 100.0
                            finally:
                                hb_task.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await hb_task
                    except Exception as e:
                        if stop_evt.is_set():
                            return
                            
                        # [CORREÇÃO 3] I/O Windows / WS drop: reconexão com backoff exponencial.
                        err_str = str(e).lower()
                        err_repr = str(type(e)).lower()
                        
                        is_win_121 = "121" in err_str or "semáforo" in err_str
                        is_ws_drop = "connectionclosed" in err_repr or "connection close" in err_str
                        b_sleep = _ws_backoff_seconds(
                            self, "_ws_orderbook_fail_streak", e
                        )
                        
                        if is_win_121 or is_ws_drop:
                            self.log_msg(
                                f"⚠️ [WS] Túnel 2 reconectando em {b_sleep:.1f}s ({type(e).__name__})."
                            )
                        else:
                            # Erro anômalo: logar para rastreio
                            self.erro_msg(f"Túnel WS Interrompido ({type(e).__name__}): {e!s}")

                        # GARANTIA ASSÍNCRONA ABSOLUTA: 
                        # Evita WinError crasso de event-loop travado. NUNCA usar time.sleep() aqui.
                        await asyncio.sleep(b_sleep)

            async def _consumer_vpin(stop_evt=stop_evt):
                if not getattr(self, "vpin_data", None):
                    self.vpin_data = {}

                while getattr(self, "is_app_alive", True):
                    if stop_evt.is_set():
                        return
                    if not self.cfg.get("use_ws", True):
                        await asyncio.sleep(2)
                        continue
                    if not getattr(self, "modo_vpin_liberado", True):
                        await asyncio.sleep(2)
                        continue
                    await asyncio.sleep(7.0)
                    try:
                        self.log_msg(
                            "🌐 Conectando Túnel 3 (VPIN / publicTrade Bybit V5)..."
                        )
                        _elite = getattr(self, "ativos_elite_top10", None) or [
                            "BTCUSDT"
                        ]
                        tickers_alvo = _filter_valid_bybit_linear_symbols(_elite)
                        trade_args = [f"publicTrade.{s}" for s in tickers_alvo]

                        async with websockets.connect(url_pub, **_WS_CONNECT_KW) as ws:
                            self._ws_vpin_fail_streak = 0
                            await _bybit_ws_subscribe_chunked(
                                ws,
                                trade_args,
                                self.log_msg,
                                inter_chunk_delay=0.5,
                                chunk_size=_BYBIT_WS_SUB_CHUNK,
                            )
                            self.log_msg(
                                "✅ Túnel 3 (VPIN) Estabelecido — publicTrade ativo."
                            )
                            hb_task = asyncio.create_task(_bybit_app_ping_loop(ws))
                            try:
                                while (
                                    getattr(self, "is_app_alive", True)
                                    and self.cfg.get("use_ws", True)
                                    and not stop_evt.is_set()
                                ):
                                    msg = await self._recv_one_ws_message(ws, stop_evt)
                                    if msg is None:
                                        break
                                    payload = _safe_ws_json_dict(msg)
                                    if payload is None:
                                        continue
                                    op = payload.get("op")
                                    if op == "ping":
                                        await ws.send(json.dumps({"op": "pong"}))
                                        continue
                                    if op == "pong":
                                        continue
                                    if op == "subscribe":
                                        _log_bybit_subscribe_rejection(
                                            self.log_msg, payload
                                        )
                                        continue
                                    topic = payload.get("topic") or ""
                                    if not topic.startswith("publicTrade."):
                                        continue
                                    raw = payload.get("data")
                                    rows = raw if isinstance(raw, list) else [raw]
                                    for data in rows:
                                        if not isinstance(data, dict):
                                            continue
                                        # Bybit V5 publicTrade usa "s" em vários payloads; "symbol" em outros
                                        s = data.get("s") or data.get("symbol")
                                        if (
                                            not s
                                            and isinstance(topic, str)
                                            and topic.startswith("publicTrade.")
                                        ):
                                            s = topic.split(".", 1)[-1].strip()
                                        if s is not None:
                                            s = str(s).strip()
                                        if not s or str(s).lower() in (
                                            "none",
                                            "null",
                                            "",
                                        ):
                                            continue
                                        p = float(
                                            data.get("price") or data.get("p") or 0
                                        )
                                        q = float(
                                            data.get("size") or data.get("v") or 0
                                        )
                                        side = (
                                            data.get("side") or data.get("S") or ""
                                        ).upper()
                                        vol_usd = p * q

                                        if s not in self.vpin_data:
                                            # [V90 Pilar 3] Eviction Protocol (Memory Leak fix)
                                            while len(self.vpin_data) >= 200:
                                                if self.vpin_data:
                                                    self.vpin_data.pop(
                                                        list(self.vpin_data.keys())[0],
                                                        None,
                                                    )
                                                else:
                                                    break
                                            self.vpin_data[s] = {
                                                "buy_vol": 0.0,
                                                "sell_vol": 0.0,
                                                "cvd_cumulativo": 0.0,
                                                "last_reset": time.time(),
                                                "strike": 0,
                                            }

                                        vd = self.vpin_data[s]
                                        # [FASE 1] Cálculo do CVD Cumulativo
                                        if side == "BUY":
                                            vd["buy_vol"] += vol_usd
                                            vd["cvd_cumulativo"] += vol_usd
                                        elif side == "SELL":
                                            vd["sell_vol"] += vol_usd
                                            vd["cvd_cumulativo"] -= vol_usd
                                            
                                        if getattr(self, "realtime_data", None) is None:
                                            self.realtime_data = {}
                                        self.realtime_data.setdefault(s, {})["cvd_vetor"] = float(vd["cvd_cumulativo"])

                                        now = time.time()
                                        # Avaliação de janela curta (5 segundos)
                                        if now - vd["last_reset"] >= 5.0:
                                            total_vol = vd["buy_vol"] + vd["sell_vol"]
                                            # Volume mínimo para ignorar ruído (ex: 500k USD em 5s)
                                            if total_vol > 500_000:
                                                buy_pct = vd["buy_vol"] / total_vol
                                                sell_pct = vd["sell_vol"] / total_vol

                                                # Gatilho de Front-Running: >= 98% (Fluxo Institucional Absoluto)
                                                _vpin_cd = getattr(
                                                    self, "vpin_cooldown", None
                                                )
                                                if _vpin_cd is None:
                                                    self.vpin_cooldown = {}
                                                    _vpin_cd = self.vpin_cooldown
                                                if s not in _vpin_cd:
                                                    while len(_vpin_cd) >= 200:
                                                        if _vpin_cd:
                                                            _vpin_cd.pop(
                                                                list(_vpin_cd.keys())[
                                                                    0
                                                                ],
                                                                None,
                                                            )
                                                        else:
                                                            break
                                                _adx_btc = float(
                                                    getattr(
                                                        self, "_btc_adx_macro", 21.0
                                                    )
                                                )
                                                _cd_ok = (
                                                    time.time() - _vpin_cd.get(s, 0)
                                                    >= 5
                                                )
                                                _adx_min_vpin = float(
                                                    self.cfg.get(
                                                        "adx_regime_minimo",
                                                        REGIME_ADX_MIN,
                                                    )
                                                    or REGIME_ADX_MIN
                                                )
                                                _can_vpin_fire = (
                                                    _cd_ok and _adx_btc >= _adx_min_vpin
                                                )
                                                _can_trap_fire = _cd_ok
                                                _direcional = getattr(
                                                    self,
                                                    "modo_vpin_direcional",
                                                    False,
                                                )
                                                _armadilha = getattr(
                                                    self, "modo_vpin_armadilha", False
                                                )
                                                margem_v = float(
                                                    self.cfg.get("margem_entrada", 10.0)
                                                )
                                                atr_like = p * 0.012

                                                if _direcional and _can_vpin_fire:
                                                    if buy_pct > 0.98:
                                                        vd["strike"] += 1
                                                        if vd["strike"] >= 2:
                                                            self.log_msg(
                                                                f"☣️ [VPIN PUMP] {s}: {buy_pct*100:.1f}% compra agressiva. FRONT-RUNNING LONG!"
                                                            )
                                                            self.registrar_sinal_feed(
                                                                s,
                                                                "LONG",
                                                                certeza=min(
                                                                    99.99,
                                                                    float(buy_pct)
                                                                    * 100.0,
                                                                ),
                                                                preco_entrada=float(p),
                                                                fluxo=float(buy_pct)
                                                                * 100.0,
                                                                status="VPIN_PUMP",
                                                            )
                                                            self.disparar_ordem(
                                                                s,
                                                                "LONG",
                                                                p,
                                                                min(
                                                                    99.0,
                                                                    float(buy_pct)
                                                                    * 100.0,
                                                                ),
                                                                margem_v,
                                                                atr_like,
                                                            )
                                                            _vpin_cd[s] = time.time()
                                                            vd["strike"] = 0
                                                    elif sell_pct > 0.98:
                                                        vd["strike"] += 1
                                                        if vd["strike"] >= 2:
                                                            self.log_msg(
                                                                f"☣️ [VPIN DUMP] {s}: {sell_pct*100:.1f}% venda agressiva. FRONT-RUNNING SHORT!"
                                                            )
                                                            self.registrar_sinal_feed(
                                                                s,
                                                                "SHORT",
                                                                certeza=min(
                                                                    99.99,
                                                                    float(sell_pct)
                                                                    * 100.0,
                                                                ),
                                                                preco_entrada=float(p),
                                                                fluxo=float(sell_pct)
                                                                * 100.0,
                                                                status="VPIN_DUMP",
                                                            )
                                                            self.disparar_ordem(
                                                                s,
                                                                "SHORT",
                                                                p,
                                                                min(
                                                                    99.0,
                                                                    float(sell_pct)
                                                                    * 100.0,
                                                                ),
                                                                margem_v,
                                                                atr_like,
                                                            )
                                                            _vpin_cd[s] = time.time()
                                                            vd["strike"] = 0
                                                    else:
                                                        vd["strike"] = 0
                                                elif _armadilha and _can_trap_fire:
                                                    try:
                                                        from lhn_sr_channels import \
                                                            calcular_canais_sr
                                                    except ImportError:
                                                        calcular_canais_sr = None
                                                    df_sr = getattr(
                                                        self, "kline_cache", {}
                                                    ).get(s)
                                                    trap_done = False
                                                    if (
                                                        calcular_canais_sr
                                                        and df_sr is not None
                                                        and len(df_sr) >= 40
                                                    ):
                                                        dados_sr = calcular_canais_sr(
                                                            df_sr
                                                        )
                                                        for canal in dados_sr.get(
                                                            "canais", []
                                                        ):
                                                            hi = float(canal["hi"])
                                                            lo = float(canal["lo"])
                                                            if (
                                                                hi * 0.998
                                                                <= p
                                                                <= hi * 1.002
                                                                and buy_pct > 0.98
                                                            ):
                                                                self.log_msg(
                                                                    f"🪤 [VPIN TRAP] Exaustão de compra no muro em {s}. SHORT."
                                                                )
                                                                self.disparar_ordem(
                                                                    s,
                                                                    "SHORT",
                                                                    p,
                                                                    min(
                                                                        99.0,
                                                                        float(
                                                                            sell_pct
                                                                        )
                                                                        * 100.0,
                                                                    ),
                                                                    margem_v,
                                                                    atr_like,
                                                                )
                                                                _vpin_cd[s] = (
                                                                    time.time()
                                                                )
                                                                trap_done = True
                                                                break
                                                            if (
                                                                lo * 0.998
                                                                <= p
                                                                <= lo * 1.002
                                                                and sell_pct > 0.98
                                                            ):
                                                                self.log_msg(
                                                                    f"🪤 [VPIN TRAP] Exaustão de venda no muro em {s}. LONG."
                                                                )
                                                                self.disparar_ordem(
                                                                    s,
                                                                    "LONG",
                                                                    p,
                                                                    min(
                                                                        99.0,
                                                                        float(
                                                                            buy_pct
                                                                        )
                                                                        * 100.0,
                                                                    ),
                                                                    margem_v,
                                                                    atr_like,
                                                                )
                                                                _vpin_cd[s] = (
                                                                    time.time()
                                                                )
                                                                trap_done = True
                                                                break
                                                    if not trap_done:
                                                        vd["strike"] = 0
                                                else:
                                                    vd["strike"] = 0

                                            # Reseta o balde (bucket)
                                            vd["buy_vol"] = 0.0
                                            vd["sell_vol"] = 0.0
                                            vd["last_reset"] = now
                            finally:
                                hb_task.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await hb_task
                    except Exception as e:
                        if stop_evt.is_set():
                            return
                            
                        # [CORREÇÃO 3] I/O Windows / WS drop: reconexão com backoff exponencial.
                        err_str = str(e).lower()
                        err_repr = str(type(e)).lower()
                        
                        is_win_121 = "121" in err_str or "semáforo" in err_str
                        is_ws_drop = "connectionclosed" in err_repr or "connection close" in err_str
                        b_sleep = _ws_backoff_seconds(
                            self, "_ws_vpin_fail_streak", e
                        )
                        
                        if is_win_121 or is_ws_drop:
                            self.log_msg(
                                f"⚠️ [WS] Túnel 3 reconectando em {b_sleep:.1f}s ({type(e).__name__})."
                            )
                        else:
                            # Erro anômalo: logar para rastreio
                            self.erro_msg(f"Túnel WS Interrompido ({type(e).__name__}): {e!s}")

                        # GARANTIA ASSÍNCRONA ABSOLUTA: 
                        # Evita WinError crasso de event-loop travado. NUNCA usar time.sleep() aqui.
                        await asyncio.sleep(b_sleep)

            async def _consumer_kline_ws(stop_evt=stop_evt):
                """Túnel 4: kline 15m por WebSocket — alimenta kline_cache (REST só no bootstrap)."""
                while getattr(self, "is_app_alive", True):
                    if stop_evt.is_set():
                        return
                    if not self.cfg.get("use_ws", True) or not self.cfg.get(
                        "use_kline_ws", True
                    ):
                        await asyncio.sleep(2)
                        continue
                    await asyncio.sleep(9.0)
                    try:
                        iv = str(self.cfg.get("kline_ws_interval", "15"))
                        tickers_alvo = _filter_valid_bybit_linear_symbols(
                            [
                                t
                                for t in (getattr(self, "tickers", []) or ["BTCUSDT"])
                                if t
                            ]
                        )
                        # Top-Tier: evita estourar limite de tópicos — prioriza elite + maior volume (início da lista).
                        _kline_cap = int(self.cfg.get("kline_ws_max_symbols", 20))
                        if _kline_cap > 0 and len(tickers_alvo) > _kline_cap:
                            tickers_alvo = tickers_alvo[:_kline_cap]
                        kline_args = [f"kline.{iv}.{s}" for s in tickers_alvo]
                        _cap_desc = "sem limite" if _kline_cap <= 0 else str(_kline_cap)
                        self.log_msg(
                            f"🌐 Conectando Túnel 4 (Kline {iv}m / Bybit V5 WS) — {len(kline_args)} tópicos (cap {_cap_desc})..."
                        )
                        async with websockets.connect(url_pub, **_WS_CONNECT_KW) as ws:
                            self._ws_kline_fail_streak = 0
                            await _bybit_ws_subscribe_chunked(
                                ws,
                                kline_args,
                                self.log_msg,
                                inter_chunk_delay=0.5,
                                chunk_size=_BYBIT_WS_SUB_CHUNK,
                            )
                            self.log_msg(
                                "✅ Túnel 4 (Kline) Estabelecido — feed de velas em tempo real."
                            )
                            hb_task = asyncio.create_task(_bybit_app_ping_loop(ws))
                            try:
                                while (
                                    getattr(self, "is_app_alive", True)
                                    and self.cfg.get("use_ws", True)
                                    and self.cfg.get("use_kline_ws", True)
                                    and not stop_evt.is_set()
                                ):
                                    msg = await self._recv_one_ws_message(ws, stop_evt)
                                    if msg is None:
                                        break
                                    payload = _safe_ws_json_dict(msg)
                                    if payload is None:
                                        continue
                                    op = payload.get("op")
                                    if op == "ping":
                                        await ws.send(json.dumps({"op": "pong"}))
                                        continue
                                    if op == "pong":
                                        continue
                                    if op == "subscribe":
                                        _log_bybit_subscribe_rejection(
                                            self.log_msg, payload
                                        )
                                        continue
                                    topic = payload.get("topic") or ""
                                    if not topic.startswith("kline."):
                                        continue
                                    parts = topic.split(".")
                                    if len(parts) < 3:
                                        continue
                                    sym = parts[-1].strip().upper()
                                    raw = payload.get("data")
                                    rows = (
                                        raw
                                        if isinstance(raw, list)
                                        else ([raw] if isinstance(raw, dict) else [])
                                    )
                                    for cndl in rows:
                                        if not isinstance(cndl, dict):
                                            continue
                                        r = ws_kline_candle_to_row(cndl)
                                        if r:
                                            self._merge_kline_ws_row(sym, r)
                            finally:
                                hb_task.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await hb_task
                    except Exception as e:
                        if stop_evt.is_set():
                            return
                            
                        # [CORREÇÃO 3] I/O Windows / WS drop: reconexão com backoff exponencial.
                        err_str = str(e).lower()
                        err_repr = str(type(e)).lower()
                        
                        is_win_121 = "121" in err_str or "semáforo" in err_str
                        is_ws_drop = "connectionclosed" in err_repr or "connection close" in err_str
                        b_sleep = _ws_backoff_seconds(
                            self, "_ws_kline_fail_streak", e
                        )
                        
                        if is_win_121 or is_ws_drop:
                            self.log_msg(
                                f"⚠️ [WS] Túnel 4 reconectando em {b_sleep:.1f}s ({type(e).__name__})."
                            )
                        else:
                            # Erro anômalo: logar para rastreio
                            self.erro_msg(f"Túnel WS Interrompido ({type(e).__name__}): {e!s}")

                        # GARANTIA ASSÍNCRONA ABSOLUTA: 
                        # Evita WinError crasso de event-loop travado. NUNCA usar time.sleep() aqui.
                        await asyncio.sleep(b_sleep)

            await asyncio.gather(
                _consumer_tickers(),
                _consumer_orderbook(),
                _consumer_vpin(),
                _consumer_kline_ws(),
                return_exceptions=True,
            )
            await asyncio.sleep(0)

    def loop_radar_rest(self):
        # [A5] Event para encerramento imediato
        if not hasattr(self, "_stop_event_radar_rest"):
            self._stop_event_radar_rest = threading.Event()
        client = self.get_bybit_client()
        while self.is_app_alive:
            try:
                # Se o WS estiver online, o REST entra em modo de dormência (fallback)
                if not self.cfg.get("use_ws", True) or not self.cfg.get(
                    "use_async_engine", True
                ):
                    kind, data = get_linear_tickers_list_safe(client)
                    if kind == "unstable":
                        code = data
                        self.log_msg(
                            f"[REST HTTP {code}] Corretora instável, aguardando 10s..."
                        )
                        time.sleep(10)
                        continue
                    if kind != "ok":
                        self._stop_event_radar_rest.wait(timeout=1)
                        self._stop_event_radar_rest.clear()
                        continue
                    lst = data
                    precos_atuais = {}
                    for t in lst:
                        if not isinstance(t, dict):
                            continue
                        sym = t.get("symbol")
                        lp = t.get("lastPrice") or t.get("markPrice")
                        if sym in getattr(self, "tickers", []) and lp is not None:
                            precos_atuais[sym] = float(lp)
                    if precos_atuais:
                        self.processar_precos_radar(precos_atuais, tick_ts_map=None)
                else:
                    self._stop_event_radar_rest.wait(timeout=1)
                    self._stop_event_radar_rest.clear()
                    continue
            except Exception as e:
                self.erro_msg(f"FALHA NO LOOP REST (FALLBACK): {e}")
                if hasattr(self, "invalidar_client_cache"):
                    self.invalidar_client_cache()
                    client = self.get_bybit_client()
                self._stop_event_radar_rest.wait(timeout=2)
                self._stop_event_radar_rest.clear()
                continue
            self._stop_event_radar_rest.wait(timeout=0.8)
            self._stop_event_radar_rest.clear()

    def reiniciar_websocket(self):
        """Sinaliza o ciclo atual de consumidores WS a encerrar; ao fechar, o loop reabre com tickers atuais."""
        self.log_msg(
            "🔄 Reconfigurando Radares e Túneis HFT (reload dinâmico de assinaturas)..."
        )
        loop = getattr(self, "loop_async", None)
        if not loop or not loop.is_running():
            self.log_msg(
                "⚠️ Motor assíncrono inativo — WS não reiniciado (aguardando ignição)."
            )
            return
        evt = getattr(self, "_ws_cycle_stop_event", None)
        if evt is None:
            return

        def _signal():
            try:
                evt.set()
            except Exception:
                pass

        try:
            loop.call_soon_threadsafe(_signal)
        except Exception:
            pass

    def _resolve_preco_saida_manual(self, ativo: str, op: dict) -> float:
        """Mark / último tick / preço de entrada — alinhado ao radar para PnL."""
        try:
            pa = float((getattr(self, "precos_atuais", {}) or {}).get(ativo) or 0.0)
        except (TypeError, ValueError):
            pa = 0.0
        if pa > 0:
            return pa
        pa = float(
            _preco_ultimo_precos_buffer(
                getattr(self, "precos_buffer", {}),
                ativo,
                0.0,
            )
        )
        if pa > 0:
            return pa
        try:
            pe = float(op.get("preco", 0) or 0.0)
        except (TypeError, ValueError):
            pe = 0.0
        return pe if pe > 0 else 0.0

    def _calcular_pnl_fechamento_posicao(self, op: dict, preco_saida: float) -> float:
        """Mesma lógica de PnL que processar_precos_radar (incl. taxas em simulação)."""
        alav = float(op.get("alav", self.cfg.get("alavancagem", 20)))
        margem_f = float(op.get("margem", 0) or 0)
        pe = float(op.get("preco", 0) or 0)
        pa = float(preco_saida)
        if pe <= 0:
            return 0.0
        if pa <= 0:
            pa = pe
        tipo = str(op.get("tipo", "LONG")).upper()
        if tipo == "LONG":
            pct = (pa - pe) / pe
        else:
            pct = (pe - pa) / pe
        lucro = margem_f * (pct * alav)
        if (
            self.cfg.get("use_backtest", True)
            and not getattr(
                self,
                "is_real_account_mode",
                lambda: bool(getattr(self, "modo_real", False)),
            )()
        ):
            fee_percentual = 0.0004
            fee_total = (margem_f * alav) * fee_percentual * 2
            lucro -= fee_total
        return float(lucro)

    @staticmethod
    def _coerce_float_br(v) -> float:
        """Converte número ou string (ex.: '1.234,56' / '1,234.56') para float."""
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(" ", "")
        if not s:
            return 0.0
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    def _resolve_entry_price_op(self, op: dict) -> float:
        """Preço de entrada tolerante a chaves alternativas (simulação / reconciliação / Bybit)."""
        for k in ("preco", "entry_price", "preco_entrada", "avgPrice"):
            v = op.get(k)
            if v is None:
                continue
            pe = self._coerce_float_br(v)
            if pe > 0:
                return pe
        return 0.0

    def _resolve_qty_contratos_op(self, op: dict, entry_price: float) -> float:
        """
        Quantidade em contratos/base. Se qty_real vier 0 mas existir `qtd`, usa `qtd`.
        Se ainda assim for 0, infere notional = margem * alav (linear USDT) / preço.
        """
        for key in ("qty_real", "qty", "qtd"):
            v = op.get(key)
            if v is None:
                continue
            q = abs(self._coerce_float_br(v))
            if q > 1e-18:
                return q
        marg = self._coerce_float_br(op.get("margem", 0) or 0)
        alav = self._coerce_float_br(
            op.get("alav", self.cfg.get("alavancagem", 20)) or 20
        )
        if entry_price > 0 and marg > 0 and alav > 0:
            return (marg * alav) / entry_price
        return 0.0

    def _calcular_campos_pnl_trade(
        self, op: dict, preco_saida: float, lucro_fallback: float = 0.0
    ) -> tuple[float, float]:
        """
        PnL absoluto e % de movimento de preço no ativo (LONG/SHORT).
        Compatível com lucro em USDT já calculado pelo radar (fallback com taxas).
        """
        entry_price = self._resolve_entry_price_op(op)
        close_price = float(preco_saida or 0.0)
        qty = self._resolve_qty_contratos_op(op, entry_price)
        side = str(op.get("tipo", op.get("side", "LONG"))).upper()
        is_long = side in ("BUY", "LONG")

        pnl_valor = 0.0
        pnl_pct = 0.0

        if entry_price > 0 and close_price > 0 and qty > 0:
            if is_long:
                pnl_valor = (close_price - entry_price) * qty
                pnl_pct = ((close_price - entry_price) / entry_price) * 100.0
            else:
                pnl_valor = (entry_price - close_price) * qty
                pnl_pct = ((entry_price - close_price) / entry_price) * 100.0

            if (
                self.cfg.get("use_backtest", True)
                and not getattr(
                    self,
                    "is_real_account_mode",
                    lambda: bool(getattr(self, "modo_real", False)),
                )()
            ):
                margem_f = float(op.get("margem", 0) or 0)
                alav_f = float(op.get("alav", self.cfg.get("alavancagem", 20)) or 20)
                fee_percentual = 0.0004
                fee_total = (margem_f * alav_f) * fee_percentual * 2
                pnl_valor -= fee_total
        else:
            pnl_valor = float(lucro_fallback or 0.0)
            margem = float(op.get("margem", 0.0) or 0.0)
            if margem > 0:
                pnl_pct = (pnl_valor / margem) * 100.0
            elif entry_price > 0 and close_price > 0:
                if is_long:
                    pnl_pct = ((close_price - entry_price) / entry_price) * 100.0
                else:
                    pnl_pct = ((entry_price - close_price) / entry_price) * 100.0

        return float(pnl_valor), float(pnl_pct)

    def _metadados_fechamento_posicao(self, close_price: float) -> dict:
        """Campos comuns ao fechar uma posição (histórico + WS)."""
        return {
            "close_price": float(close_price),
            "status": "CLOSED",
            "close_time": datetime.now().isoformat(),
        }

    def _calcular_estatisticas(self) -> dict:
        """
        Consolida desempenho a partir das operações finalizadas (mesma lista do histórico WS).
        Não altera contadores internos do motor — apenas agrega `pnl`/`lucro`/`profit`.
        """
        # Nexus HFT V90: Soft Cutoff de Sessão — agrega só fechos visíveis (epoch), alinhado ao WS.
        _raw_rows = list(
            getattr(self, "operacoes_finalizadas", None)
            or getattr(self, "historico_operacoes", [])
            or []
        )
        rows = (
            list(self.historico_filtrado_sessao(_raw_rows))
            if hasattr(self, "historico_filtrado_sessao")
            else _raw_rows
        )
        total = len(rows)
        pls: list[float] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            raw = r.get("pnl")
            if raw is None:
                raw = r.get("profit")
            if raw is None:
                raw = r.get("lucro")
            try:
                pls.append(float(raw or 0.0))
            except (TypeError, ValueError):
                pls.append(0.0)
        wins = sum(1 for x in pls if x > 0)
        losses = sum(1 for x in pls if x <= 0)
        pnl_liquido = float(sum(pls))
        winrate = (wins / total * 100.0) if total else 0.0
        return {
            "total_trades": int(total),
            "wins": int(wins),
            "losses": int(losses),
            "winrate": float(winrate),
            "pnl_liquido": pnl_liquido,
        }

    def _guardian_position_age_sec(self, op: dict) -> float | None:
        """Idade da posição em segundos; respeita entry_time, ts_abertura e timestamp ms/s."""
        if not isinstance(op, dict):
            return None
        now = time.time()
        et = op.get("entry_time")
        if et:
            try:
                if isinstance(et, str):
                    s = et.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s)
                    return max(0.0, now - dt.timestamp())
            except (TypeError, ValueError, OSError):
                pass
        ts = op.get("ts_abertura")
        if ts is None:
            ts = op.get("timestamp")
        if ts is not None:
            try:
                tsf = float(ts)
                if tsf > 1e12:
                    tsf /= 1000.0
                return max(0.0, now - tsf)
            except (TypeError, ValueError):
                pass
        return None

    def _minutos_em_operacao(self, op: dict) -> float:
        """Tempo em minutos desde a abertura (entry_time ISO, ts_abertura ou timestamp ms)."""
        age = self._guardian_position_age_sec(op)
        if age is None:
            return 0.0
        return max(0.0, age / 60.0)

    def _guardian_grace_seconds(self) -> float:
        grace = float(self.cfg.get("guardian_survival_grace_sec", 180) or 180)
        return max(0.0, min(900.0, grace))

    def _guardian_grace_active(self, op: dict) -> bool:
        age = self._guardian_position_age_sec(op)
        if age is None:
            return False
        return age < self._guardian_grace_seconds()

    def _guardian_hard_stop_hit(
        self, op: dict, pnl_percent: float, p_atual: float | None = None
    ) -> bool:
        hard_cfg = self.cfg.get("guardian_hard_sl_pct", None)
        if hard_cfg is None:
            hard_cfg = float(self.cfg.get("hard_sl_pct", 0.015) or 0.015) * 100.0
        hard = abs(float(hard_cfg or 1.5))
        if float(pnl_percent) <= -hard:
            return True
        if p_atual is None:
            return False
        try:
            return not self._preco_nao_atingiu_sl_guardiao(op, float(p_atual))
        except Exception:
            return False

    def _preco_nao_atingiu_sl_guardiao(self, op: dict, p_atual: float) -> bool:
        """True se o preço ainda não disparou o SL lógico (posição viva no radar)."""
        tipo = str(op.get("tipo", "LONG")).upper()
        sl = float(op.get("sl", 0) or 0)
        if sl <= 0:
            return True
        if tipo in ("LONG", "BUY"):
            return p_atual > sl
        return p_atual < sl

    def _guardiao_custo_oportunidade(
        self, op: dict, p_atual: float, pnl_percent: float
    ) -> bool:
        """
        Scalping/daytrade: após N minutos, se o PnL % estiver fraco/estagnado e o SL não tiver sido atingido,
        forçar saída total (custo de oportunidade).
        """
        if not isinstance(op, dict) or op.get("_pending"):
            return False
        if op.get("custo_oportunidade_feito"):
            return False
        max_min = float(self.cfg.get("guardian_opportunity_max_minutes", 120.0))
        pnl_cap = float(self.cfg.get("guardian_opportunity_pnl_percent_cap", 0.35))
        mins = self._minutos_em_operacao(op)
        if mins < max_min:
            return False
        if not self._preco_nao_atingiu_sl_guardiao(op, p_atual):
            return False
        if op.get("_breakeven_protegido") and pnl_percent > 0:
            return False
        if pnl_percent > pnl_cap:
            return False
        return True

    def _registrar_fechamento_manual_sessao(
        self,
        ativo: str,
        op: dict,
        lucro: float,
        resultado: str,
        preco_saida: float | None = None,
        *,
        origem_label: str = "MANUAL",
        pnl_hist_override: float | None = None,
        pnl_pct_override: float | None = None,
    ):
        """Wrapper para o pipeline unificado de fechamento (manual e autônomo)."""
        if preco_saida is None:
            preco_saida = self._resolve_preco_saida_manual(ativo, op)
        if pnl_hist_override is None or pnl_pct_override is None:
            pnl_hist, pnl_pct_hist = self._calcular_campos_pnl_trade(
                op, float(preco_saida or 0.0), lucro_fallback=float(lucro)
            )
        else:
            pnl_hist = float(pnl_hist_override)
            pnl_pct_hist = float(pnl_pct_override)

        self._registrar_fechamento_sessao(
            ativo=ativo,
            op=op,
            resultado=resultado,
            pnl_hist=float(pnl_hist),
            pnl_pct_hist=float(pnl_pct_hist),
            preco_saida=float(preco_saida or 0.0),
            origem_label=origem_label,
        )

    async def encerrar_operacao_manual(self, symbol: str):
        """Encerramento manual: PnL + histórico + métricas de sessão (paridade com fecho automático)."""
        try:
            if symbol is None or not isinstance(symbol, str):
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "🛡️ [RISK] Encerramento manual abortado: símbolo ausente ou inválido."
                    )
                return
            raw = symbol.strip()
            if not raw or raw.lower() in ("none", "null"):
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "🛡️ [RISK] Encerramento manual abortado: símbolo ausente ou inválido."
                    )
                return
            target_symbol = raw.upper()
            if hasattr(self, "log_msg"):
                self.log_msg(f"--- COMANDO DE EMERGÊNCIA: ENCERRAR {target_symbol} ---")
            ativo = target_symbol
            print(f"🔧 [MANUAL CLOSE] Iniciando encerramento manual para {ativo}")
            with self._ops_lock:
                op = getattr(self, "operacoes_abertas", {}).get(ativo)
            if not op:
                print(f"⚠️ [ERRO] {ativo} não encontrado nas ordens abertas.")
                return
            if isinstance(op, dict) and op.get("_pending"):
                print(f"⚠️ [ERRO] {ativo} está pendente de confirmação — abortado.")
                return

            op = dict(op)
            preco_saida = self._resolve_preco_saida_manual(ativo, op)
            lucro_fb = self._calcular_pnl_fechamento_posicao(op, preco_saida)
            pnl_net, _ = self._calcular_campos_pnl_trade(
                op, float(preco_saida or 0.0), lucro_fallback=lucro_fb
            )
            lucro = float(pnl_net)
            if lucro > 0:
                resultado = "GANHO (FECHAMENTO MANUAL)"
            else:
                resultado = "PERDA (FECHAMENTO MANUAL)"

            is_real = getattr(
                self,
                "is_real_account_mode",
                lambda: bool(getattr(self, "modo_real", False)),
            )()
            qty_real = float(op.get("qty_real") or 0)
            if qty_real <= 0 and op.get("qtd"):
                try:
                    qty_real = float(op["qtd"])
                except (TypeError, ValueError):
                    qty_real = 0.0
            op_tipo = op.get("tipo", "LONG")

            if is_real and qty_real > 0 and hasattr(self, "fechar_ordem_real"):
                self.fechar_ordem_real(ativo, op_tipo, qty_real)

            with self._ops_lock:
                if self.operacoes_abertas.pop(ativo, None) is None:
                    print(
                        f"⚠️ [MANUAL CLOSE] {ativo} já não estava em operacoes_abertas."
                    )
                    return

            if hasattr(self, "_ajustar_saldo_pos_fechamento"):
                self._ajustar_saldo_pos_fechamento(lucro, bool(is_real))
            else:
                self.saldo_atual = float(getattr(self, "saldo_atual", 0.0)) + float(
                    lucro
                )
            self._radar_enqueue_saldo_persist()

            self.log_msg(
                f"RESULTADO FINALIZADO: {datetime.now().strftime('%H:%M:%S')} - {ativo} - "
                f"{op_tipo} - {resultado} - Lucro: {lucro:+.2f}"
            )
            self._registrar_fechamento_manual_sessao(
                ativo, op, lucro, resultado, preco_saida=preco_saida
            )
        except Exception as e:
            print(f"❌ [MANUAL CLOSE ERROR] {e}")
            if hasattr(self, "erro_msg"):
                self.erro_msg(f"Falha ao encerrar operação manual em {symbol}.")

    def _get_symbol_tick_size(self, ativo):
        try:
            client = self.get_bybit_client()
            info = self.get_futures_exchange_info_cached(client, ttl_sec=86400)
            for s in info.get("symbols", []):
                if s.get("symbol") == ativo:
                    for f in s.get("filters", []):
                        if f.get("filterType") == "PRICE_FILTER":
                            return float(f.get("tickSize", 0.01))
                    break
        except Exception:
            pass
        return 0.01

    def _round_price_to_tick(self, ativo, preco):
        try:
            tick = self._get_symbol_tick_size(ativo)
            if tick <= 0:
                return float(preco)
            d_preco = Decimal(str(preco))
            d_tick = Decimal(str(tick))
            qtd_ticks = (d_preco / d_tick).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
            return float(qtd_ticks * d_tick)
        except Exception:
            return float(preco)

    def validar_regime_mercado(self, ativo, adx_val):
        """Atualiza _regime_state (lateral vs trending). Contagem no resumo do Radar, sem log por ativo."""
        try:
            adx_num = float(adx_val)
        except Exception:
            adx_num = 0.0
        adx_minimo = float(self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN))

        if not hasattr(self, "_regime_state"):
            self._regime_state = {}

        is_lateral = adx_num < adx_minimo
        self._regime_state[ativo] = "lateral" if is_lateral else "trending"
        return True

    def get_regime_summary(self):
        """Retorna contagem de ativos por estado de regime para o resumo de ciclo."""
        if not hasattr(self, "_regime_state"):
            return 0, 0
        lateral = sum(1 for s in self._regime_state.values() if s == "lateral")
        trending = sum(1 for s in self._regime_state.values() if s == "trending")
        return lateral, trending

    def validar_consenso_triplo(self, ativo, sinal, certeza):
        """Tripla confluência: Neural + L2 + Macro NLP. Retorna (ok, motivo) para resumo."""
        if certeza < float(self.cfg.get("winrate_minimo", 50.0)):
            return False

        l2_imbalance = float(getattr(self, "l2_books", {}).get(ativo, 0.0))
        sentimento_nlp = float(getattr(self, "pontuacao_sentimento_atual", 0.0))
        l2_corte = float(self.cfg.get("l2_imbalance_corte", 0.0))
        nlp_minimo = float(self.cfg.get("nlp_sentimento_minimo", -3.0))

        # [V90 FINAL.2] Escudo de Raio-X (Fuga de Muros de Liquidez)
        muros = getattr(self, "liquidity_walls", {}).get(ativo)
        p_atual = _preco_ultimo_precos_buffer(
            getattr(self, "precos_buffer", {}), ativo, 0.0
        )
        if muros and p_atual:
            if sinal == "LONG":
                dist_muro_venda = (muros["ask_wall_px"] - p_atual) / p_atual
                # Se o maior muro de venda está muito perto (< 0.5%) da entrada, aborta (Bull Trap)
                if 0 < dist_muro_venda < 0.005:
                    self.log_msg(
                        f"🧱 [RAIO-X] Bull Trap Evitada em {ativo}: Muro de Venda detectado a {dist_muro_venda*100:.2f}% de distância."
                    )
                    return False
            elif sinal == "SHORT":
                dist_muro_compra = (p_atual - muros["bid_wall_px"]) / p_atual
                # Se o maior muro de compra está muito perto (< 0.5%) da entrada, aborta (Bear Trap)
                if 0 < dist_muro_compra < 0.005:
                    self.log_msg(
                        f"🧱 [RAIO-X] Bear Trap Evitada em {ativo}: Muro de Compra detectado a {dist_muro_compra*100:.2f}% de distância."
                    )
                    return False

        if sinal == "LONG":
            l2_ok = l2_imbalance > l2_corte
            nlp_ok = sentimento_nlp >= nlp_minimo
        else:
            l2_ok = l2_imbalance < (-l2_corte)
            nlp_ok = True

        if not (l2_ok and nlp_ok):
            return False
        return True

    def loop_comite_gestor(self):
        """Maestro: alterna Seguidor de Tendência (Sniper+VPIN direcional) vs Caçador de Lateralidade (Lateral+VPIN armadilha+Arb)."""
        self.modo_arbitragem_liberado = True
        self.modo_sniper_liberado = True
        self.modo_vpin_liberado = True
        self.modo_lateral_macro_liberado = False
        self.modo_vpin_direcional = True
        self.modo_vpin_armadilha = False

        while getattr(self, "is_app_alive", True):
            if not getattr(self, "is_searching", False):
                time.sleep(5)
                continue
            try:
                client = self.get_bybit_client()
                # Puxa klines do BTC para avaliar a força do mercado geral (fora do event loop asyncio)
                k_btc = self._feature_executor.submit(
                    self._binance_futures_klines_safe,
                    client,
                    symbol="BTCUSDT",
                    interval="15m",
                    limit=50,
                ).result(timeout=120)
                if k_btc is None:
                    time.sleep(5)
                    continue
                import pandas as pd

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

                # Calcula ADX do BTC (Volatilidade Macro)
                tr = pd.concat(
                    [
                        df_btc["h"] - df_btc["l"],
                        abs(df_btc["h"] - df_btc["c"].shift()),
                        abs(df_btc["l"] - df_btc["c"].shift()),
                    ],
                    axis=1,
                ).max(axis=1)
                _plus_dm = df_btc["h"].diff().clip(lower=0)
                _minus_dm = (-df_btc["l"].diff()).clip(lower=0)
                _atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
                _plus_di = (
                    100
                    * _plus_dm.ewm(alpha=1 / 14, adjust=False).mean()
                    / _atr.replace(0, 1)
                )
                _minus_di = (
                    100
                    * _minus_dm.ewm(alpha=1 / 14, adjust=False).mean()
                    / _atr.replace(0, 1)
                )
                _dx = (
                    100
                    * abs(_plus_di - _minus_di)
                    / (_plus_di + _minus_di).replace(0, 1)
                )
                adx_btc = _dx.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1]
                # Cache macro para o radar VPIN (front-running só em tendência)
                self._btc_adx_macro = float(adx_btc)

                _regime_adx = float(
                    self.cfg.get("adx_regime_minimo", REGIME_ADX_MIN) or REGIME_ADX_MIN
                )
                # Maestro V91: Tendência (ADX≥limiar) = Sniper + VPIN direcional | Lateral = IA Lateral + VPIN armadilha + Arb
                if adx_btc < _regime_adx:
                    if getattr(self, "modo_sniper_liberado", True):
                        self.log_msg(
                            f"🏛️ [MAESTRO] Regime CAÇADOR DE LIQUIDEZ (BTC ADX: {adx_btc:.1f}). "
                            f"Sniper OFF | Cérebro Lateral ON | VPIN ARMADILHA ON | Arbitragem ON."
                        )
                    self.modo_arbitragem_liberado = True
                    self.modo_sniper_liberado = False
                    self.modo_lateral_macro_liberado = True
                    self.modo_vpin_direcional = False
                    self.modo_vpin_armadilha = True
                    self.modo_vpin_liberado = True
                else:
                    if not getattr(self, "modo_sniper_liberado", True):
                        self.log_msg(
                            f"🏛️ [MAESTRO] Regime SEGUIDOR DE TENDÊNCIA (BTC ADX: {adx_btc:.1f}). "
                            f"Sniper ON | VPIN DIRECIONAL ON | Lateral macro OFF | Arbitragem OFF."
                        )
                    self.modo_arbitragem_liberado = False
                    self.modo_sniper_liberado = True
                    self.modo_lateral_macro_liberado = False
                    self.modo_vpin_direcional = True
                    self.modo_vpin_armadilha = False
                    self.modo_vpin_liberado = True
            except Exception as e:
                self.erro_msg(f"Erro no Comitê Gestor: {e}")
            _interval = float(
                getattr(self, "cfg", {}).get("comite_macro_interval_sec", 60)
            )
            # Evita valores absurdamente baixos (spam REST); piso 30s.
            time.sleep(max(30.0, _interval))

    def _arb_leg_unrealized_pnl_usd(self, op: dict | None, mark_px: float) -> float:
        """PnL não realizado estimado (USD) de uma perna linear (margem × alav × %Δ preço)."""
        if not op or not isinstance(op, dict):
            return 0.0
        try:
            tipo = str(op.get("tipo", "LONG")).upper()
            pe = float(op.get("preco", 0.0) or 0.0)
            mg = float(op.get("margem", 0.0) or 0.0)
            alv = float(op.get("alav", self.cfg.get("alavancagem", 20)) or 20)
            if pe <= 1e-12 or mg <= 0:
                return 0.0
            px = float(mark_px or 0.0)
            if px <= 0:
                return 0.0
            notional = float(mg) * float(alv)
            if tipo in ("LONG", "BUY"):
                pct = (px - pe) / pe
            else:
                pct = (pe - px) / pe
            return float(pct * notional)
        except (TypeError, ValueError, ZeroDivisionError):
            return 0.0

    def _arb_pair_roundtrip_fee_cost_usd(self, *ops: dict | None) -> float:
        fee_rate = float(
            self.cfg.get(
                "taker_fee_rate",
                self.cfg.get("bybit_taker_fee_rate", 0.0006),
            )
            or 0.0006
        )
        fee_rate = max(0.0, fee_rate)
        notional_total = 0.0
        for op in ops:
            if not isinstance(op, dict):
                continue
            margem = self._coerce_float_br(op.get("margem", 0.0) or 0.0)
            alav = self._coerce_float_br(
                op.get("alav", self.cfg.get("alavancagem", 20)) or 20.0
            )
            notional_total += max(0.0, margem * alav)
        return float(notional_total * fee_rate * 2.0)

    def _guardian_survival_allows_panic(
        self, op: dict, pnl_percent: float, motivo: str
    ) -> bool:
        """Grace period pós-abertura: bloqueia PANIC/SCALE_OUT salvo hard stop."""
        if not isinstance(op, dict):
            return True
        mo = str(motivo or "")
        if "Hard Stop" in mo:
            return True
        if "Custo de Oportunidade" in mo or "Exaustão de Tempo" in mo:
            return True
        if self._guardian_hard_stop_hit(op, pnl_percent):
            return True
        return not self._guardian_grace_active(op)

    def _fechar_operacao_mercado_sync(
        self, ativo: str, *, origem_label: str = "ARB_CONVERGENCIA"
    ) -> bool:
        """Fecho a mercado (paridade com encerrar_operacao_manual) para uso em threads síncronas (pairs)."""
        if ativo is None or not isinstance(ativo, str):
            return False
        raw = ativo.strip()
        if not raw or raw.lower() in ("none", "null"):
            return False
        ativo = raw.upper()
        with self._ops_lock:
            op = getattr(self, "operacoes_abertas", {}).get(ativo)
        if not op or (isinstance(op, dict) and op.get("_pending")):
            return False
        op = dict(op)
        preco_saida = self._resolve_preco_saida_manual(ativo, op)
        lucro_fb = self._calcular_pnl_fechamento_posicao(op, preco_saida)
        pnl_net, _ = self._calcular_campos_pnl_trade(
            op, float(preco_saida or 0.0), lucro_fallback=lucro_fb
        )
        lucro = float(pnl_net)
        resultado = (
            "GANHO (FECHAMENTO ARB)" if lucro > 0 else "PERDA (FECHAMENTO ARB)"
        )
        is_real = getattr(
            self,
            "is_real_account_mode",
            lambda: bool(getattr(self, "modo_real", False)),
        )()
        qty_real = float(op.get("qty_real") or 0)
        if qty_real <= 0 and op.get("qtd"):
            try:
                qty_real = float(op["qtd"])
            except (TypeError, ValueError):
                qty_real = 0.0
        op_tipo = op.get("tipo", "LONG")

        if is_real and qty_real > 0 and hasattr(self, "fechar_ordem_real"):
            self.fechar_ordem_real(ativo, op_tipo, qty_real)

        with self._ops_lock:
            if self.operacoes_abertas.pop(ativo, None) is None:
                return False
        if hasattr(self, "_ajustar_saldo_pos_fechamento"):
            self._ajustar_saldo_pos_fechamento(lucro, bool(is_real))
        else:
            self.saldo_atual = float(getattr(self, "saldo_atual", 0.0)) + float(lucro)
        self._radar_enqueue_saldo_persist()
        if hasattr(self, "log_msg"):
            self.log_msg(
                f"RESULTADO FINALIZADO: {datetime.now().strftime('%H:%M:%S')} - {ativo} - "
                f"{op_tipo} - {resultado} - Lucro: {lucro:+.2f} [{origem_label}]"
            )
        self._registrar_fechamento_manual_sessao(
            ativo,
            op,
            lucro,
            resultado,
            preco_saida=preco_saida,
            origem_label=origem_label,
        )
        return True

    def loop_ia_arbitragem(self):
        """Motor de Arbitragem Estatística (Pairs Trading / Market Neutral).

        ENTRADA (divergência): |Z| cruza ``arb_zscore_entry`` — pernas opostas, mesma margem nominal por perna.
        SAÍDA (convergência): com par ativo em ``posicoes_arbitragem_ativas``, |Z| recua para banda perto de zero
        (``arb_zscore_exit``) — fecho a mercado das duas pernas (síncrono, sem nested lock em ``disparar_ordem``).
        """
        import numpy as np

        pares_correlacionados = [
            ("DOGEUSDT", "1000SHIBUSDT"),
            ("OPUSDT", "ARBUSDT"),
            ("SOLUSDT", "AVAXUSDT"),
            ("LINKUSDT", "UNIUSDT"),
            ("PEPEUSDT", "1000BONKUSDT"),
        ]

        if not getattr(self, "posicoes_arbitragem_ativas", None):
            self.posicoes_arbitragem_ativas = {}
        if not getattr(self, "realtime_data", None):
            self.realtime_data = {}

        arb_window = int(self.cfg.get("arb_ratio_window", 60) or 60)
        arb_window = max(20, min(240, arb_window))
        spread_buffers = {par: deque(maxlen=arb_window) for par in pares_correlacionados}

        while getattr(self, "is_app_alive", True):
            if not getattr(self, "is_searching", False):
                time.sleep(1)
                continue
            if not getattr(self, "modo_arbitragem_liberado", True):
                time.sleep(5)
                continue

            z_entry = float(self.cfg.get("arb_zscore_entry", 2.0) or 2.0)
            z_entry = max(0.5, min(6.0, z_entry))
            z_exit = float(self.cfg.get("arb_zscore_exit", 0.5) or 0.5)
            z_exit = max(0.05, min(2.0, z_exit))
            min_obs = int(self.cfg.get("arb_min_observations", 20) or 20)
            min_obs = max(5, min(arb_window - 1, min_obs))
            loop_sleep = float(self.cfg.get("arb_loop_interval_sec", 5.0) or 5.0)
            loop_sleep = max(1.0, min(120.0, loop_sleep))

            try:
                for par in pares_correlacionados:
                    ativo1, ativo2 = par
                    pb = getattr(self, "precos_buffer", {})
                    p1 = _preco_ultimo_precos_buffer(pb, ativo1, 0.0)
                    p2 = _preco_ultimo_precos_buffer(pb, ativo2, 0.0)

                    if not p1 or not p2:
                        continue

                    ratio = float(p1) / float(p2)
                    buf = spread_buffers[par]
                    buf.append(ratio)

                    z_score = 0.0
                    std_spread = 0.0
                    dq = list(buf)
                    if len(dq) > min_obs:
                        arr = np.asarray(dq, dtype=np.float64)
                        mean_spread = float(np.mean(arr))
                        std_spread = float(np.std(arr))
                        if std_spread > 1e-12:
                            z_score = (ratio - mean_spread) / std_spread

                    self.realtime_data.setdefault(ativo1, {})["z_score_arb"] = float(
                        z_score
                    )
                    self.realtime_data.setdefault(ativo2, {})["z_score_arb"] = float(
                        z_score
                    )

                    # --- SAÍDA: convergência do Z à média (mean reversion completa) ---
                    if par in self.posicoes_arbitragem_ativas:
                        # Exige janela válida para não interpretar Z=0 (warm-up) como convergência.
                        if (
                            len(dq) > min_obs
                            and std_spread > 1e-12
                            and abs(z_score) <= z_exit
                        ):
                            with self._ops_lock:
                                ops_live = getattr(self, "operacoes_abertas", {})
                                o1 = ops_live.get(ativo1)
                                o2 = ops_live.get(ativo2)
                            pnl_comb = self._arb_leg_unrealized_pnl_usd(
                                o1 if isinstance(o1, dict) else None, p1
                            ) + self._arb_leg_unrealized_pnl_usd(
                                o2 if isinstance(o2, dict) else None, p2
                            )
                            fee_cost = self._arb_pair_roundtrip_fee_cost_usd(
                                o1 if isinstance(o1, dict) else None,
                                o2 if isinstance(o2, dict) else None,
                            )
                            if pnl_comb <= fee_cost:
                                self.log_msg(
                                    f"⚖️ [ARBITRAGEM] Z convergiu (|Z|={abs(z_score):.3f}) mas PnL combinado "
                                    f"est. US${pnl_comb:.2f} ≤ custo taker ida+volta US${fee_cost:.2f} — mantendo par até cobrir taxas."
                                )
                            else:
                                self.log_msg(
                                    f"⚖️ [ARBITRAGEM] SAÍDA (convergência+fees): |Z|={abs(z_score):.3f} ≤ {z_exit:.3f}, "
                                    f"PnL bruto est. US${pnl_comb:.2f} > taxas US${fee_cost:.2f}. Fechando {ativo1} / {ativo2}."
                                )
                                self._fechar_operacao_mercado_sync(
                                    ativo1, origem_label="ARB_CONVERGENCIA"
                                )
                                self._fechar_operacao_mercado_sync(
                                    ativo2, origem_label="ARB_CONVERGENCIA"
                                )
                                with self._ops_lock:
                                    self.posicoes_arbitragem_ativas.pop(par, None)
                        else:
                            with self._ops_lock:
                                ops_live = getattr(self, "operacoes_abertas", {})
                                o1 = ops_live.get(ativo1)
                                o2 = ops_live.get(ativo2)
                            leg1 = isinstance(o1, dict) and not o1.get("_pending")
                            leg2 = isinstance(o2, dict) and not o2.get("_pending")
                            if not (leg1 and leg2):
                                with self._ops_lock:
                                    self.posicoes_arbitragem_ativas.pop(par, None)
                        continue

                    if len(dq) <= min_obs or std_spread <= 1e-12:
                        continue

                    # --- Pré-checagem sob lock (sem envolver disparar_ordem: evita deadlock) ---
                    with self._ops_lock:
                        abertas = list(getattr(self, "operacoes_abertas", {}).keys())
                        if ativo1 in abertas or ativo2 in abertas:
                            continue
                        if len(abertas) > MAX_OPERACOES_SIMULTANEAS - 2:
                            continue

                    saldo_conta = max(0.0, float(getattr(self, "saldo_atual", 0.0) or 0.0))
                    _lim_arb = obter_limites_risco(saldo_conta)
                    abertas_dict = getattr(self, "operacoes_abertas", {})
                    margem_em_uso = sum(
                        float(op.get("margem", 0.0) or 0.0)
                        for op in abertas_dict.values()
                        if isinstance(op, dict)
                    )
                    teto_global = saldo_conta * float(_lim_arb["pct_exposicao_total"])
                    margem_disponivel = max(0.0, teto_global - margem_em_uso)

                    margem_minima_por_ativo = max(
                        5.0,
                        float(self.cfg.get("arb_margem_minima_por_ativo", 5.0) or 5.0),
                    )
                    margem_base = max(
                        0.0, float(self.cfg.get("margem_entrada", 10.0) or 0.0)
                    )
                    margem_calculada = margem_base / 2.0
                    margem_arb = max(margem_calculada, margem_minima_por_ativo)
                    margem_necessaria_total = margem_arb * 2

                    if margem_disponivel < margem_necessaria_total:
                        self.log_msg(
                            f"🛡️ [ARBITRAGEM] Saldo insuficiente. Veto. "
                            f"Teto livre US${margem_disponivel:.2f} < mínimo US${margem_necessaria_total:.2f}."
                        )
                        continue

                    # --- ENTRADA: divergência (Z esticado) ---
                    if z_score > z_entry:
                        self.log_msg(
                            f"⚖️ [ARBITRAGEM] ENTRADA (divergência): Z={z_score:.3f} > +{z_entry:.2f} → "
                            f"SHORT {ativo1} + LONG {ativo2} (margem US${margem_arb:.2f}/perna)."
                        )
                        self.disparar_ordem(
                            ativo1,
                            "SHORT",
                            p1,
                            90.0,
                            margem_arb,
                            p1 * 0.01,
                            arena_regime_override="ARBITRAGEM",
                        )
                        self.disparar_ordem(
                            ativo2,
                            "LONG",
                            p2,
                            90.0,
                            margem_arb,
                            p2 * 0.01,
                            arena_regime_override="ARBITRAGEM",
                        )
                        with self._ops_lock:
                            ops_live = getattr(self, "operacoes_abertas", {})
                            leg1_ok = isinstance(ops_live.get(ativo1), dict) and not ops_live[ativo1].get("_pending")
                            leg2_ok = isinstance(ops_live.get(ativo2), dict) and not ops_live[ativo2].get("_pending")
                        if not (leg1_ok and leg2_ok):
                            self.log_msg(
                                f"🛡️ [ARBITRAGEM] Par abortado pós-gate: {ativo1}/{ativo2}. Zerando perna órfã."
                            )
                            if leg1_ok:
                                self._fechar_operacao_mercado_sync(ativo1, origem_label="ARB_ORFA_ABORT")
                            if leg2_ok:
                                self._fechar_operacao_mercado_sync(ativo2, origem_label="ARB_ORFA_ABORT")
                            continue
                        with self._ops_lock:
                            self.posicoes_arbitragem_ativas[par] = {
                                "z_at_entry": float(z_score),
                                "ts": time.time(),
                                "leg_a": ativo1,
                                "leg_b": ativo2,
                            }

                    elif z_score < -z_entry:
                        self.log_msg(
                            f"⚖️ [ARBITRAGEM] ENTRADA (divergência): Z={z_score:.3f} < -{z_entry:.2f} → "
                            f"LONG {ativo1} + SHORT {ativo2} (margem US${margem_arb:.2f}/perna)."
                        )
                        self.disparar_ordem(
                            ativo1,
                            "LONG",
                            p1,
                            90.0,
                            margem_arb,
                            p1 * 0.01,
                            arena_regime_override="ARBITRAGEM",
                        )
                        self.disparar_ordem(
                            ativo2,
                            "SHORT",
                            p2,
                            90.0,
                            margem_arb,
                            p2 * 0.01,
                            arena_regime_override="ARBITRAGEM",
                        )
                        with self._ops_lock:
                            ops_live = getattr(self, "operacoes_abertas", {})
                            leg1_ok = isinstance(ops_live.get(ativo1), dict) and not ops_live[ativo1].get("_pending")
                            leg2_ok = isinstance(ops_live.get(ativo2), dict) and not ops_live[ativo2].get("_pending")
                        if not (leg1_ok and leg2_ok):
                            self.log_msg(
                                f"🛡️ [ARBITRAGEM] Par abortado pós-gate: {ativo1}/{ativo2}. Zerando perna órfã."
                            )
                            if leg1_ok:
                                self._fechar_operacao_mercado_sync(ativo1, origem_label="ARB_ORFA_ABORT")
                            if leg2_ok:
                                self._fechar_operacao_mercado_sync(ativo2, origem_label="ARB_ORFA_ABORT")
                            continue
                        with self._ops_lock:
                            self.posicoes_arbitragem_ativas[par] = {
                                "z_at_entry": float(z_score),
                                "ts": time.time(),
                                "leg_a": ativo1,
                                "leg_b": ativo2,
                            }

            except Exception as e:
                self.erro_msg(f"Erro na Thread de Arbitragem: {e}")

            time.sleep(loop_sleep)

    async def _recv_one_ws_message(self, ws, stop_evt: asyncio.Event):
        """Aguarda um frame WS ou sinal de parada (reload de tickers) — sem polling agressivo."""
        if stop_evt.is_set():
            return None
        recv_task = asyncio.create_task(ws.recv())
        stop_task = asyncio.create_task(stop_evt.wait())
        done, pending = await asyncio.wait(
            {recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if stop_task in done:
            recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await recv_task
            return None
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_task
        try:
            return recv_task.result()
        except Exception:
            raise

    def _radar_enqueue_close_disk_io(self, payload: dict):
        """Persistência RL / histórico / replay fora do hot path do radar (fila Disk-IO)."""

        def _run():
            hora = payload.get("hora", "")
            ativo = payload.get("ativo", "")
            op_tipo = payload.get("op_tipo", "")
            resultado = payload.get("resultado", "")
            lucro = float(payload.get("lucro", 0.0))
            prob_ia = float(payload.get("prob_ia", 0.0))
            lucro_rl = float(payload.get("lucro_rl", 0.0))
            arquivo_historico = payload.get("arquivo_historico")
            workspace_raiz = payload.get("workspace_raiz")

            if arquivo_historico:
                try:
                    with open(arquivo_historico, "a") as f:
                        f.write(
                            f"RESULTADO FINALIZADO: {hora} - {ativo} - {op_tipo} - {resultado} - Lucro: {lucro}\n"
                        )
                except Exception as e:
                    if hasattr(self, "erro_msg"):
                        self.erro_msg(f"Erro ao salvar historico: {str(e)}")

            if workspace_raiz:
                rl_file = os.path.join(workspace_raiz, "trade_rl_logs.csv")
                if not os.path.exists(rl_file):
                    try:
                        with open(rl_file, "w") as f:
                            f.write("ativo,sinal,prob_ia,resultado_binario,lucro\n")
                    except Exception:
                        logger.exception(
                            "rl_file_create_failed_deferred | ts=%s | ativo=%s",
                            int(time.time() * 1000),
                            ativo,
                        )

                try:
                    with open(rl_file, "r") as f:
                        linhas_rl = f.readlines()
                    if len(linhas_rl) > 10_000:
                        with open(rl_file, "w") as f:
                            f.write(linhas_rl[0])
                            f.writelines(linhas_rl[-9_999:])
                except Exception:
                    logger.exception(
                        "rl_file_trim_failed_deferred | ts=%s | ativo=%s",
                        int(time.time() * 1000),
                        ativo,
                    )

                try:
                    with open(rl_file, "a") as f:
                        f.write(
                            f"{ativo},{op_tipo},{prob_ia:.2f},{payload.get('res_simplificado', 0)},{lucro_rl:.4f}\n"
                        )
                except Exception:
                    logger.exception(
                        "rl_file_append_failed_deferred | ts=%s | ativo=%s",
                        int(time.time() * 1000),
                        ativo,
                    )

            if payload.get("registrar_replay") and hasattr(
                self, "registrar_resultado_replay"
            ):
                try:
                    self.registrar_resultado_replay(
                        ativo, float(lucro_rl), "SNIPER_V90 FINAL"
                    )
                except Exception:
                    logger.exception(
                        "registrar_replay_deferred | ts=%s | ativo=%s",
                        int(time.time() * 1000),
                        ativo,
                    )

        if hasattr(self, "enqueue_disk_io"):
            self.enqueue_disk_io(_run)
        else:
            _run()

    def _radar_enqueue_saldo_persist(self):
        def _run():
            try:
                self.salvar_saldo()
            except Exception:
                logger.exception("salvar_saldo_deferred")

        if hasattr(self, "enqueue_disk_io"):
            self.enqueue_disk_io(_run)
        else:
            _run()

    def _solicitar_precos_radar_rest_sync(self, motivo=""):
        """Atualização imediata via get_tickers quando o radar rejeita preços inválidos."""
        now = time.time()
        ws_online = bool(getattr(self, "websocket_online", True))
        if self.cfg.get("use_ws", True) and ws_online:
            return None
        if now - float(getattr(self, "_last_radar_rest_preco_ts", 0) or 0) < 60.0:
            return
        last_ws_tick = float(getattr(self, "_ws_price_last_tick_ts", 0) or 0)
        if ws_online and (now - last_ws_tick) < 3.0:
            return
        self._last_radar_rest_preco_ts = now
        try:
            client = self.get_bybit_client()
            kind, data = get_linear_tickers_list_safe(client)
            if kind == "unstable":
                self.log_msg(
                    f"[REST HTTP {data}] Corretora instável, aguardando 10s..."
                )
                time.sleep(10)
                return
            if kind != "ok":
                return
            lst = data
            precos_ok = {}
            tickers_alvo = getattr(self, "tickers", ["BTCUSDT"]) or []
            for t in lst:
                if not isinstance(t, dict):
                    continue
                sym = t.get("symbol")
                lp = t.get("lastPrice") or t.get("markPrice")
                if sym not in tickers_alvo or lp is None:
                    continue
                try:
                    fv = float(lp)
                except (TypeError, ValueError):
                    continue
                if fv > 0:
                    precos_ok[sym] = fv
            if precos_ok:
                self.processar_precos_radar(
                    precos_ok, tick_ts_map=None, _from_rest=True
                )
        except Exception:
            logger.exception("radar_rest_preco_failed | motivo=%s", motivo)

    def _bootstrap_precos_tickers_rest(self):
        """Seed de preços via REST antes do WebSocket (painel não fica em 0.0000 na ignição)."""
        try:
            client = self.get_bybit_client()
            kind, data = get_linear_tickers_list_safe(client)
            if kind == "unstable":
                self.log_msg(
                    f"[REST HTTP {data}] Corretora instável, aguardando 10s..."
                )
                time.sleep(10)
                return
            if kind != "ok":
                return
            lst = data
            precos = {}
            tickers_alvo = getattr(self, "tickers", ["BTCUSDT"]) or []
            for t in lst:
                if not isinstance(t, dict):
                    continue
                sym = t.get("symbol")
                lp = t.get("lastPrice") or t.get("markPrice")
                if sym not in tickers_alvo or lp is None:
                    continue
                try:
                    fv = float(lp)
                except (TypeError, ValueError):
                    continue
                if fv > 0:
                    precos[sym] = fv
            if precos:
                self.processar_precos_radar(precos, tick_ts_map=None, _from_rest=True)
        except Exception:
            pass

    def _atualizar_sl_fisico_bybit(self, ativo, symbol_side, sl_price):
        """[V90 Pilar 2] Envia atualização de trailing stop para os servidores da corretora."""
        is_real = getattr(
            self,
            "is_real_account_mode",
            lambda: bool(getattr(self, "modo_real", False)),
        )()
        if not is_real:
            # Conta em modo simulação explícito — SL só existe no estado interno.
            return

        # Papel / snapshot restaurado: sem contrato na Bybit (qty_real=0) → ErrCode 10001 em loop.
        try:
            with self._ops_lock:
                op_live = (getattr(self, "operacoes_abertas", {}) or {}).get(ativo)
            if op_live is not None:
                qtr = float(op_live.get("qty_real", 0) or 0)
                if qtr <= 0:
                    return
        except (TypeError, ValueError):
            return

        def _run_req():
            try:
                is_real2 = getattr(
                    self,
                    "is_real_account_mode",
                    lambda: bool(getattr(self, "modo_real", False)),
                )()
                if not is_real2:
                    return
                with self._ops_lock:
                    op_now = (getattr(self, "operacoes_abertas", {}) or {}).get(ativo)
                if not op_now:
                    return
                try:
                    qtr2 = float(op_now.get("qty_real", 0) or 0)
                except (TypeError, ValueError):
                    qtr2 = 0.0
                if qtr2 <= 0:
                    return

                client = getattr(self, "get_bybit_client", lambda: None)()
                if not client:
                    return
                sl_str = str(round(sl_price, 5))
                # [FIX ErrCode 10001] A conta está configurada em modo One-Way (não Hedge).
                # One-Way mode: positionIdx DEVE ser 0 para qualquer lado (LONG ou SHORT).
                # Modo Hedge usaria: 1=LONG, 2=SHORT — mas nunca foi habilitado para BTCUSDT.
                client.set_trading_stop(
                    category="linear",
                    symbol=ativo,
                    stopLoss=sl_str,
                    slTriggerBy="LastPrice",
                    positionIdx=0,  # 0 = One-Way mode (padrão Bybit)
                )
                self.log_msg(
                    f"🛡️ [FÍSICO] Stop Loss de {ativo} ancorado na Bybit: {sl_str}"
                )
            except Exception as e:
                err_txt = str(e)
                low = err_txt.lower()
                # Posição zerada na corretora (fechada externamente ou dessincronia): não spammar [ERRO] a cada tick.
                if "10001" in err_txt or "zero position" in low:
                    now = time.time()
                    k = "_sl_fisico_zero_last_log"
                    if not hasattr(self, k):
                        setattr(self, k, {})
                    last_map = getattr(self, k)
                    sym = str(ativo or "").upper()
                    last_t = float(last_map.get(sym, 0.0) or 0.0)
                    if now - last_t >= 120.0:
                        last_map[sym] = now
                        self.log_msg(
                            f"⏭️ SL físico ignorado ({sym}): sem posição na Bybit (10001). "
                            f"Sincronize ou remova a operação interna se já estiver fechada."
                        )
                    return
                self.erro_msg(f"Erro ao ancorar SL Físico ({ativo}): {e}")

        if hasattr(self, "enqueue_disk_io"):
            self.enqueue_disk_io(_run_req)
        else:
            self.submit_background_task(_run_req)

    def _arena_sum_titular_week(self, regime: str, cutoff_ts: float) -> float:
        soma = 0.0
        for h in getattr(self, "historico_operacoes", []) or []:
            if h.get("arena_regime", "SNIPER") != regime:
                continue
            ts = float(h.get("ts", 0) or 0)
            if ts <= 0 or ts < cutoff_ts:
                continue
            soma += float(h.get("lucro", 0.0) or 0.0)
        return soma

    def _arena_count_titular_week(self, regime: str, cutoff_ts: float) -> int:
        total = 0
        for h in getattr(self, "historico_operacoes", []) or []:
            if h.get("arena_regime", "SNIPER") != regime:
                continue
            ts = float(h.get("ts", 0) or 0)
            if ts <= 0 or ts < cutoff_ts:
                continue
            total += 1
        return total

    def _tick_arena_sombra_radar(self, precos_atuais):
        """Atualiza PnL virtual das posições ARENA_RESERVA e fecha em TP/SL."""
        if not precos_atuais:
            return
        abertas = getattr(self, "_arena_sombra_abertas", None)
        if not abertas:
            return
        lock = getattr(self, "_arena_sombra_lock", None)
        if lock:
            with lock:
                _copy = list(abertas)
        else:
            _copy = list(abertas)
        alav = float(self.cfg.get("alavancagem", 20))
        fechadas = []
        import sqlite3

        for op in _copy:
            atv = op.get("ativo")
            if atv not in precos_atuais:
                continue
            pa = float(precos_atuais[atv])
            pe = float(op.get("preco", 0) or 0)
            if pe <= 0:
                continue
            tipo = op.get("tipo")
            slv = float(op.get("sl", 0) or 0)
            tpv = float(op.get("tp", 0) or 0)
            mg = float(op.get("margem_sim", 10.0) or 10.0)
            if tipo == "LONG":
                pct = (pa - pe) / pe
                hit_tp = pa >= tpv
                hit_sl = pa <= slv
            else:
                pct = (pe - pa) / pe
                hit_tp = pa <= tpv
                hit_sl = pa >= slv
            pnl_usd = mg * pct * alav
            op["pnl_simulado"] = float(pct * 100.0)
            uid = op.get("shadow_uid")
            if hit_tp or hit_sl:
                fechadas.append(op)
                motivo = "TP_SOMBRA" if hit_tp else "SL_SOMBRA"

                def _close(
                    uid_=uid,
                    pnl_=pnl_usd,
                    pct_=pct,
                    op_=op,
                    mot_=motivo,
                ):
                    try:
                        dbp = getattr(self, "arquivo_db_memoria", None)
                        if dbp and uid_:
                            with sqlite3.connect(dbp, timeout=5) as conn:
                                conn.execute(
                                    """
                                    UPDATE arena_reserva_log
                                    SET status='CLOSED', pnl_simulado=?, lucro_usd=?
                                    WHERE shadow_uid=?
                                    """,
                                    (float(pct_) * 100.0, float(pnl_), uid_),
                                )
                                conn.commit()
                    except Exception:
                        pass
                    try:
                        if hasattr(self, "registrar_resultado_replay"):
                            ag = (
                                "ARENA_LATERAL_RESERVA"
                                if op_.get("regime") == "LATERAL"
                                else "ARENA_SNIPER_RESERVA"
                            )
                            self.registrar_resultado_replay(
                                op_.get("ativo"), float(pnl_), ag
                            )
                    except Exception:
                        pass
                    self.log_msg(
                        f"👻 [ARENA SOMBRA] Fechado {op_.get('ativo')} {mot_} PnL~${float(pnl_):.2f}"
                    )

                if hasattr(self, "enqueue_disk_io"):
                    self.enqueue_disk_io(_close)
                else:
                    _close()

        if fechadas and lock:
            with lock:
                uids = {f.get("shadow_uid") for f in fechadas}
                self._arena_sombra_abertas = [
                    x
                    for x in (self._arena_sombra_abertas or [])
                    if x.get("shadow_uid") not in uids
                ]
        elif fechadas:
            uids = {f.get("shadow_uid") for f in fechadas}
            self._arena_sombra_abertas = [
                x
                for x in (self._arena_sombra_abertas or [])
                if x.get("shadow_uid") not in uids
            ]

    def _julgar_arena_presidente(self, cursor):
        """Domingo: compara PnL titular vs reserva por regime; opcional swap de ficheiros .keras."""
        import os
        import shutil

        cut_ts = time.time() - 7 * 86400
        wr = getattr(self, "workspace_raiz", None)
        if not wr:
            return
        modelos = os.path.join(wr, "modelos")
        for regime in ("SNIPER", "LATERAL"):
            pnl_tit = self._arena_sum_titular_week(regime, cut_ts)
            n_tit = self._arena_count_titular_week(regime, cut_ts)
            cursor.execute(
                """
                SELECT SUM(COALESCE(lucro_usd, 0)), COUNT(*) FROM arena_reserva_log
                WHERE regime = ? AND status = 'CLOSED'
                AND timestamp >= datetime('now', '-7 days')
                """,
                (regime,),
            )
            row = cursor.fetchone()
            pnl_res = float(row[0]) if row and row[0] is not None else 0.0
            n_res = int(row[1]) if row and row[1] is not None else 0
            n_eval = max(n_tit, n_res)
            self.log_msg(
                f"📊 [VEREDITO {regime}] Titular: US${pnl_tit:.2f} | Reserva: US${pnl_res:.2f} | amostras={n_eval}"
            )
            if n_eval < MIN_TRADES_FOR_EVALUATION:
                continue
            if pnl_res > pnl_tit:
                self.log_msg(
                    f"🏆 [REBAIXAMENTO] Reserva superou titular em {regime} — troca de ficheiros."
                )
                if regime == "SNIPER":
                    a = os.path.join(modelos, "SNIPER_TITULAR.keras")
                    b = os.path.join(modelos, "SNIPER_RESERVA.keras")
                else:
                    a = os.path.join(modelos, "LATERAL_TITULAR.keras")
                    b = os.path.join(modelos, "LATERAL_RESERVA.keras")
                if os.path.isfile(a) and os.path.isfile(b):
                    tmp = os.path.join(modelos, "_TEMP_ARENA_SWAP.keras")
                    try:
                        shutil.copy2(a, tmp)
                        shutil.copy2(b, a)
                        shutil.copy2(tmp, b)
                        os.remove(tmp)
                    except Exception as e:
                        self.erro_msg(f"Arena swap falhou ({regime}): {e}")
            else:
                self.log_msg(
                    f"❌ [MANUTENÇÃO] Titular manteve a vaga em {regime}. Punindo reserva."
                )
            if hasattr(self, "punir_e_retreinar_reserva"):
                try:
                    self.punir_e_retreinar_reserva(regime)
                except Exception as e:
                    self.erro_msg(f"Punição arena {regime}: {e}")
        if hasattr(self, "enqueue_disk_io") and hasattr(self, "ligar_cerebro_ia"):

            def _reload():
                try:
                    self.ligar_cerebro_ia()
                except Exception:
                    pass

            self.enqueue_disk_io(_reload)

    def processar_precos_radar(self, precos_atuais, tick_ts_map=None, _from_rest=False):
        """[V90 Alpha] Radar HFT: snapshot de posições; trailing/TP fora do _ops_lock; lock só buffer + merge."""
        if not self.cfg.get("use_ws", True):
            self.websocket_online = True
            self.monitoramento_apenas_por_ws = False
        elif not _from_rest:
            self.websocket_online = True
            self.monitoramento_apenas_por_ws = False
        if precos_atuais:
            try:
                if any(float(v) <= 0 for v in precos_atuais.values() if v is not None):
                    if not _from_rest:
                        self._solicitar_precos_radar_rest_sync("preço_zero_radar")
                    return
            except (TypeError, ValueError):
                if not _from_rest:
                    self._solicitar_precos_radar_rest_sync("preço_inválido_radar")
                return

        try:
            if precos_atuais:
                self._tick_arena_sombra_radar(precos_atuais)
        except Exception:
            pass

        ativos_para_fechar = []

        # [FIX ANTI-FREEZE] Lista isolada para não disparar REST (Binance) com _ops_lock preso.
        ordens_pendentes_fechamento_real = []
        snapshots = {}

        with self._ops_lock:
            if not hasattr(self, "precos_atuais") or self.precos_atuais is None:
                self.precos_atuais = {}
            for ativo, p_atual in precos_atuais.items():
                pv = float(p_atual)
                if pv <= 0:
                    continue
                self.precos_atuais[ativo] = pv
                buf = self.precos_buffer.get(ativo)
                if not isinstance(buf, deque):
                    buf = deque(maxlen=100)
                    self.precos_buffer[ativo] = buf
                buf.append(pv)
                if not hasattr(self, "tick_timestamps") or self.tick_timestamps is None:
                    self.tick_timestamps = {}
                ts_tick = None
                if tick_ts_map and ativo in tick_ts_map and tick_ts_map.get(ativo):
                    ts_tick = int(tick_ts_map.get(ativo))
                else:
                    ts_tick = int(time.time() * 1000)
                self.tick_timestamps[ativo] = ts_tick

                if ativo in self.operacoes_abertas:
                    _op_r = self.operacoes_abertas[ativo]
                    if isinstance(_op_r, dict) and _op_r.get("_pending"):
                        continue
                    # [FIX 4] Substituído deepcopy (bloqueio de GIL extremo) por dict raso
                    snapshots[ativo] = dict(_op_r)

        for ativo, p_atual in precos_atuais.items():
            if ativo not in snapshots:
                continue
            op = snapshots[ativo]
            resultado = None

            # Marcação a mercado (simulação): PnL flutuante substituído a cada tick — nunca += entre ticks.
            try:
                pe = float(op.get("preco", 0) or 0)
                pa = float(p_atual)
                if pe > 0 and pa > 0:
                    alav_u = float(op.get("alav", self.cfg.get("alavancagem", 20)))
                    marg_u = float(op.get("margem", 0) or 0)
                    if op.get("tipo") == "SHORT":
                        pct_u = (pe - pa) / pe
                    else:
                        pct_u = (pa - pe) / pe
                    op["pnl_flutuante_usd"] = marg_u * (pct_u * alav_u)
                else:
                    op["pnl_flutuante_usd"] = 0.0
            except (TypeError, ValueError, ZeroDivisionError):
                op["pnl_flutuante_usd"] = 0.0

            if self.cfg.get("active_exit_strategy") == "GUARDIAN_AI":
                pass  # Trailing Stop ignorado. Córtex neural assumiu o controle de fechamento.
            else:
                atr_ts = float(op.get("atr_dca", p_atual * 0.01))
                preco_entrada = float(op["preco"])
                
                # Defina o Stop Loss inicial (Hard Stop) a uma distância de 1.0 * ATR do pivô de entrada
                if not op.get("_hard_stop_definido"):
                    op["_hard_stop_definido"] = True
                    distancia_sl = 1.0 * atr_ts
                    if op["tipo"] == "LONG":
                        novo_sl = preco_entrada - distancia_sl
                    else:
                        novo_sl = preco_entrada + distancia_sl
                    op["sl"] = self._round_price_to_tick(ativo, novo_sl)
                    if not getattr(self, "modo_lateral_macro_liberado", False):
                        self._atualizar_sl_fisico_bybit(ativo, op["tipo"], op["sl"])
                    op["sl_fisico"] = op["sl"]

                fee_buf = float(self.cfg.get("breakeven_fee_buffer_pct", 0.0018) or 0.0018)
                
                # Scale-Out parcial: Quando o lucro aberto bater a proporção de 2R (duas vezes o risco)
                distancia_risco = 1.0 * atr_ts
                gatilho_2r = distancia_risco * 2.0
                
                if not op.get("scale_out_feito"):
                    if (op["tipo"] == "LONG" and (p_atual - preco_entrada) >= gatilho_2r) or \
                       (op["tipo"] == "SHORT" and (preco_entrada - p_atual) >= gatilho_2r):
                        self.log_msg(f"💸 [SCALE-OUT 2R] Realizando lucro parcial (30%) em {ativo} ({op['tipo']})!")
                        op["scale_out_feito"] = True
                        
                        # Fecha 30% da posição
                        def _run_scale_2r():
                            try:
                                if not getattr(self, "is_real_account_mode", lambda: False)():
                                    return
                                client = self.get_bybit_client()
                                qty_fechar = str(round(float(op["qtd"]) * 0.30, 4))
                                side_close = "Sell" if op["tipo"] == "LONG" else "Buy"
                                client.place_order(
                                    category="linear",
                                    symbol=ativo,
                                    side=side_close,
                                    orderType="Market",
                                    qty=qty_fechar,
                                    reduceOnly=True,
                                )
                            except Exception as e:
                                self.erro_msg(f"Erro Scale-Out 2R: {e}")
                                
                        if hasattr(self, "enqueue_disk_io"):
                            self.enqueue_disk_io(_run_scale_2r)
                        
                        op["qtd"] = str(float(op["qtd"]) * 0.70)
                        
                        # Move Hard Stop restante para Breakeven + taxas
                        if op["tipo"] == "LONG":
                            novo_sl = preco_entrada * (1.0 + fee_buf)
                        else:
                            novo_sl = preco_entrada * (1.0 - fee_buf)
                        op["sl"] = self._round_price_to_tick(ativo, novo_sl)
                        if not getattr(self, "modo_lateral_macro_liberado", False):
                            self._atualizar_sl_fisico_bybit(ativo, op["tipo"], op["sl"])
                        op["sl_fisico"] = op["sl"]

                # Chandelier Exit: gatilho de fechamento deve seguir o preço a uma distância exata de 2.5 * ATR
                offset_chandelier = 2.5 * atr_ts
                profit_lock_ratio = float(self.cfg.get("trailing_profit_lock_ratio", 0.80) or 0.80)
                profit_lock_ratio = max(0.0, min(0.98, profit_lock_ratio))
                watermark_trigger = float(self.cfg.get("trailing_watermark_activation_pct", 0.03) or 0.03)
                watermark_trigger = max(0.0, watermark_trigger)
                if op["tipo"] == "LONG":
                    op["max_price_reached"] = max(
                        float(op.get("max_price_reached", preco_entrada) or preco_entrada),
                        float(p_atual),
                    )
                    novo_sl_proposto = p_atual - offset_chandelier
                    lucro_maximo = op["max_price_reached"] - preco_entrada
                    if preco_entrada > 0 and (lucro_maximo / preco_entrada) >= watermark_trigger:
                        sl_watermark = preco_entrada + (lucro_maximo * profit_lock_ratio)
                        novo_sl_proposto = max(novo_sl_proposto, sl_watermark)
                    novo_sl_proposto = self._round_price_to_tick(ativo, novo_sl_proposto)
                    if novo_sl_proposto > op["sl"]:
                        op["sl"] = novo_sl_proposto
                        if not getattr(self, "modo_lateral_macro_liberado", False):
                            self._atualizar_sl_fisico_bybit(ativo, op["tipo"], op["sl"])
                        op["sl_fisico"] = op["sl"]
                elif op["tipo"] == "SHORT":
                    op["max_price_reached"] = max(
                        float(op.get("max_price_reached", preco_entrada) or preco_entrada),
                        float(p_atual),
                    )
                    op["min_price_reached"] = min(
                        float(op.get("min_price_reached", preco_entrada) or preco_entrada),
                        float(p_atual),
                    )
                    novo_sl_proposto = p_atual + offset_chandelier
                    lucro_maximo = preco_entrada - op["min_price_reached"]
                    if preco_entrada > 0 and (lucro_maximo / preco_entrada) >= watermark_trigger:
                        sl_watermark = preco_entrada - (lucro_maximo * profit_lock_ratio)
                        novo_sl_proposto = min(novo_sl_proposto, sl_watermark)
                    novo_sl_proposto = self._round_price_to_tick(ativo, novo_sl_proposto)
                    if novo_sl_proposto < op["sl"]:
                        op["sl"] = novo_sl_proposto
                        if not getattr(self, "modo_lateral_macro_liberado", False):
                            self._atualizar_sl_fisico_bybit(ativo, op["tipo"], op["sl"])
                        op["sl_fisico"] = op["sl"]

            # 3. VETO DO GUARDIÃO (PANIC SELL via VPIN)
            if hasattr(self, "vpin_data") and ativo in self.vpin_data:
                vd = self.vpin_data[ativo]
                total_vol = vd["buy_vol"] + vd["sell_vol"]
                if total_vol > 0:
                    buy_pct = vd["buy_vol"] / total_vol
                    sell_pct = vd["sell_vol"] / total_vol
                    hard_stop_hit = self._guardian_hard_stop_hit(
                        op, 0.0, float(p_atual)
                    )
                    vpin_panic_allowed = (
                        hard_stop_hit or not self._guardian_grace_active(op)
                    )
                    if vpin_panic_allowed and op["tipo"] == "LONG" and sell_pct > 0.98:
                        self.log_msg(f"🚨 [VETO GUARDIÃO] Toxicidade institucional detectada (Sell {sell_pct*100:.1f}%) contra o LONG em {ativo}. PANIC SELL!")
                        op["fechar_agora"] = "PANIC_SELL (Toxic Flow)"
                    elif vpin_panic_allowed and op["tipo"] == "SHORT" and buy_pct > 0.98:
                        self.log_msg(f"🚨 [VETO GUARDIÃO] Toxicidade institucional detectada (Buy {buy_pct*100:.1f}%) contra o SHORT em {ativo}. PANIC SELL!")
                        op["fechar_agora"] = "PANIC_SELL (Toxic Flow)"

            if op.get("fechar_agora"):
                resultado = op["fechar_agora"]
            elif op["tipo"] == "LONG":
                if p_atual <= op["sl"]:
                    resultado = "PERDA (SL) OU GANHO (CHANDELIER)"
            elif op["tipo"] == "SHORT":
                if p_atual >= op["sl"]:
                    resultado = "PERDA (SL) OU GANHO (CHANDELIER)"

            if resultado:
                alav = self._coerce_float_br(op.get("alav", self.cfg["alavancagem"]))
                margem_f = self._coerce_float_br(op.get("margem", 0) or 0)
                pe = self._coerce_float_br(op.get("preco", 0) or 0)
                pa = float(p_atual)
                preco_execucao = pa
                sl_atual = float(op.get("sl", pa) or pa)
                if op["tipo"] == "LONG" and pa <= sl_atual:
                    # Simulação protegida: quando o stop já foi travado no lucro,
                    # não considerar execução pior que o próprio SL no cálculo do PnL final.
                    preco_execucao = max(pa, sl_atual)
                elif op["tipo"] == "SHORT" and pa >= sl_atual:
                    preco_execucao = min(pa, sl_atual)
                if pe <= 0:
                    pct = 0.0
                elif op["tipo"] == "LONG":
                    pct = (preco_execucao - pe) / pe
                else:
                    pct = (pe - preco_execucao) / pe
                # Realizado no fecho: (Δpreço/preço) * notional; notional = margem * alav (coerente com linear USDT).
                lucro = margem_f * (pct * alav)

                # BACKTEST SIMULADOR: Aplicar taxas na saída
                if (
                    self.cfg["use_backtest"]
                    and not getattr(
                        self,
                        "is_real_account_mode",
                        lambda: bool(getattr(self, "modo_real", False)),
                    )()
                ):
                    fee_percentual = 0.0004  # 0.04% Taker fee
                    fee_total = (margem_f * alav) * fee_percentual * 2  # Open + Close
                    lucro -= fee_total

                if resultado == "PERDA (SL)" and lucro > 0:
                    resultado = "TRAILING STOP (WIN)"

                if "EXIT" in resultado:
                    tipo_saida = (
                        resultado.split(" (")[1].replace(")", "")
                        if "(" in resultado
                        else "INTELIGENTE"
                    )
                    resultado = (
                        f"GANHO ({tipo_saida})"
                        if lucro > 0
                        else f"PERDA ({tipo_saida})"
                    )

                is_real_mode = getattr(
                    self,
                    "is_real_account_mode",
                    lambda: bool(getattr(self, "modo_real", False)),
                )()

                if is_real_mode:
                    # A API Rest da Binance é muito lenta e causava Thread Deadlock no V87 / V90 FINAL.
                    # Guardamos a intenção de fechar e só chamamos a rede FORA do "with self._ops_lock:"
                    ordens_pendentes_fechamento_real.append(
                        (
                            ativo,
                            op["tipo"],
                            op.get("qty_real", 0),
                            op.get("qtd"),
                            lucro,
                            resultado,
                            alav,
                            op["margem"],
                            op.get("ia_prob", 0),
                            op.get("atr_dca", 1.0),
                            op.get("certeza", 0.0),
                            op.get("arena_regime", "SNIPER"),
                            float(pe),
                            float(preco_execucao),
                        )
                    )
                    continue

                pnl_hist, pnl_pct_hist = self._calcular_campos_pnl_trade(
                    op, float(preco_execucao), lucro_fallback=lucro
                )
                lucro = float(pnl_hist)

                # Simulação: remove a posição com pop atômico — segunda passagem não credita saldo.
                with self._ops_lock:
                    if self.operacoes_abertas.pop(ativo, None) is None:
                        continue
                    if hasattr(self, "_ajustar_saldo_pos_fechamento"):
                        self._ajustar_saldo_pos_fechamento(lucro, False)
                    else:
                        self.saldo_atual += lucro
                self._radar_enqueue_saldo_persist()
                self._registrar_fechamento_sessao(
                    ativo=ativo,
                    op=op,
                    resultado=resultado,
                    pnl_hist=float(lucro),
                    pnl_pct_hist=float(pnl_pct_hist),
                    preco_saida=float(preco_execucao),
                    origem_label="AUTÔNOMO",
                    op_tipo=op["tipo"],
                )
                ativos_para_fechar.append(ativo)

        with self._ops_lock:
            for ativo, op in snapshots.items():
                if ativo not in self.operacoes_abertas:
                    continue
                if ativo in ativos_para_fechar:
                    continue
                live = self.operacoes_abertas.get(ativo)
                if isinstance(live, dict):
                    # Evita sobrescrever flags já gravadas (snapshot mais antigo / corrida).
                    if live.get("scale_out_feito") or op.get("scale_out_feito"):
                        op["scale_out_feito"] = True
                    if live.get("trailing_tp_active") or op.get("trailing_tp_active"):
                        op["trailing_tp_active"] = True
                    if live.get("scale_out_guardian_feito") or op.get(
                        "scale_out_guardian_feito"
                    ):
                        op["scale_out_guardian_feito"] = True
                self.operacoes_abertas[ativo] = op
            for a in ativos_para_fechar:
                if a in self.operacoes_abertas:
                    self.log_msg(f"Removendo {a} das operacoes abertas")
                    del self.operacoes_abertas[a]

        # Merge aplicado; _ops_lock liberado entre ticks de radar.

        # [FIX ANTI-FREEZE] Disparo de Rede (Bybit) totalmente assíncrono à trava
        is_real_mode = getattr(
            self,
            "is_real_account_mode",
            lambda: bool(getattr(self, "modo_real", False)),
        )()
        if is_real_mode and ordens_pendentes_fechamento_real:
            for (
                ativo,
                op_tipo,
                qty_real,
                qtd_snap,
                lucro,
                resultado,
                alav,
                op_margem,
                prob_ia,
                atr_dca,
                certeza,
                arena_regime_h,
                pe_snap,
                preco_exec_snap,
            ) in ordens_pendentes_fechamento_real:
                if getattr(self, "_api_auth_fatal", False):
                    break
                qty_close = float(qty_real or 0)
                if qty_close <= 0 and qtd_snap is not None:
                    try:
                        qty_close = abs(float(qtd_snap))
                    except (TypeError, ValueError):
                        qty_close = 0.0
                fechado = self.fechar_ordem_real(ativo, op_tipo, qty_close)
                if fechado:
                    try:
                        preco_saida_real = float(
                            _preco_ultimo_precos_buffer(
                                getattr(self, "precos_buffer", {}),
                                ativo,
                                0.0,
                            )
                            or 0.0
                        )
                        if preco_saida_real <= 0:
                            preco_saida_real = float(preco_exec_snap or 0.0)
                        op_for_pnl = {
                            "preco": float(pe_snap or 0.0),
                            "tipo": op_tipo,
                            "margem": float(op_margem),
                            "alav": float(alav),
                            "qty_real": float(qty_close or 0),
                            "qtd": qtd_snap,
                        }
                    except Exception:
                        op_for_pnl = {
                            "preco": float(pe_snap or 0.0),
                            "tipo": op_tipo,
                            "margem": float(op_margem),
                            "alav": float(alav),
                            "qty_real": float(qty_close or 0),
                            "qtd": qtd_snap,
                        }
                        preco_saida_real = float(preco_exec_snap or 0.0)
                    pnl_hist, pnl_pct_hist = self._calcular_campos_pnl_trade(
                        op_for_pnl, preco_saida_real, lucro_fallback=lucro
                    )
                    lucro = float(pnl_hist)
                    # [AIRBAG V90 FINAL.4] Limpar ordens pendentes (SL físico de emergência) após fecho
                    if hasattr(self, "loop") and self.loop:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                self._limpar_airbag_async(ativo), self.loop
                            )
                        except Exception as e:
                            self.erro_msg(
                                f"⚠️ Erro ao agendar limpeza do Airbag de {ativo}: {e}"
                            )
                    # Trava super-curta só para remover do dicionário pós-confirmação
                    with self._ops_lock:
                        if ativo in self.operacoes_abertas:
                            self.log_msg(
                                f"Removendo {ativo} das operacoes abertas (PÓS-REDE)"
                            )
                            del self.operacoes_abertas[ativo]
                        if hasattr(self, "_ajustar_saldo_pos_fechamento"):
                            self._ajustar_saldo_pos_fechamento(lucro, True)
                        else:
                            self.saldo_atual += lucro
                    self._radar_enqueue_saldo_persist()
                    op_hist = {
                        "tipo": op_tipo,
                        "margem": float(op_margem),
                        "alav": float(alav),
                        "certeza": float(certeza),
                        "arena_regime": (
                            arena_regime_h
                            if arena_regime_h is not None
                            else "SNIPER"
                        ),
                        "ia_prob": float(prob_ia),
                        "atr_dca": float(atr_dca),
                    }
                    self._registrar_fechamento_sessao(
                        ativo=ativo,
                        op=op_hist,
                        resultado=resultado,
                        pnl_hist=float(lucro),
                        pnl_pct_hist=float(pnl_pct_hist),
                        preco_saida=float(preco_saida_real or 0.0),
                        origem_label="AUTÔNOMO",
                        op_tipo=op_tipo,
                        margem_usada=float(op_margem),
                        alav=float(alav),
                        certeza=float(certeza),
                        arena_regime=(
                            arena_regime_h if arena_regime_h is not None else "SNIPER"
                        ),
                        prob_ia=float(prob_ia),
                        atr_op=float(atr_dca),
                    )

    async def _armar_airbag_async(self, symbol, tipo, preco_entrada):
        """[AIRBAG V90 FINAL.6] SL de emergência (~4%) via set_trading_stop (Bybit V5 linear)."""
        if not getattr(
            self,
            "is_real_account_mode",
            lambda: bool(getattr(self, "modo_real", False)),
        )():
            return
        if symbol is None:
            return
        sym = str(symbol).strip()
        if not sym or sym.lower() in ("none", "null", ""):
            self.log_msg(
                "🛡️ [RISK] Airbag (set_trading_stop) abortado: símbolo ausente ou inválido."
            )
            return
        symbol = sym

        pct_emergencia = 0.04
        loop = asyncio.get_running_loop()
        client = self.get_bybit_client()

        def _mark_price():
            try:
                from bybit_helpers import get_mark_price_and_funding

                m = get_mark_price_and_funding(client, symbol)
                return float(m.get("markPrice", preco_entrada))
            except Exception:
                return float(preco_entrada)

        mark_px = await loop.run_in_executor(None, _mark_price)
        preco_calc = (
            mark_px * (1 - pct_emergencia)
            if tipo == "LONG"
            else mark_px * (1 + pct_emergencia)
        )
        preco_sl_formatado = self._round_price_to_tick(symbol, preco_calc)

        def _armar():
            client.set_trading_stop(
                category="linear",
                symbol=symbol,
                positionIdx=0,
                stopLoss=str(preco_sl_formatado),
                slTriggerBy="MarkPrice",
            )

        try:
            await loop.run_in_executor(None, _armar)
            self.log_msg(
                f"🛡️ [AIRBAG] {symbol}: SL de emergência armado em {preco_sl_formatado}"
            )
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(
                f"⚠️ Falha ao armar Airbag para {symbol}: {format_bybit_exception(e)}"
            )
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(f"⚠️ Falha ao armar Airbag para {symbol}: {e}")

    async def _limpar_airbag_async(self, symbol):
        """Após fechamento real: remove ordens pendentes do símbolo (Bybit)."""
        if symbol is None:
            return
        sym = str(symbol).strip()
        if not sym or sym.lower() in ("none", "null", ""):
            self.log_msg(
                "🛡️ [RISK] Limpeza Airbag abortada: símbolo ausente ou inválido."
            )
            return
        symbol = sym

        loop = asyncio.get_running_loop()
        client = self.get_bybit_client()
        try:

            def cancel_rest():
                client.cancel_all_orders(category="linear", symbol=symbol)

            await loop.run_in_executor(None, cancel_rest)
            self.log_msg(f"🧹 [AIRBAG] {symbol}: ordens residuais limpas.")
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(
                f"⚠️ Erro ao limpar Airbag de {symbol}: {format_bybit_exception(e)}"
            )
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(f"⚠️ Erro ao limpar Airbag de {symbol}: {e}")

    async def _enviar_ordem_ws(self, method, params):
        """Roteamento HFT: mesmo caminho que WS-FAPI Binance, executado via REST Bybit."""
        if method != "order.place":
            return
        loop = asyncio.get_running_loop()
        client = self.get_bybit_client()

        def _place():
            side = bybit_side_from_binance(params.get("side", ""))
            sym = params.get("symbol")
            qty = params.get("quantity")
            qty_str = str(qty)
            reduce_only = str(params.get("reduceOnly", "")).lower() == "true"
            otype = (params.get("type") or "MARKET").upper()
            po = {
                "category": "linear",
                "symbol": sym,
                "side": side,
                "orderType": "Market" if otype == "MARKET" else "Limit",
                "qty": qty_str,
                "positionIdx": 0,
            }
            if reduce_only:
                po["reduceOnly"] = True
            if po["orderType"] == "Limit":
                po["price"] = str(params.get("price", ""))
                po["timeInForce"] = params.get("timeInForce") or "GTC"
            client.place_order(**po)
            self.log_msg(f"✅ ORDEM HFT (REST Bybit) {sym} {side}")

        try:
            await loop.run_in_executor(None, _place)
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(f"ERRO Bybit: {format_bybit_exception(e)}")
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return
            self.erro_msg(f"FALHA HFT REST: {e}")

    def executar_ordem_real(self, ativo, lado, margem, alavancagem, preco_atual):
        """
        Envia ordem real após margem ISOLADA + gate (exchange + caixa).
        Retorno: (sucesso, qty_executável, margem_usada_pós_gate).
        """
        try:
            if ativo is None:
                return False, 0, 0.0
            sym = str(ativo).strip()
            if not sym or sym.lower() in ("none", "null", ""):
                self.log_msg("🛡️ [RISK] Disparo abortado: símbolo ausente ou inválido.")
                return False, 0, 0.0
            ativo = sym

            if not self.ensure_isolated_margin_linear(ativo, int(alavancagem)):
                self.log_msg(
                    f"🛡️ [RISK] Disparo abortado em {ativo}: não foi possível confirmar margem ISOLADA."
                )
                return False, 0, 0.0

            client = self.get_bybit_client()
            lev_cache = getattr(self, "_leverage_by_symbol", None)
            if lev_cache is None:
                self._leverage_by_symbol = {}
                lev_cache = self._leverage_by_symbol
            prev_lev = lev_cache.get(ativo)
            if prev_lev != alavancagem:
                client.set_leverage(
                    category="linear",
                    symbol=ativo,
                    buyLeverage=str(alavancagem),
                    sellLeverage=str(alavancagem),
                )
                lev_cache[ativo] = alavancagem

            # [E4] Cache de exchange_info (singleton 24h em CoreMixin)
            info = self.get_futures_exchange_info_cached(client, ttl_sec=86400)
            available_balance = self.get_available_balance_usdt_for_orders()
            gate = validate_linear_open_order(
                symbol=ativo,
                margem=float(margem),
                leverage=int(alavancagem),
                mark_price=float(preco_atual),
                exchange_info=info,
                available_balance_usdt=available_balance,
                max_order_usd=float(self.cfg.get("max_order_usd", 500_000.0)),
                margin_buffer_pct=float(self.cfg.get("order_margin_buffer_pct", 0.002)),
            )
            if not gate.ok:
                self.log_msg(
                    f"🛡️ [RISK] Disparo abortado em {ativo} (pré-envio): {gate.motivo}"
                )
                return False, 0, 0.0
            margem = gate.margem_usada
            qty_formatada = gate.qty
            tick_size = gate.tick_size

            limite_risco = float(self.cfg.get("risk_threshold", 0.02) or 0.02)
            hard_sl = float(self.cfg.get("hard_sl_pct", 0.015) or 0.015)
            if limite_risco <= 0:
                limite_risco = 0.02
            if hard_sl <= 0:
                hard_sl = 0.015
            notional_est = float(qty_formatada) * float(preco_atual)
            saldo_ref = max(float(available_balance), 1e-9)
            loss_sl = notional_est * hard_sl
            risk_frac = loss_sl / saldo_ref
            if risk_frac > limite_risco + 1e-12:
                max_loss_ok = limite_risco * saldo_ref
                max_notional = max_loss_ok / hard_sl
                if max_notional + 1e-9 < float(gate.min_notional):
                    self.log_msg(
                        "WARNING: Limite de Risco Excedido — operação abortada "
                        f"(risco {risk_frac:.4f} > cap {limite_risco}; abaixo do min_notional)."
                    )
                    return False, 0, 0.0
                qty_scaled = max_notional / float(preco_atual)
                qty_adj, _prec = round_qty_to_step(qty_scaled, float(gate.step_size))
                notional_adj = float(qty_adj) * float(preco_atual)
                if qty_adj + 1e-12 < float(gate.min_qty) or notional_adj + 1e-9 < float(
                    gate.min_notional
                ):
                    self.log_msg(
                        "WARNING: Limite de Risco Excedido — operação abortada após recálculo de lote."
                    )
                    return False, 0, 0.0
                margem_adj = notional_adj / float(max(1, int(alavancagem)))
                if margem_adj > available_balance + 1e-6:
                    self.log_msg(
                        "WARNING: Limite de Risco Excedido — aborto (lote reduzido excede saldo disponível)."
                    )
                    return False, 0, 0.0
                loss_adj = notional_adj * hard_sl
                if loss_adj / saldo_ref > limite_risco + 1e-9:
                    self.log_msg(
                        "WARNING: Limite de Risco Excedido — operação abortada (residual pós-floor)."
                    )
                    return False, 0, 0.0
                self.log_msg(
                    "WARNING: Limite de Risco Excedido — lote reduzido proporcionalmente "
                    f"(qty {qty_formatada} -> {qty_adj}; risco {risk_frac:.4f} -> {loss_adj / saldo_ref:.4f})."
                )
                qty_formatada = qty_adj
                margem = margem_adj

            side = "BUY" if lado == "LONG" else "SELL"
            bside = bybit_side_from_binance(side)
            tipo_exec = self.cfg.get("tipo_execucao", "TAKER")
            qty_str = str(qty_formatada)

            if self.cfg.get("use_ws_orders", False):
                self.log_msg(
                    f"⚡ ROTEAMENTO HFT: Disparando {lado} via REST Bybit (baixa latência)..."
                )
                params = {
                    "symbol": ativo,
                    "side": side,
                    "type": "MARKET" if tipo_exec == "TAKER" else "LIMIT",
                    "quantity": qty_formatada,
                }
                if tipo_exec != "TAKER":
                    price_precision = 0
                    if tick_size < 1:
                        price_precision = len(
                            f"{tick_size:.8f}".rstrip("0").split(".")[1]
                        )
                    preco_fmt = f"{preco_atual:.{price_precision}f}"
                    params["timeInForce"] = "GTC"
                    params["price"] = preco_fmt

                loop_hft = getattr(self, "loop_async", None)
                if loop_hft is not None and loop_hft.is_running():
                    fut = asyncio.run_coroutine_threadsafe(
                        self._enviar_ordem_ws("order.place", params), loop_hft
                    )
                    try:
                        fut.result(timeout=120.0)
                    except concurrent.futures.TimeoutError:
                        self.erro_msg(
                            f"FALHA CORRETORA ({ativo}): timeout aguardando confirmação "
                            "da ordem HFT (120s)."
                        )
                        return False, 0, 0.0
                else:
                    self.log_msg(
                        "⚠️ ROTEAMENTO HFT: loop assíncrono indisponível; "
                        "enviando ordem via REST síncrono."
                    )
                    if tipo_exec == "TAKER":
                        client.place_order(
                            category="linear",
                            symbol=ativo,
                            side=bside,
                            orderType="Market",
                            qty=qty_str,
                            positionIdx=0,
                        )
                    else:
                        price_precision = 0
                        if tick_size < 1:
                            price_precision = len(
                                f"{tick_size:.8f}".rstrip("0").split(".")[1]
                            )
                        preco_fmt = f"{preco_atual:.{price_precision}f}"
                        client.place_order(
                            category="linear",
                            symbol=ativo,
                            side=bside,
                            orderType="Limit",
                            qty=qty_str,
                            price=preco_fmt,
                            timeInForce="GTC",
                            positionIdx=0,
                        )
            else:
                self.log_msg(f"📡 ROTEAMENTO REST: Disparando {lado} ({tipo_exec})...")
                if tipo_exec == "TAKER":
                    client.place_order(
                        category="linear",
                        symbol=ativo,
                        side=bside,
                        orderType="Market",
                        qty=qty_str,
                        positionIdx=0,
                    )
                else:  # MAKER
                    price_precision = 0
                    if tick_size < 1:
                        price_precision = len(
                            f"{tick_size:.8f}".rstrip("0").split(".")[1]
                        )
                    preco_fmt = f"{preco_atual:.{price_precision}f}"
                    client.place_order(
                        category="linear",
                        symbol=ativo,
                        side=bside,
                        orderType="Limit",
                        qty=qty_str,
                        price=preco_fmt,
                        timeInForce="GTC",
                        positionIdx=0,
                    )

            return True, qty_formatada, float(margem)
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False, 0, 0.0
            self.erro_msg(f"FALHA CORRETORA ({ativo}): {format_bybit_exception(e)}")
            return False, 0, 0.0
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False, 0, 0.0
            self.erro_msg(f"FALHA CORRETORA ({ativo}): {e}")
            return False, 0, 0.0

    def configurar_ordens_protecao_exchange(self, ativo, sinal, qty_real, sl, tp):
        try:
            if not getattr(
                self,
                "is_real_account_mode",
                lambda: bool(getattr(self, "modo_real", False)),
            )():
                return False
            if ativo is None:
                return False
            sym = str(ativo).strip()
            if not sym or sym.lower() in ("none", "null", ""):
                self.log_msg(
                    "🛡️ [RISK] Trailing/proteção (set_trading_stop) abortada: "
                    "símbolo ausente ou inválido."
                )
                return False
            ativo = sym

            if qty_real is None or float(qty_real) <= 0:
                return False
            client = self.get_bybit_client()
            sl_px = self._round_price_to_tick(ativo, sl)
            tp_px = self._round_price_to_tick(ativo, tp)

            client.set_trading_stop(
                category="linear",
                symbol=ativo,
                positionIdx=0,
                stopLoss=str(sl_px),
                takeProfit=str(tp_px),
                slTriggerBy="MarkPrice",
                tpTriggerBy="MarkPrice",
            )
            return True
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False
            logger.exception(
                "exchange_protection_order_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                ativo,
                {"sinal": sinal, "qty_real": qty_real, "sl": sl, "tp": tp},
            )
            return False
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False
            logger.exception(
                "exchange_protection_order_failed | ts=%s | ativo=%s | payload=%s",
                int(time.time() * 1000),
                ativo,
                {"sinal": sinal, "qty_real": qty_real, "sl": sl, "tp": tp},
            )
            return False

    def fechar_ordem_real(self, ativo, lado_original, qty, allow_retry_enqueue: bool = True):
        try:
            if ativo is None:
                return False
            sym = str(ativo).strip()
            if not sym or sym.lower() in ("none", "null", ""):
                self.log_msg(
                    "🛡️ [RISK] Fechamento abortado: símbolo ausente ou inválido."
                )
                return False
            ativo = sym

            client = self.get_bybit_client()
            side = "SELL" if lado_original == "LONG" else "BUY"
            bside = bybit_side_from_binance(side)
            qty_str = str(qty)

            if self.cfg.get("use_ws_orders", False):
                self.log_msg(
                    f"⚡ ROTEAMENTO HFT: Fechando {ativo} via REST Bybit (confirmação síncrona para evitar operação fantasma)..."
                )
            client.place_order(
                category="linear",
                symbol=ativo,
                side=bside,
                orderType="Market",
                qty=qty_str,
                reduceOnly=True,
                positionIdx=0,
            )
            return True
        except FailedRequestError as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False
            self.erro_msg(
                f"ERRO FECHAR CORRETORA ({ativo}): {format_bybit_exception(e)}"
            )
            if allow_retry_enqueue:
                self._enqueue_close_retry(
                    ativo=ativo,
                    lado_original=lado_original,
                    qty=float(qty or 0.0),
                    motivo=format_bybit_exception(e),
                )
            return False
        except Exception as e:
            if self._abortar_se_erro_autenticacao_api(e):
                return False
            self.erro_msg(f"ERRO FECHAR CORRETORA ({ativo}): {e}")
            if allow_retry_enqueue:
                self._enqueue_close_retry(
                    ativo=ativo,
                    lado_original=lado_original,
                    qty=float(qty or 0.0),
                    motivo=str(e),
                )
            return False

    def iniciar_bot(self):
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
            self.erro_msg("Por favor, configure a pasta Workspace antes de iniciar.")
            return

        if getattr(self, "is_real_account_mode", lambda: False)():
            try:
                self.log_msg("🔒 Validando credenciais Bybit Unified (linear USDT)...")
                client = self.get_bybit_client()
                try:
                    acc = client.get_wallet_balance(accountType="UNIFIED")
                except Exception as e:
                    low = str(e).lower()
                    if any(
                        token in low
                        for token in (
                            "not valid json",
                            "unexpected token",
                            "expecting value",
                            "internal server error",
                            "<html",
                            "<!doctype html",
                        )
                    ) or getattr(e, "status_code", None) in (500, 502, 503, 504):
                        self.log_msg(
                            "[REST HTTP] Falha ao decodificar JSON: corretora retornou HTML"
                        )
                        self.log_msg(
                            "⚠️ Corretora instável durante a validação Bybit. Backend permanece vivo em standby."
                        )
                        acc = {}
                    else:
                        raise
                if isinstance(acc, dict) and acc.get("retCode") not in (0, None):
                    raise RuntimeError(
                        acc.get("retMsg") or f"retCode={acc.get('retCode')}"
                    )
                if acc:
                    self.log_msg(
                        "✅ Autenticação Fogo Livre (Modo Real) Validada com Sucesso."
                    )
            except FailedRequestError as e:
                self.erro_msg(f"FALHA DE SEGURANÇA NA API: {format_bybit_exception(e)}")
                self.log_msg(
                    "A Bybit recusou a conexão. Verifique IP, permissões de contratos lineares e secret."
                )
                return
            except Exception as e:
                self.erro_msg(f"FALHA DE SEGURANÇA NA API: {e}")
                self.log_msg(
                    "A Bybit recusou a conexão. Verifique IP, permissões de contratos lineares e secret."
                )
                return

        # [V90 FIX] REMOVIDA a linha self.is_searching = True
        # A ignição agora é controlada EXCLUSIVAMENTE pelo botão do Painel React via /api/command

        # [V90] Carrega a IA do Guardião e faz um check-up histórico rápido na inicialização
        if hasattr(self, "forjar_guardiao_historico"):
            try:
                self.submit_background_task(self.forjar_guardiao_historico)
            except Exception as e:
                self.erro_msg(f"Falha ao iniciar Bootcamp do Guardião: {e}")

        if getattr(self, "ia_treinada", False) == False:
            try:
                self.log_msg(
                    "⏳ Disparando Thread de Forja Neural (Treinamento Início Rápido)..."
                )
                if hasattr(self, "forjar_ia_sniper_nexus"):
                    self.forjar_ia_sniper_nexus()
                    self._limpar_acumuladores_virtuais_pos_forja()
                else:
                    self.submit_background_task(self.treinar_ia)
            except Exception as e:
                self.erro_msg(f"Erro ao disparar Thread de Treino: {e}")
        else:
            # [M5] Proteção Max Drawdown: bloqueia operações se saldo caiu muito
            if self._drawdown_sessao_pode_avaliar():
                saldo_ini = getattr(self, "_saldo_inicial_sessao", None)
                saldo_now = float(getattr(self, "saldo_atual", 1000) or 0.0)
                if saldo_ini is None:
                    self._saldo_inicial_sessao = saldo_now
                elif saldo_now < float(saldo_ini) * 0.70:
                    self.log_msg(
                        "⚠️ MAX DRAWDOWN: Saldo real confirmado caiu significativamente nesta sessão. Bot pausado para proteção."
                    )
                    self.pausar_bot()
                    return

            self.log_msg(
                f"▶ BUSCA NEURAL ATIVADA | Certeza Exigida: {self.cfg.get('winrate_minimo', 80)}% | Ambiente: {'REAL' if getattr(self, 'modo_real', False) else 'SIMULAÇÃO'}"
            )

            # Quando re-iniciado com a IA já treinada, dispara a Thread de Aprendizado Contínuo para o SSD
            if hasattr(self, "loop_aprendizado_continuo"):
                self.submit_background_task(self.loop_aprendizado_continuo)

    def pausar_bot(self):
        self.is_searching = False
        # [FIX] Resetar marcador do loop de análise para permitir reinício
        self._loop_analise_started = False
        # [A3] Resetar marcador de drawdown para que cada sessão inicie limpa
        self._saldo_inicial_sessao = None
        self.log_msg("⏸ BUSCA PAUSADA. Operações em andamento continuam protegidas.")
