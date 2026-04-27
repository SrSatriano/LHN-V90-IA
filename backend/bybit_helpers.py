"""
Utilitários Bybit Unified V5 (linear USDT) — formato compatível com o pipeline Binance legado.
"""

from __future__ import annotations

import functools
import json
import logging
import math
import os
import re
import socket
import threading
import time
from typing import (Any, Callable, Dict, List, Optional, Sequence, Tuple,
                    TypeVar, Union)

try:
    from pybit.exceptions import FailedRequestError, InvalidRequestError
except ImportError:  # pragma: no cover
    FailedRequestError = Exception  # type: ignore
    InvalidRequestError = Exception  # type: ignore

from pybit.unified_trading import HTTP

logger = logging.getLogger(__name__)

# Timeout explícito para todas as chamadas REST via cliente pybit (HTTP); evita hang indefinido.
BYBIT_REST_TIMEOUT_SEC = 10

# Respostas HTML / gateway instável — nunca parsear como JSON (evita crash no motor).
BYBIT_REST_UNSTABLE_HTTP = frozenset({500, 502, 503, 504})
_BYBIT_REST_DEGRADED_UNTIL: float = 0.0
_BYBIT_REST_LAST_DEGRADED_LOG_TS: float = 0.0


def _is_rest_json_decode_failure(exc: BaseException) -> bool:
    """Detecta payload HTML/erro de decode vindo da camada REST da corretora."""
    if isinstance(exc, json.JSONDecodeError):
        return True
    low = str(exc).lower()
    return any(
        token in low
        for token in (
            "not valid json",
            "unexpected token",
            "expecting value",
            "internal server error",
            "<html",
            "<!doctype html",
        )
    )


def _is_dns_resolution_failure(exc: BaseException) -> bool:
    if isinstance(exc, socket.gaierror):
        return True
    low = str(exc).lower()
    return any(
        token in low
        for token in (
            "getaddrinfo failed",
            "failed to resolve",
            "name resolution",
            "temporary failure in name resolution",
        )
    )


def _is_rate_limit_failure(exc: BaseException) -> bool:
    low = str(exc).lower()
    return any(
        token in low
        for token in (
            "10006",
            "too many visits",
            "rate limit",
            "x-bapi-limit-reset-timestamp",
        )
    )


def _extract_rate_limit_reset_wait_sec(exc: BaseException) -> Optional[float]:
    """Lê x-bapi-limit-reset-timestamp do erro/headers e converte em segundos de espera."""
    candidates: List[Any] = []
    for attr in ("headers", "response_headers"):
        h = getattr(exc, attr, None)
        if h:
            candidates.append(h)
    response = getattr(exc, "response", None)
    if response is not None:
        h = getattr(response, "headers", None)
        if h:
            candidates.append(h)

    reset_raw: Any = None
    for headers in candidates:
        try:
            reset_raw = headers.get("x-bapi-limit-reset-timestamp") or headers.get(
                "X-Bapi-Limit-Reset-Timestamp"
            )
        except AttributeError:
            continue
        if reset_raw:
            break

    if not reset_raw:
        match = re.search(
            r"x-bapi-limit-reset-timestamp['\"\s:=]+(\d{10,13})",
            str(exc),
            flags=re.IGNORECASE,
        )
        if match:
            reset_raw = match.group(1)

    if not reset_raw:
        return None
    try:
        reset_val = float(str(reset_raw).strip())
    except (TypeError, ValueError):
        return None
    reset_sec = reset_val / 1000.0 if reset_val > 10_000_000_000 else reset_val
    wait = reset_sec - time.time()
    return max(0.05, wait) if wait > 0 else 0.05


def _mark_rest_degraded(wait_sec: float, *, context: str, reason: str) -> None:
    global _BYBIT_REST_DEGRADED_UNTIL, _BYBIT_REST_LAST_DEGRADED_LOG_TS
    now = time.time()
    _BYBIT_REST_DEGRADED_UNTIL = max(_BYBIT_REST_DEGRADED_UNTIL, now + wait_sec)
    if now - _BYBIT_REST_LAST_DEGRADED_LOG_TS >= 10.0:
        _BYBIT_REST_LAST_DEGRADED_LOG_TS = now
        logger.warning(
            "[REST DEGRADED] standby %.1fs | ctx=%s | motivo=%s",
            wait_sec,
            context,
            reason,
        )


def _log_rest_json_decode_failure(
    exc: BaseException, *, context: str = "bybit_rest"
) -> None:
    logger.warning(
        "[REST HTTP] Falha ao decodificar JSON: corretora retornou HTML | ctx=%s | err=%s",
        context,
        exc,
    )


def rest_unstable_status_from_exc(exc: BaseException) -> Optional[int]:
    """Retorna o código HTTP se for erro de servidor instável (5xx típico da corretora)."""
    code = getattr(exc, "status_code", None)
    if code is None:
        return None
    try:
        c = int(code)
    except (TypeError, ValueError):
        return None
    return c if c in BYBIT_REST_UNSTABLE_HTTP else None


def get_linear_tickers_list_safe(
    client: HTTP,
) -> Tuple[str, Union[List[Dict[str, Any]], int, None]]:
    """
    get_tickers(linear) com tratamento de 5xx / corpo inválido — não propaga crash de parsing.

    Retornos:
        ("ok", lista de dicts de ticker)
        ("unstable", int)  — código HTTP 500/502/503/504; caller deve aguardar e não parsear
        ("error", None)    — outras falhas ou resposta inesperada
    """
    try:
        res = client.get_tickers(category="linear")
    except Exception as e:
        u = rest_unstable_status_from_exc(e)
        if u is not None:
            return ("unstable", u)
        if _is_rest_json_decode_failure(e):
            _log_rest_json_decode_failure(e, context="get_linear_tickers")
        logger.debug("get_linear_tickers_list_safe request error: %s", e)
        return ("error", None)

    if not isinstance(res, dict):
        logger.warning(
            "[REST HTTP] Falha ao decodificar JSON: corretora retornou payload inválido | ctx=%s | type=%s",
            "get_linear_tickers",
            type(res).__name__,
        )
        return ("error", None)

    # retCode != 0 — não forçar .json em corpo não estruturado
    rc = res.get("retCode")
    if rc not in (0, None, "0"):
        return ("error", None)

    r = res.get("result") or {}
    lst = r.get("list") if isinstance(r, dict) else None
    if not isinstance(lst, list):
        lst = []
    out: List[Dict[str, Any]] = []
    for t in lst:
        if not isinstance(t, dict):
            continue
        out.append(t)
    return ("ok", out)


T = TypeVar("T")


def _is_retryable_bybit_error(exc: BaseException) -> bool:
    """429, 5xx e falhas transitórias de rede / rate limit."""
    if _is_rest_json_decode_failure(exc):
        return True
    if _is_dns_resolution_failure(exc) or _is_rate_limit_failure(exc):
        return True
    code = getattr(exc, "status_code", None)
    if code is not None:
        try:
            c = int(code)
            if c == 429 or (500 <= c <= 599):
                return True
        except (TypeError, ValueError):
            pass
    low = str(exc).lower()
    if "429" in low or "rate limit" in low or "too many requests" in low:
        return True
    if "503" in low or "502" in low or "504" in low or "500" in low:
        return True
    if "timeout" in low or "temporarily" in low:
        return True
    return isinstance(exc, (ConnectionError, OSError))


def _call_with_retry(
    fn: Callable[[], T],
    max_attempts: int = 5,
    delay_sec: float = 1.0,
    context: str = "bybit_rest",
) -> T:
    """Executa um callable sem argumentos com as mesmas regras do decorator."""
    last: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            is_rate_limit = _is_rate_limit_failure(e)
            es = str(e)
            unstable_http = rest_unstable_status_from_exc(e)
            is_json_decode_failure = _is_rest_json_decode_failure(e)
            is_dns_failure = _is_dns_resolution_failure(e)
            if "429" in es or "too many visits" in es.lower() or "10006" in es:
                is_rate_limit = True
            # ErrCode 10002: req_timestamp vs server_timestamp — comum ao mudar modo real; retry após pausa.
            is_timestamp_skew = "10002" in es or "timestamp" in es.lower()

            if attempt < max_attempts - 1 and (
                is_rate_limit
                or is_dns_failure
                or is_timestamp_skew
                or unstable_http is not None
                or is_json_decode_failure
                or _is_retryable_bybit_error(e)
            ):
                # Exponential Backoff para Rate Limit; 10002 costuma resolver com 1–2s
                if is_rate_limit:
                    reset_wait = _extract_rate_limit_reset_wait_sec(e)
                    wait_time = reset_wait if reset_wait is not None else delay_sec * (2**attempt)
                elif is_dns_failure:
                    wait_time = max(5.0, delay_sec * (2**attempt))
                elif is_timestamp_skew:
                    wait_time = max(1.0, delay_sec * 2)
                elif unstable_http is not None:
                    wait_time = max(1.0, delay_sec * (2**attempt))
                elif is_json_decode_failure:
                    wait_time = max(1.0, delay_sec)
                else:
                    wait_time = delay_sec
                if unstable_http is not None:
                    logger.warning(
                        "[REST HTTP %s] Corretora instável | ctx=%s | tentativa %s/%s | aguardando %.1fs",
                        unstable_http,
                        context,
                        attempt + 1,
                        max_attempts,
                        wait_time,
                    )
                elif is_dns_failure:
                    logger.warning(
                        "[REST DNS] Falha de resolução | ctx=%s | tentativa %s/%s | aguardando %.1fs | err=%s",
                        context,
                        attempt + 1,
                        max_attempts,
                        wait_time,
                        e,
                    )
                elif is_json_decode_failure:
                    logger.warning(
                        "[REST HTTP] Falha ao decodificar JSON: corretora retornou HTML | ctx=%s | tentativa %s/%s | aguardando %.1fs",
                        context,
                        attempt + 1,
                        max_attempts,
                        wait_time,
                    )
                else:
                    logger.warning(
                        "bybit_retry inline attempt %s/%s | ctx=%s: aguardando %.1fs. Erro: %s",
                        attempt + 1,
                        max_attempts,
                        context,
                        wait_time,
                        e,
                    )
                if is_rate_limit:
                    _mark_rest_degraded(
                        wait_time,
                        context=context,
                        reason="rate_limit_reset_timestamp",
                    )
                time.sleep(wait_time)
                continue
            raise
    assert last is not None
    raise last


def _call_dict_with_retry(
    fn: Callable[[], Any],
    *,
    max_attempts: int = 5,
    delay_sec: float = 1.0,
    context: str = "bybit_rest",
) -> Dict[str, Any]:
    """Variante segura para endpoints REST da Bybit que deveriam devolver dict."""
    if time.time() < _BYBIT_REST_DEGRADED_UNTIL:
        return {}
    try:
        res = _call_with_retry(
            fn,
            max_attempts=max_attempts,
            delay_sec=delay_sec,
            context=context,
        )
    except Exception as e:
        unstable_http = rest_unstable_status_from_exc(e)
        if unstable_http is not None:
            _mark_rest_degraded(10.0, context=context, reason=f"http_{unstable_http}")
            logger.warning(
                "[REST HTTP %s] Corretora instável, retornando payload vazio | ctx=%s",
                unstable_http,
                context,
            )
            return {}
        if _is_rest_json_decode_failure(e):
            _mark_rest_degraded(10.0, context=context, reason="invalid_json")
            _log_rest_json_decode_failure(e, context=context)
            return {}
        if _is_dns_resolution_failure(e):
            _mark_rest_degraded(20.0, context=context, reason="dns_resolution")
            logger.warning(
                "[REST DNS] Falha de resolução da Bybit, retornando payload vazio | ctx=%s",
                context,
            )
            return {}
        if _is_rate_limit_failure(e):
            reset_wait = _extract_rate_limit_reset_wait_sec(e)
            wait_time = reset_wait if reset_wait is not None else 15.0
            _mark_rest_degraded(wait_time, context=context, reason="rate_limit")
            logger.warning(
                "[REST RATE LIMIT] Standby temporário, retornando payload vazio | ctx=%s",
                context,
            )
            return {}
        if _is_retryable_bybit_error(e):
            _mark_rest_degraded(10.0, context=context, reason="transient_network")
            logger.warning(
                "[REST REDE] Falha transitória, retornando payload vazio | ctx=%s | err=%s",
                context,
                e,
            )
            return {}
        raise
    if not isinstance(res, dict):
        logger.warning(
            "[REST HTTP] Payload inesperado da Bybit | ctx=%s | type=%s",
            context,
            type(res).__name__,
        )
        return {}
    return res


def with_bybit_retry(
    max_attempts: int = 3, delay_sec: float = 1.0
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator fino: delega para _call_with_retry (429 / 5xx / rede)."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return _call_with_retry(
                lambda: fn(*args, **kwargs),
                max_attempts=max_attempts,
                delay_sec=delay_sec,
                context=getattr(fn, "__name__", "bybit_rest"),
            )

        return wrapper

    return decorator


CATEGORY_LINEAR = "linear"

# Limite máximo documentado Bybit V5 market/kline por pedido.
BYBIT_KLINE_MAX_LIMIT = 1000

# ~1 ano civil em ms (ingestão MTF / orçamento de páginas).
_MS_PER_YEAR = int(365.25 * 24 * 3600 * 1000)

# Intervalos MTF institucionais (string API Bybit, em minutos).
MTF_KLINE_INTERVALS_DEFAULT: Tuple[str, ...] = ("15", "60", "240")

# Fila global para REST market/get_kline — evita rajadas paralelas (ErrCode 10006).
_BYBIT_KLINE_LOCK = threading.Lock()
_NEXT_KLINE_OK: float = 0.0


def interval_minutes_from_api(interval: str) -> int:
    """Converte interval API linear (ex.: '15','60','240') para minutos inteiros."""
    s = str(interval or "").strip().upper()
    if s in ("D", "W", "M"):
        return {"D": 1440, "W": 10080, "M": 43200}.get(s, 1440)
    try:
        return max(1, int(s))
    except (TypeError, ValueError):
        return 15


def kline_bars_one_year(interval: str) -> int:
    """Número de barras necessárias para cobrir ~1 ano no timeframe dado."""
    mins = interval_minutes_from_api(interval)
    return int(math.ceil((365.25 * 24 * 60) / float(mins)))


def kline_max_pages_for_range_ms(range_ms: int, interval: str) -> int:
    """Páginas de até BYBIT_KLINE_MAX_LIMIT para cobrir range_ms (com folga)."""
    if range_ms <= 0:
        return 1
    mins = interval_minutes_from_api(interval)
    bars = int(math.ceil(range_ms / float(mins * 60 * 1000)))
    return max(50, min(2000, int(math.ceil(bars / float(BYBIT_KLINE_MAX_LIMIT))) + 80))


def get_kline_interval_sec() -> float:
    """Espaçamento mínimo entre inícios de chamadas get_kline (ajuste via BYBIT_KLINE_INTERVAL_SEC)."""
    try:
        return max(0.05, float(os.environ.get("BYBIT_KLINE_INTERVAL_SEC", "0.10")))
    except (TypeError, ValueError):
        return 0.10


def get_kline_throttled(client: HTTP, **kwargs: Any) -> Any:
    """
    Única porta recomendada para get_kline: serializa + BYBIT_KLINE_INTERVAL_SEC
    + circuit breaker para ErrCode 10006 / rate limit (backoff exponencial, sem buraco silencioso).
    """
    global _NEXT_KLINE_OK
    gap = get_kline_interval_sec()
    try:
        base_b = float(os.environ.get("BYBIT_KLINE_10006_BASE_SEC", "2.0"))
    except (TypeError, ValueError):
        base_b = 2.0
    try:
        max_iter = int(os.environ.get("BYBIT_KLINE_CIRCUIT_MAX_ITER", "64"))
    except (TypeError, ValueError):
        max_iter = 64
    backoff = max(0.5, base_b)
    max_backoff = 120.0
    sym = str(kwargs.get("symbol", "") or "")
    ctx = f"get_kline:{sym}"
    res: Any = {}

    for n in range(max(8, max_iter)):
        if time.time() < _BYBIT_REST_DEGRADED_UNTIL:
            wait_d = min(backoff, max(0.05, _BYBIT_REST_DEGRADED_UNTIL - time.time()))
            time.sleep(wait_d)

        exc: Optional[BaseException] = None
        res = None
        with _BYBIT_KLINE_LOCK:
            now = time.time()
            if now < _NEXT_KLINE_OK:
                time.sleep(_NEXT_KLINE_OK - now)
            try:
                res = client.get_kline(**kwargs)
            except Exception as e:
                exc = e
            finally:
                _NEXT_KLINE_OK = time.time() + gap

        if exc is not None:
            if _is_rate_limit_failure(exc) or "10006" in str(exc):
                reset_wait = _extract_rate_limit_reset_wait_sec(exc)
                wait_time = reset_wait if reset_wait is not None else backoff
                _mark_rest_degraded(
                    wait_time,
                    context=ctx,
                    reason="kline_rate_limit_reset_timestamp",
                )
                logger.warning(
                    "bybit_kline_circuit | ctx=%s | iter=%s/%s | backoff=%.1fs | err=%s",
                    ctx,
                    n + 1,
                    max_iter,
                    wait_time,
                    exc,
                )
                time.sleep(wait_time)
                backoff = min(max(backoff * 2.0, wait_time * 2.0), max_backoff)
                continue
            if _is_retryable_bybit_error(exc):
                logger.warning(
                    "bybit_kline_retry | ctx=%s | iter=%s | backoff=%.1fs | err=%s",
                    ctx,
                    n + 1,
                    backoff,
                    exc,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2.0, max_backoff)
                continue
            raise exc

        if isinstance(res, dict):
            rc = res.get("retCode")
            rcs = str(rc) if rc is not None else ""
            if rc in (0, "0", None) or rcs in ("", "0"):
                return res
            if rc == 10006 or rcs == "10006":
                reset_wait = _extract_rate_limit_reset_wait_sec(Exception(str(res)))
                wait_time = reset_wait if reset_wait is not None else backoff
                _mark_rest_degraded(
                    wait_time,
                    context=ctx,
                    reason="kline_retcode_10006",
                )
                logger.warning(
                    "bybit_kline_retcode_10006 | ctx=%s | iter=%s | backoff=%.1fs",
                    ctx,
                    n + 1,
                    wait_time,
                )
                time.sleep(wait_time)
                backoff = min(max(backoff * 2.0, wait_time * 2.0), max_backoff)
                continue
            return res
        return res

    logger.error("bybit_kline_circuit_exhausted | ctx=%s | returning last payload", ctx)
    return res if isinstance(res, dict) else {}


# Intervalos API Bybit (string) ↔ nomes usados no frontend / REST legado
INTERVAL_TO_BYBIT: Dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "1d": "D",
}


def map_interval(interval: str) -> str:
    return INTERVAL_TO_BYBIT.get(interval, "15")


def _ret_list(res: Any) -> List[Any]:
    if not isinstance(res, dict):
        return []
    r = res.get("result") or {}
    if not isinstance(r, dict):
        return []
    lst = r.get("list")
    return lst if isinstance(lst, list) else []


def ws_kline_candle_to_row(candle: Any) -> Optional[List[float]]:
    """
    Converte um objeto kline do WebSocket público V5 (linear) para o mesmo formato
    numérico de kline_row_to_binance_shape (12 colunas).
    """
    if not isinstance(candle, dict):
        return None
    try:
        t = int(candle.get("start") or candle.get("startTime") or 0)
        if t <= 0:
            return None
        o = float(candle.get("open") or 0.0)
        h = float(candle.get("high") or 0.0)
        l = float(candle.get("low") or 0.0)
        c = float(candle.get("close") or 0.0)
        v = float(candle.get("volume") or 0.0)
        qv = float(candle.get("turnover") or 0.0)
        end = candle.get("end")
        if end is not None:
            ct = int(end)
        else:
            ct = t + 899_999
        return [t, o, h, l, c, v, ct, qv, 0.0, 0.0, 0.0, 0.0]
    except (TypeError, ValueError):
        return None


def kline_row_to_binance_shape(row: Sequence[Union[str, float, int]]) -> List[float]:
    """
    Converte uma linha Bybit get_kline (lista de 7 strings) para o formato de lista
    numérica usado pelo restante do código (compatível com colunas Binance).
    """
    t = int(row[0])
    o, h, l, c = float(row[1]), float(row[2]), float(row[3]), float(row[4])
    v = float(row[5])
    qv = float(row[6]) if len(row) > 6 else 0.0
    ct = t + 899_999
    return [t, o, h, l, c, v, ct, qv, 0.0, 0.0, 0.0, 0.0]


def normalize_klines_result(res: Any) -> List[List[float]]:
    out: List[List[float]] = []
    for row in _ret_list(res):
        if isinstance(row, (list, tuple)) and len(row) >= 6:
            out.append(kline_row_to_binance_shape(row))
    out.sort(key=lambda x: int(x[0]))
    return out


def get_usdt_available_balance(client: HTTP) -> float:
    """
    Saldo USDT disponível (UNIFIED → CONTRACT) para UI / Kelly / sync.
    """
    for account_type in ("UNIFIED", "CONTRACT"):
        try:
            res = _call_dict_with_retry(
                lambda at=account_type: client.get_wallet_balance(accountType=at),
                context=f"get_wallet_balance:{account_type}",
            )
        except (FailedRequestError, InvalidRequestError, PermissionError) as e:
            logger.debug("wallet_balance_failed accountType=%s err=%s", account_type, e)
            continue
        for acc in _ret_list(res):
            if not isinstance(acc, dict):
                continue
            total_avail = acc.get("totalAvailableBalance") or acc.get(
                "totalMarginBalance"
            )
            if total_avail is not None:
                try:
                    return float(total_avail)
                except (TypeError, ValueError):
                    pass
            for coin in acc.get("coin") or []:
                if not isinstance(coin, dict):
                    continue
                if str(coin.get("coin", "")).upper() != "USDT":
                    continue
                for key in (
                    "availableToWithdraw",
                    "availableBalance",
                    "walletBalance",
                    "transferBalance",
                ):
                    if coin.get(key) is not None:
                        try:
                            return float(coin[key])
                        except (TypeError, ValueError):
                            pass
    return 0.0


def get_total_equity_usdt(client: HTTP) -> float:
    """Equity total em USDT (fallback para carregar_saldo quando preferir equity)."""
    try:
        res = _call_dict_with_retry(
            lambda: client.get_wallet_balance(accountType="UNIFIED"),
            context="get_wallet_balance:UNIFIED",
        )
    except Exception:
        return 0.0
    for acc in _ret_list(res):
        if isinstance(acc, dict) and acc.get("totalEquity") is not None:
            try:
                return float(acc["totalEquity"])
            except (TypeError, ValueError):
                pass
    return get_usdt_available_balance(client)


def fetch_all_linear_instruments(client: HTTP) -> List[Dict[str, Any]]:
    """Pagina get_instruments_info até esgotar nextPageCursor."""
    all_rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"category": CATEGORY_LINEAR, "limit": 1000}
        if cursor:
            kwargs["cursor"] = cursor
        try:
            res = _call_dict_with_retry(
                lambda: client.get_instruments_info(**kwargs),
                context="get_instruments_info",
            )
        except Exception:
            break
        if not isinstance(res, dict) or res.get("retCode") != 0:
            break
        r = res.get("result") or {}
        lst = r.get("list") if isinstance(r, dict) else None
        if isinstance(lst, list):
            all_rows.extend([x for x in lst if isinstance(x, dict)])
        cursor = r.get("nextPageCursor") if isinstance(r, dict) else None
        if not cursor:
            break
    return all_rows


def build_exchange_info_from_instruments(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Estrutura compatível com o cache Binance: { "symbols": [ { "symbol", "filters" } ] }.
    """
    symbols = []
    for inst in rows:
        sym = inst.get("symbol")
        if not sym:
            continue
        lf = inst.get("lotSizeFilter") or {}
        pf = inst.get("priceFilter") or {}
        qty_step = str(lf.get("qtyStep") or "0.001")
        tick = str(pf.get("tickSize") or "0.01")
        min_order_qty = str(lf.get("minOrderQty") or qty_step)
        # minNotionalValue em USDT (linear) — ordem abaixo disso é rejeitada
        min_nv = lf.get("minNotionalValue")
        if min_nv is None or str(min_nv).strip() == "":
            min_nv = "5"
        symbols.append(
            {
                "symbol": sym,
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": qty_step},
                    {"filterType": "PRICE_FILTER", "tickSize": tick},
                    {"filterType": "MIN_QTY", "minQty": min_order_qty},
                    {"filterType": "MIN_NOTIONAL", "minNotional": str(min_nv)},
                ],
            }
        )
    return {"symbols": symbols}


def linear_tickers_24h(client: HTTP) -> List[Dict[str, Any]]:
    """Lista de dicts {symbol, quoteVolume} para ranking Top 100."""
    try:
        res = _call_dict_with_retry(
            lambda: client.get_tickers(category=CATEGORY_LINEAR),
            context="get_tickers:24h",
        )
    except Exception:
        return []
    out = []
    for t in _ret_list(res):
        if not isinstance(t, dict):
            continue
        sym = t.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        qv = t.get("turnover24h") or t.get("volume24h") or "0"
        try:
            vol = float(qv)
        except (TypeError, ValueError):
            vol = 0.0
        out.append({"symbol": sym, "quoteVolume": vol})
    return out


def get_linear_ticker_row(client: HTTP, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        res = _call_dict_with_retry(
            lambda: client.get_tickers(category=CATEGORY_LINEAR, symbol=symbol),
            context=f"get_tickers:{symbol}",
        )
        lst = _ret_list(res)
        if lst and isinstance(lst[0], dict):
            return lst[0]
        return None
    except Exception:
        return {}


def get_mark_price_and_funding(client: HTTP, symbol: str) -> Dict[str, float]:
    row = get_linear_ticker_row(client, symbol)
    if not row:
        return {"markPrice": 0.0, "lastFundingRate": 0.0, "openInterest": 0.0}
    try:
        mp = float(row.get("markPrice") or row.get("lastPrice") or 0.0)
    except (TypeError, ValueError):
        mp = 0.0
    try:
        fr = float(row.get("fundingRate") or 0.0)
    except (TypeError, ValueError):
        fr = 0.0
    try:
        oi = float(row.get("openInterest") or 0.0)
    except (TypeError, ValueError):
        oi = 0.0
    return {"markPrice": mp, "lastFundingRate": fr, "openInterest": oi}


def get_open_interest_now(client: HTTP, symbol: str) -> float:
    try:
        res = _call_dict_with_retry(
            lambda: client.get_open_interest(
                category=CATEGORY_LINEAR, symbol=symbol, intervalTime="5min", limit=1
            ),
            context=f"get_open_interest:{symbol}",
        )
    except Exception:
        return 0.0
    lst = _ret_list(res)
    if not lst or not isinstance(lst[0], dict):
        return 0.0
    try:
        return float(lst[0].get("openInterest") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def get_oi_funding_snapshots_batch(
    client: HTTP,
    symbols: Sequence[str],
    pause_sec: float = 0.05,
) -> Dict[str, Dict[str, float]]:
    """Mark price, funding e OI (ticker) por símbolo — pausa curta entre chamadas para rate limit."""
    out: Dict[str, Dict[str, float]] = {}
    for sym in symbols:
        try:
            out[str(sym)] = get_mark_price_and_funding(client, sym)
        except Exception:
            out[str(sym)] = {
                "markPrice": 0.0,
                "lastFundingRate": 0.0,
                "openInterest": 0.0,
            }
        time.sleep(max(0.0, float(pause_sec)))
    return out


def fetch_klines_historical_bulk(
    client: HTTP,
    symbol: str,
    interval: str = "15",
    months_back: int = 12,
) -> List[List[float]]:
    """
    Baixa velas para trás em blocos de até BYBIT_KLINE_MAX_LIMIT (1000), respeitando
    get_kline_throttled (BYBIT_KLINE_INTERVAL_SEC + circuit 10006).

    O número máximo de páginas deriva do horizonte `months_back` e do intervalo,
    para ~1 ano (12×30d) não estourar o contador de segurança prematuramente.
    """
    end_ms = int(time.time() * 1000)
    range_ms = int(max(1, months_back) * 30.437 * 24 * 3600 * 1000)
    start_floor = end_ms - range_ms
    rows_raw: List[List[str]] = []
    cur_end = end_ms
    safety = kline_max_pages_for_range_ms(range_ms, interval)
    empty_streak = 0
    while cur_end > start_floor and safety > 0:
        safety -= 1
        try:
            res = get_kline_throttled(
                client,
                category=CATEGORY_LINEAR,
                symbol=symbol,
                interval=interval,
                end=cur_end,
                limit=BYBIT_KLINE_MAX_LIMIT,
            )
        except Exception as e:
            logger.exception(
                "fetch_klines_historical_bulk_fatal | sym=%s | iv=%s | end=%s | err=%s",
                symbol,
                interval,
                cur_end,
                e,
            )
            break
        lst = _ret_list(res)
        if not lst:
            empty_streak += 1
            if empty_streak >= 6:
                break
            time.sleep(min(2.0 * (2 ** (empty_streak - 1)), 30.0))
            continue
        empty_streak = 0
        parsed = [x for x in lst if isinstance(x, (list, tuple)) and len(x) >= 6]
        if not parsed:
            empty_streak += 1
            if empty_streak >= 6:
                break
            time.sleep(0.25)
            continue
        times = [int(x[0]) for x in parsed]
        oldest = min(times)
        for x in parsed:
            if int(x[0]) >= start_floor:
                rows_raw.append([str(a) for a in x])
        if oldest <= start_floor:
            break
        cur_end = oldest - 1
        if len(parsed) < BYBIT_KLINE_MAX_LIMIT:
            break
    uniq: Dict[int, List[str]] = {}
    for row in rows_raw:
        uniq[int(row[0])] = row
    sorted_rows = sorted(uniq.values(), key=lambda r: int(r[0]))
    return [kline_row_to_binance_shape(r) for r in sorted_rows]


def fetch_klines_historical_mtf(
    client: HTTP,
    symbol: str,
    intervals: Optional[Sequence[str]] = None,
    months_back: int = 12,
) -> Dict[str, List[List[float]]]:
    """
    Roteamento histórico MTF blindado: um intervalo de cada vez (15 → 60 → 240),
    reutilizando throttling global — evita 10006 e não paraleliza rajadas.
    """
    seq = tuple(intervals) if intervals is not None else MTF_KLINE_INTERVALS_DEFAULT
    out: Dict[str, List[List[float]]] = {}
    for idx, iv in enumerate(seq):
        ivs = str(iv).strip()
        if idx > 0:
            time.sleep(0.05)
        out[ivs] = fetch_klines_historical_bulk(
            client, symbol, interval=ivs, months_back=months_back
        )
    return out


def fetch_klines_since_ms(
    client: HTTP,
    symbol: str,
    start_ms: int,
    interval: str = "15",
) -> List[List[float]]:
    """Velas com startTime > start_ms até agora."""
    end_ms = int(time.time() * 1000)
    if start_ms >= end_ms:
        return []
    rows_raw: List[List[str]] = []
    cur_end = end_ms
    range_ms = max(0, end_ms - start_ms)
    safety = kline_max_pages_for_range_ms(range_ms, interval)
    empty_streak = 0
    while cur_end > start_ms and safety > 0:
        safety -= 1
        try:
            res = get_kline_throttled(
                client,
                category=CATEGORY_LINEAR,
                symbol=symbol,
                interval=interval,
                end=cur_end,
                limit=BYBIT_KLINE_MAX_LIMIT,
            )
        except Exception as e:
            logger.exception(
                "fetch_klines_since_ms_fatal | sym=%s | iv=%s | err=%s",
                symbol,
                interval,
                e,
            )
            break
        lst = _ret_list(res)
        if not lst:
            empty_streak += 1
            if empty_streak >= 6:
                break
            time.sleep(min(2.0 * (2 ** (empty_streak - 1)), 30.0))
            continue
        empty_streak = 0
        parsed = [x for x in lst if isinstance(x, (list, tuple)) and len(x) >= 6]
        if not parsed:
            empty_streak += 1
            if empty_streak >= 6:
                break
            time.sleep(0.25)
            continue
        times = [int(x[0]) for x in parsed]
        oldest = min(times)
        for x in parsed:
            if int(x[0]) > start_ms:
                rows_raw.append([str(a) for a in x])
        if oldest <= start_ms:
            break
        cur_end = oldest - 1
        if len(parsed) < BYBIT_KLINE_MAX_LIMIT:
            break
    uniq: Dict[int, List[str]] = {}
    for row in rows_raw:
        uniq[int(row[0])] = row
    sorted_rows = sorted(uniq.values(), key=lambda r: int(r[0]))
    return [kline_row_to_binance_shape(r) for r in sorted_rows]


def bybit_side_from_binance(side: str) -> str:
    s = (side or "").upper()
    if s == "BUY":
        return "Buy"
    if s == "SELL":
        return "Sell"
    return "Buy"


def format_bybit_exception(e: BaseException) -> str:
    from lhn_log_sanitize import sanitize_log_message

    if isinstance(e, (FailedRequestError, InvalidRequestError)):
        msg = getattr(e, "message", None) or str(e)
        code = getattr(e, "status_code", None)
        base = f"{msg}"
        if code is not None:
            base = f"[{code}] {base}"
        low = base.lower()
        if "margin" in low or "1100" in str(code):
            base += " (margem / risco — verifique alavancagem e saldo na conta UNIFIED)"
        if (
            "balance" in low
            or "insufficient" in low
            or str(code) in ("170131", "110007", "10002")
        ):
            base += " (saldo ou disponível insuficiente)"
        return sanitize_log_message(base)
    return sanitize_log_message(str(e))


def ws_public_linear_url() -> str:
    return "wss://stream.bybit.com/v5/public/linear"


def ws_public_linear_testnet_url() -> str:
    return "wss://stream-testnet.bybit.com/v5/public/linear"
