"""
Túnel 5 — Oráculo Binance Futures (bookTicker BTCUSDT).

Somente leitura: escuta `wss://fstream.binance.com/ws/btcusdt@bookTicker`,
calcula momentum de ~1s e expõe estado global (sem ordens, sem Keras).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Optional, Tuple

import websockets

logger = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/ws/btcusdt@bookTicker"

# --- Estado global exportável (ler via get_*; escrita sob lock) ---
BINANCE_LEAD_SIGNAL: int = 0
BINANCE_ORACLE_LAST_BID: float = 0.0
BINANCE_ORACLE_LAST_ASK: float = 0.0
BINANCE_ORACLE_LAST_MID: float = 0.0
_BINANCE_ORACLE_LOCK = threading.Lock()

_MOMENTUM_WINDOW_SEC = 1.0
_MOMENTUM_THRESH_PCT = 0.05
_HISTORY: Deque[Tuple[float, float]] = deque(maxlen=512)


def get_binance_lead_signal() -> int:
    with _BINANCE_ORACLE_LOCK:
        return int(BINANCE_LEAD_SIGNAL)


def get_binance_oracle_top() -> Tuple[float, float, float]:
    """(bid, ask, mid) últimos vistos; 0 se ainda não houver tick."""
    with _BINANCE_ORACLE_LOCK:
        return (
            float(BINANCE_ORACLE_LAST_BID),
            float(BINANCE_ORACLE_LAST_ASK),
            float(BINANCE_ORACLE_LAST_MID),
        )


def _apply_book(bid: float, ask: float) -> None:
    global BINANCE_LEAD_SIGNAL, BINANCE_ORACLE_LAST_BID, BINANCE_ORACLE_LAST_ASK
    global BINANCE_ORACLE_LAST_MID
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
    now = time.monotonic()
    with _BINANCE_ORACLE_LOCK:
        BINANCE_ORACLE_LAST_BID = bid
        BINANCE_ORACLE_LAST_ASK = ask
        BINANCE_ORACLE_LAST_MID = mid
    if mid <= 0:
        return
    _HISTORY.append((now, mid))
    while _HISTORY and now - _HISTORY[0][0] > _MOMENTUM_WINDOW_SEC + 0.25:
        _HISTORY.popleft()
    ref_mid: Optional[float] = None
    for ts, m in _HISTORY:
        if now - ts >= _MOMENTUM_WINDOW_SEC * 0.92:
            ref_mid = m
    if ref_mid is None or ref_mid <= 0:
        return
    pct = (mid - ref_mid) / ref_mid * 100.0
    if pct <= -_MOMENTUM_THRESH_PCT:
        sig = -1
    elif pct >= _MOMENTUM_THRESH_PCT:
        sig = 1
    else:
        sig = 0
    with _BINANCE_ORACLE_LOCK:
        BINANCE_LEAD_SIGNAL = int(sig)


def _parse_book_ticker(raw: str) -> Optional[Tuple[float, float]]:
    d = json.loads(raw) if raw else None
    if not isinstance(d, dict):
        return None
    b = d.get("b")
    a = d.get("a")
    if b is None or a is None:
        return None
    try:
        return float(b), float(a)
    except (TypeError, ValueError):
        return None


async def run_binance_oracle_forever(
    should_run: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Loop principal: liga ao WS Binance, reconecta com backoff exponencial.
    `should_run` (opcional) — ex.: lambda: self.is_app_alive; default sempre True.
    """
    sr = should_run or (lambda: True)
    backoff = 1.0
    cap = 60.0
    ws_kw: dict[str, Any] = {
        "ping_interval": 20,
        "ping_timeout": 120,
        "close_timeout": 10,
        "open_timeout": 30,
    }
    try:
        ws_kw["family"] = socket.AF_INET
    except Exception:
        pass

    logger.info("binance_oracle_tunnel5_start url=%s", WS_URL)

    while sr():
        try:
            async with websockets.connect(WS_URL, **ws_kw) as ws:
                backoff = 1.0
                async for message in ws:
                    if not sr():
                        return
                    if isinstance(message, (bytes, bytearray)):
                        message = message.decode("utf-8", errors="ignore")
                    if not isinstance(message, str):
                        continue
                    try:
                        ba = _parse_book_ticker(message)
                        if ba is not None:
                            _apply_book(ba[0], ba[1])
                    except Exception:
                        logger.debug("binance_oracle_tick_parse_fail", exc_info=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                "binance_oracle_ws_disconnect backoff=%.1fs err=%s",
                backoff,
                e,
                exc_info=False,
            )
            await asyncio.sleep(backoff)
            backoff = min(cap, max(1.0, backoff * 2.0))
