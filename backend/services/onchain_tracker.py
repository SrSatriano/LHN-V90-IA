"""
Túnel 6 — On-Chain Radar (alternative data, read-only).

Polling assíncrono (aiohttp) a cada 5 min num endpoint mock (CoinGecko ping).
Estado global simulado para evolução futura (CryptoQuant / Glassnode / WhaleAlert).
"""

from __future__ import annotations

import asyncio
import logging
import random
import threading
from typing import Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)

MOCK_PING_URL = "https://api.coingecko.com/api/v3/ping"
POLL_INTERVAL_SEC = 300

# --- Estado global exportável (0 Normal, 1 Atenção, 2 Perigo — mock só usa 0/1 por ora) ---
ONCHAIN_WHALE_ALERT_LEVEL: int = 0
_ONCHAIN_LOCK = threading.Lock()


def get_onchain_whale_alert_level() -> int:
    with _ONCHAIN_LOCK:
        return int(ONCHAIN_WHALE_ALERT_LEVEL)


def _set_alert_level(v: int) -> None:
    global ONCHAIN_WHALE_ALERT_LEVEL
    with _ONCHAIN_LOCK:
        ONCHAIN_WHALE_ALERT_LEVEL = max(0, min(2, int(v)))


def _mock_level_after_poll() -> None:
    """Simula baleia: maior peso em normal; infraestrutura de níveis 0–2."""
    lvl = random.choice([0, 0, 0, 1])
    _set_alert_level(lvl)


async def run_onchain_tracker_forever(
    should_run: Optional[Callable[[], bool]] = None,
) -> None:
    """
    Loop principal: GET assíncrono (não bloqueante), throttle 5 min, erros engolidos.
    """
    sr = should_run or (lambda: True)
    timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)
    headers = {"Accept": "application/json", "User-Agent": "LHN-Sovereign-OnChainRadar/1.0"}

    logger.info("onchain_tracker_tunnel6_start interval=%ss url=%s", POLL_INTERVAL_SEC, MOCK_PING_URL)

    while sr():
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(MOCK_PING_URL) as resp:
                    await resp.read()
            _mock_level_after_poll()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("onchain_tracker_poll_skipped", exc_info=True)
        for _ in range(POLL_INTERVAL_SEC):
            if not sr():
                return
            await asyncio.sleep(1.0)
