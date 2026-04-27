"""Encapsula verificações de risco operacional (delega ao bot)."""

import time


class RiskManager:
    """Fachada fina sobre CoreMixin/EngineMixin para evolução futura sem alterar estratégia."""

    __slots__ = ("_bot",)

    def __init__(self, bot):
        self._bot = bot

    def ws_feed_blocks_new_orders(self) -> bool:
        """True se o feed WS estiver considerado offline (kill switch)."""
        bot = self._bot
        if not bot.cfg.get("use_ws", True):
            return False
        last = float(getattr(bot, "_ws_price_last_tick_ts", 0) or 0)
        stale_sec = float(bot.cfg.get("ws_feed_stale_sec", 30))
        if last <= 0 or (time.time() - last) > stale_sec:
            return True
        return False

    def notional_exceeds_cap(self, notional_usd: float) -> bool:
        cap = float(self._bot.cfg.get("max_order_usd", 500_000.0))
        return notional_usd > cap
