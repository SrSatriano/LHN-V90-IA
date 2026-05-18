"""
Regression guard: asyncio.run_coroutine_threadsafe returns a Future whose
errors only surface after result().

The real-account HFT path must await that Future; otherwise the caller can
return success while the exchange call failed (ghost positions).
"""

import asyncio
import threading

import pytest


def test_threadsafe_future_propagates_coroutine_exception():
    loop = asyncio.new_event_loop()
    started = threading.Event()

    def _run_loop():
        asyncio.set_event_loop(loop)
        started.set()
        loop.run_forever()

    th = threading.Thread(target=_run_loop, daemon=True)
    th.start()
    assert started.wait(timeout=5.0)

    async def _failing_place():
        raise RuntimeError("simulated exchange rejection")

    fut = asyncio.run_coroutine_threadsafe(_failing_place(), loop)
    with pytest.raises(RuntimeError, match="simulated exchange rejection"):
        fut.result(timeout=5.0)

    loop.call_soon_threadsafe(loop.stop)
    th.join(timeout=5.0)
