import asyncio
import time
from typing import List

import pytest
from configmanager import Config

import aleph.config
from aleph.toolkit.loop_watchdog import (
    EXIT_CODE,
    LoopWatchdog,
    _exit_on_stall,
    start_loop_watchdog,
)


@pytest.mark.asyncio
async def test_ticking_loop_is_not_a_stall():
    calls: List[float] = []
    watchdog = LoopWatchdog(
        stall_timeout_s=0.3, tick_interval_s=0.05, on_stall=calls.append
    )
    watchdog.start()
    try:
        await asyncio.sleep(0.7)
    finally:
        watchdog.stop()
    assert calls == []
    assert not watchdog.is_running


@pytest.mark.asyncio
async def test_blocked_loop_triggers_on_stall_once():
    calls: List[float] = []
    watchdog = LoopWatchdog(
        stall_timeout_s=0.2, tick_interval_s=0.05, on_stall=calls.append
    )
    watchdog.start()
    try:
        await asyncio.sleep(0)
        # A synchronous call that blocks the event loop, like a libpq connect
        # with no timeout.
        time.sleep(0.6)
        await asyncio.sleep(0.1)
    finally:
        watchdog.stop()
    assert len(calls) == 1
    assert calls[0] >= 0.2


@pytest.mark.asyncio
async def test_stop_ends_the_thread_and_the_ticker():
    watchdog = LoopWatchdog(
        stall_timeout_s=1.0, tick_interval_s=0.05, on_stall=lambda _: None
    )
    watchdog.start()
    assert watchdog.is_running
    watchdog.stop()
    assert not watchdog.is_running
    # Idempotent.
    watchdog.stop()


def test_exit_on_stall_dumps_all_threads_and_exits(monkeypatch):
    recorded = {}
    monkeypatch.setattr(
        "aleph.toolkit.loop_watchdog.faulthandler.dump_traceback",
        lambda file=None, all_threads=True: recorded.update(all_threads=all_threads),
    )
    monkeypatch.setattr(
        "aleph.toolkit.loop_watchdog.os._exit",
        lambda code: recorded.update(code=code),
    )
    _exit_on_stall(12.5)
    assert recorded == {"all_threads": True, "code": EXIT_CODE}
    assert EXIT_CODE == 2


def test_start_loop_watchdog_is_disabled_by_zero():
    config = Config(aleph.config.get_defaults())
    config.aleph.watchdog.stall_timeout_s.value = 0
    assert start_loop_watchdog(config) is None


@pytest.mark.asyncio
async def test_start_loop_watchdog_uses_the_config_default():
    config = Config(aleph.config.get_defaults())
    watchdog = start_loop_watchdog(config)
    assert watchdog is not None
    try:
        assert watchdog.stall_timeout_s == 300
        assert watchdog.is_running
    finally:
        watchdog.stop()


def test_blocked_process_exits_with_status_2_and_dumps_the_stack():
    import subprocess
    import sys

    program = """
import asyncio, time
from aleph.toolkit.loop_watchdog import LoopWatchdog

async def main():
    LoopWatchdog(stall_timeout_s=0.3, tick_interval_s=0.05).start()
    await asyncio.sleep(0)
    time.sleep(10)  # the synchronous call that never returns

asyncio.run(main())
"""
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == EXIT_CODE
    assert time.monotonic() - started < 5
    assert "Event loop blocked" in result.stderr
    # faulthandler names the blocking frame (program line 8, the sleep).
    assert "line 8 in main" in result.stderr
