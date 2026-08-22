import asyncio
import os
import signal
from unittest.mock import AsyncMock

import pytest

from aleph.toolkit.lifecycle import (
    closing_quietly,
    install_signal_handlers,
    safe_async_cleanup,
)


@pytest.mark.asyncio
async def test_install_signal_handlers_invokes_callback_on_sigterm():
    loop = asyncio.get_running_loop()
    called = asyncio.Event()
    install_signal_handlers(loop, called.set)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(called.wait(), timeout=1.0)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        loop.remove_signal_handler(signal.SIGINT)


@pytest.mark.asyncio
async def test_safe_async_cleanup_swallows_exceptions(caplog):
    async def boom():
        raise RuntimeError("boom")

    await safe_async_cleanup("test resource", boom())  # must not raise

    assert "test resource" in caplog.text
    assert "boom" in caplog.text


@pytest.mark.asyncio
async def test_safe_async_cleanup_runs_coroutine_to_completion():
    completed = False

    async def slow():
        nonlocal completed
        await asyncio.sleep(0)
        completed = True

    await safe_async_cleanup("slow", slow())
    assert completed


@pytest.mark.asyncio
async def test_closing_quietly_closes_on_exit():
    closeable = AsyncMock()
    async with closing_quietly("resource", closeable) as yielded:
        assert yielded is closeable
        closeable.close.assert_not_called()
    closeable.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_closing_quietly_swallows_close_errors(caplog):
    closeable = AsyncMock()
    closeable.close.side_effect = RuntimeError("broker down")

    # A close() failure on exit must not propagate (would mask the real cause).
    async with closing_quietly("resource", closeable):
        pass

    assert "resource" in caplog.text
