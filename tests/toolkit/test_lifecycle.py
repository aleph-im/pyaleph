import asyncio
import os
import signal

import pytest

from aleph.toolkit.lifecycle import install_signal_handlers, safe_async_cleanup


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
