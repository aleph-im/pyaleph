import asyncio
import multiprocessing as mp
import os
import signal
import time

import pytest

from aleph.jobs import JobsRunner


def _graceful_child(ready):
    """Subprocess that exits cleanly on SIGTERM."""

    def _handler(_signum, _frame):
        os._exit(0)

    signal.signal(signal.SIGTERM, _handler)
    ready.set()
    while True:
        time.sleep(0.1)


def _stubborn_child(ready):
    """Subprocess that ignores SIGTERM, forcing the parent to kill it."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ready.set()
    while True:
        time.sleep(0.1)


@pytest.mark.asyncio
async def test_stop_terminates_cooperative_processes():
    ctx = mp.get_context("fork")
    ready = ctx.Event()
    proc = ctx.Process(target=_graceful_child, args=(ready,))
    proc.start()
    try:
        assert ready.wait(timeout=5.0), "child did not signal ready"
        runner = JobsRunner(processes=[proc], tasks=[])
        await runner.stop(terminate_timeout=2.0)
        assert not proc.is_alive()
        assert proc.exitcode == 0
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join()


@pytest.mark.asyncio
async def test_stop_kills_stubborn_processes_after_timeout():
    ctx = mp.get_context("fork")
    ready = ctx.Event()
    proc = ctx.Process(target=_stubborn_child, args=(ready,))
    proc.start()
    try:
        assert ready.wait(timeout=5.0), "child did not signal ready"
        runner = JobsRunner(processes=[proc], tasks=[])
        start = asyncio.get_running_loop().time()
        await runner.stop(terminate_timeout=0.5)
        elapsed = asyncio.get_running_loop().time() - start
        assert not proc.is_alive()
        assert proc.exitcode == -signal.SIGKILL
        assert elapsed < 3.0
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join()


@pytest.mark.asyncio
async def test_stop_is_safe_with_empty_runner():
    await JobsRunner(processes=[], tasks=[]).stop(terminate_timeout=0.5)
