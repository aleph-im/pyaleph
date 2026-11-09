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


def _dying_child(ready, exitcode):
    ready.set()
    os._exit(exitcode)


@pytest.mark.asyncio
async def test_wait_for_child_exit_raises_when_a_job_dies():
    from aleph.jobs import JobDiedError

    ctx = mp.get_context("fork")
    ready = ctx.Event()
    proc = ctx.Process(target=_dying_child, args=(ready, 3), name="dying-job")
    proc.start()
    runner = JobsRunner(processes=[proc], tasks=[])
    try:
        with pytest.raises(JobDiedError) as excinfo:
            await asyncio.wait_for(runner.wait_for_child_exit(), timeout=5.0)
    finally:
        await runner.stop()
    assert excinfo.value.name == "dying-job"
    assert excinfo.value.exitcode == 3
    assert "dying-job" in str(excinfo.value)
    assert "3" in str(excinfo.value)


@pytest.mark.asyncio
async def test_wait_for_child_exit_ignores_children_stopped_after_cancel():
    ctx = mp.get_context("fork")
    ready = ctx.Event()
    proc = ctx.Process(target=_graceful_child, args=(ready,), name="live-job")
    proc.start()
    runner = JobsRunner(processes=[proc], tasks=[])
    try:
        assert ready.wait(timeout=5.0), "child did not signal ready"
        waiter = asyncio.create_task(runner.wait_for_child_exit())
        await asyncio.sleep(0.1)
        assert not waiter.done()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        # The sentinel is no longer watched: stopping the child is not a death.
        assert asyncio.get_running_loop().remove_reader(proc.sentinel) is False
    finally:
        await runner.stop()
    assert not proc.is_alive()
