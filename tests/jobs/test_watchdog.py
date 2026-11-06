import asyncio
import multiprocessing as mp
import time
from pathlib import Path

import pytest

from aleph.jobs.watchdog import JobDiedError, JobStalledError, watch_jobs
from aleph.toolkit.heartbeat import Heartbeat


def _sleeping_child():
    time.sleep(30)


def _exiting_child():
    pass


@pytest.fixture
def alive_process():
    proc = mp.get_context("fork").Process(target=_sleeping_child)
    proc.start()
    yield proc
    proc.kill()
    proc.join()


@pytest.mark.asyncio
async def test_dead_process_raises(tmp_path: Path):
    proc = mp.get_context("fork").Process(target=_exiting_child)
    proc.start()
    proc.join()
    Heartbeat("job", tmp_path).beat()

    with pytest.raises(JobDiedError) as exc_info:
        await asyncio.wait_for(
            watch_jobs([proc], ["job"], tmp_path, max_age_s=5, interval_s=0.05),
            timeout=1,
        )
    assert exc_info.value.name == "job"


@pytest.mark.asyncio
async def test_stale_heartbeat_raises(tmp_path: Path, alive_process):
    # Beats once and never again, like a loop stuck in a blocking call.
    Heartbeat("job", tmp_path).beat()

    with pytest.raises(JobStalledError) as exc_info:
        await asyncio.wait_for(
            watch_jobs(
                [alive_process], ["job"], tmp_path, max_age_s=0.2, interval_s=0.05
            ),
            timeout=1,
        )
    assert exc_info.value.name == "job"
    assert exc_info.value.age > 0.2


@pytest.mark.asyncio
async def test_fresh_heartbeat_does_not_raise(tmp_path: Path, alive_process):
    async def keep_beating():
        while True:
            (tmp_path / "job").write_text(str(time.time()))
            await asyncio.sleep(0.02)

    beater = asyncio.create_task(keep_beating())
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                watch_jobs(
                    [alive_process], ["job"], tmp_path, max_age_s=0.2, interval_s=0.05
                ),
                timeout=0.5,
            )
    finally:
        beater.cancel()


@pytest.mark.asyncio
async def test_missing_heartbeat_within_grace_does_not_raise(
    tmp_path: Path, alive_process
):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            watch_jobs(
                [alive_process], ["job"], tmp_path, max_age_s=10, interval_s=0.05
            ),
            timeout=0.3,
        )


@pytest.mark.asyncio
async def test_missing_heartbeat_after_grace_raises(tmp_path: Path, alive_process):
    with pytest.raises(JobStalledError):
        await asyncio.wait_for(
            watch_jobs(
                [alive_process],
                ["job"],
                tmp_path,
                max_age_s=0.2,
                interval_s=0.05,
                started_at=time.time() - 1,
            ),
            timeout=1,
        )


@pytest.mark.asyncio
async def test_disabled_watchdog_returns_immediately(tmp_path: Path):
    proc = mp.get_context("fork").Process(target=_exiting_child)
    proc.start()
    proc.join()

    await asyncio.wait_for(
        watch_jobs([proc], ["job"], tmp_path, max_age_s=0, interval_s=10),
        timeout=0.1,
    )


def test_unhealthy_jobs_exit_with_status_2(monkeypatch):
    import aleph.commands

    async def unhealthy_main(_args):
        raise JobStalledError("job", 412)

    monkeypatch.setattr(aleph.commands, "main", unhealthy_main)
    monkeypatch.setattr("sys.argv", ["pyaleph"])

    with pytest.raises(SystemExit) as exc_info:
        aleph.commands.run()
    assert exc_info.value.code == 2
