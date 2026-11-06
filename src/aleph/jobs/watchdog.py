"""
Watchdog run by the main process over the job subprocesses.

A job subprocess that dies, or whose loop stops beating (for example blocked
forever on a database connection), makes the watchdog raise. The exception
stops the main process with a non-zero status so that the container
orchestrator restarts the whole node.
"""

import asyncio
import logging
import time
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Optional, Sequence

from aleph.toolkit.heartbeat import read_age

LOGGER = logging.getLogger(__name__)


class JobsUnhealthyError(Exception):
    pass


class JobDiedError(JobsUnhealthyError):
    def __init__(self, name: str, exitcode: Optional[int] = None):
        self.name = name
        self.exitcode = exitcode
        super().__init__(f"Job {name} died (exit code {exitcode})")


class JobStalledError(JobsUnhealthyError):
    def __init__(self, name: str, age: float):
        self.name = name
        self.age = age
        super().__init__(f"Job {name} has not beaten for {age:.0f}s")


def check_jobs(
    processes: Sequence[BaseProcess],
    names: Sequence[str],
    directory: Path,
    max_age_s: float,
    started_at: float,
    now: Optional[float] = None,
) -> None:
    if now is None:
        now = time.time()

    for process, name in zip(processes, names):
        if not process.is_alive():
            LOGGER.critical(
                "Job %s (pid=%s) died with exit code %s",
                name,
                process.pid,
                process.exitcode,
            )
            raise JobDiedError(name, process.exitcode)

        age = read_age(name, directory, now=now)
        if age is None:
            # Jobs spend their first seconds waiting for services before the
            # first beat, so a missing file only counts once the grace is over.
            since_start = now - started_at
            if since_start <= max_age_s:
                continue
            LOGGER.critical(
                "Job %s (pid=%s) has not written a heartbeat %.0fs after start",
                name,
                process.pid,
                since_start,
            )
            raise JobStalledError(name, since_start)

        if age > max_age_s:
            LOGGER.critical(
                "Job %s (pid=%s) is stalled: last heartbeat %.0fs ago (max %.0fs)",
                name,
                process.pid,
                age,
                max_age_s,
            )
            raise JobStalledError(name, age)


async def watch_jobs(
    processes: Sequence[BaseProcess],
    names: Sequence[str],
    directory: Path,
    max_age_s: float,
    interval_s: float = 10.0,
    started_at: Optional[float] = None,
) -> None:
    if max_age_s <= 0:
        LOGGER.info("Job watchdog disabled (max_age_s=%s)", max_age_s)
        return

    if started_at is None:
        started_at = time.time()

    while True:
        await asyncio.sleep(interval_s)
        check_jobs(
            processes=processes,
            names=names,
            directory=directory,
            max_age_s=max_age_s,
            started_at=started_at,
        )
