"""
Liveness heartbeats for long-running job loops.

Each job loop periodically writes the current time to a file named after the job.
The main process (see aleph.jobs.watchdog) and the `pyaleph-healthcheck` command
read these files to detect loops that are still alive but no longer make progress.

This module must stay cheap to import: the healthcheck command imports it.
"""

import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger(__name__)

# Heartbeat names match the process titles of the job subprocesses, so that the
# file, the process list and the logs all use the same name.
FETCH_MESSAGES_JOB = "aleph.jobs.fetch_messages"
PROCESS_MESSAGES_JOB = "aleph.jobs.messages_task_loop"
PROCESS_TXS_JOB = "aleph.jobs.txs_task_loop"
SUBPROCESS_JOB_NAMES = (FETCH_MESSAGES_JOB, PROCESS_MESSAGES_JOB, PROCESS_TXS_JOB)

# Beats closer together than this are skipped, so loops can beat once per item.
MIN_BEAT_INTERVAL_S = 1.0


class Heartbeat:
    def __init__(self, name: str, directory: Path):
        self.name = name
        self.directory = Path(directory)
        self._last_beat: Optional[float] = None

    @property
    def path(self) -> Path:
        return self.directory / self.name

    def beat(self) -> None:
        now = time.monotonic()
        if self._last_beat is not None and now - self._last_beat < MIN_BEAT_INTERVAL_S:
            return
        # Set before writing so a failing disk is not retried (and logged) per item.
        self._last_beat = now

        tmp_path = None
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(dir=self.directory, prefix=f".{self.name}.")
            with os.fdopen(fd, "w") as f:
                f.write(repr(time.time()))
            # Readers must never see a partially written file.
            os.replace(tmp_path, self.path)
            tmp_path = None
        except OSError:
            # A heartbeat that cannot be written must not take the job down;
            # the watchdog will report it as stale if this persists.
            LOGGER.warning("Could not write heartbeat %s", self.path, exc_info=True)
        finally:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


def read_age(
    name: str, directory: Path, now: Optional[float] = None
) -> Optional[float]:
    """Seconds since the last beat of `name`, or None if there is no readable beat."""
    try:
        last_beat = float((Path(directory) / name).read_text())
    except (OSError, ValueError):
        return None

    if now is None:
        now = time.time()
    return max(0.0, now - last_beat)
