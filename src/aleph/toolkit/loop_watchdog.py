"""Detect an event loop blocked by a synchronous call and end the process.

pyaleph's main process and job subprocesses run asyncio loops that also make
synchronous calls (psycopg2 through SQLAlchemy, for instance). A synchronous
call that never returns, such as a libpq connect with no timeout to a
half-open connection, blocks the loop: the process stays alive, nothing is
logged, and no task in that process runs again. A heartbeat task on the same
loop cannot notice it, so the watchdog lives in a plain thread.

The loop refreshes a timestamp every ``tick_interval_s`` from an asyncio task;
the thread checks the timestamp's age. Past ``stall_timeout_s`` it dumps every
thread's stack to stderr (the blocking call is in the main thread's trace),
logs at CRITICAL and exits the process with status 2, so the container
orchestrator or supervisor restarts it.
"""

import asyncio
import faulthandler
import logging
import os
import sys
import threading
import time
from typing import Callable, Optional

from configmanager import Config

LOGGER = logging.getLogger(__name__)

EXIT_CODE = 2


def _exit_on_stall(age: float) -> None:
    LOGGER.critical(
        "Event loop blocked for %.1f s (synchronous call that never returned?), "
        "dumping thread stacks and exiting with status %d",
        age,
        EXIT_CODE,
    )
    for handler in logging.getLogger().handlers:
        handler.flush()
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
    # Not sys.exit: the main thread is stuck, SystemExit raised here would only
    # end the watchdog thread.
    os._exit(EXIT_CODE)


class LoopWatchdog:
    def __init__(
        self,
        stall_timeout_s: float,
        tick_interval_s: float = 1.0,
        on_stall: Callable[[float], None] = _exit_on_stall,
    ):
        if stall_timeout_s <= 0:
            raise ValueError("stall_timeout_s must be positive")
        self.stall_timeout_s = stall_timeout_s
        self.tick_interval_s = tick_interval_s
        self.on_stall = on_stall
        self._last_tick = time.monotonic()
        self._stopping = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ticker: Optional[asyncio.Task] = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Start watching the running event loop. Call from inside the loop."""
        if self._thread is not None:
            raise RuntimeError("watchdog already started")
        self._last_tick = time.monotonic()
        self._stopping.clear()
        self._ticker = asyncio.get_running_loop().create_task(
            self._tick(), name="loop-watchdog-tick"
        )
        self._thread = threading.Thread(
            target=self._watch, name="loop-watchdog", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._ticker is not None:
            self._ticker.cancel()
            self._ticker = None
        if self._thread is not None:
            if self._thread is not threading.current_thread():
                self._thread.join()
            self._thread = None

    async def _tick(self) -> None:
        while True:
            self._last_tick = time.monotonic()
            await asyncio.sleep(self.tick_interval_s)

    def _watch(self) -> None:
        while not self._stopping.wait(self.tick_interval_s):
            age = time.monotonic() - self._last_tick
            if age > self.stall_timeout_s:
                self.on_stall(age)
                return


def start_loop_watchdog(config: Config) -> Optional[LoopWatchdog]:
    """Start the watchdog configured in ``aleph.watchdog``; None when disabled."""
    stall_timeout_s = config.aleph.watchdog.stall_timeout_s.value
    if not stall_timeout_s:
        return None
    watchdog = LoopWatchdog(stall_timeout_s=stall_timeout_s)
    watchdog.start()
    return watchdog
