import asyncio
import logging
from dataclasses import dataclass, field
from multiprocessing import Process
from pathlib import Path
from typing import Coroutine, List

from aleph.jobs.fetch_pending_messages import fetch_pending_messages_subprocess
from aleph.jobs.process_pending_messages import (
    fetch_and_process_messages_task,
    pending_messages_subprocess,
)
from aleph.jobs.process_pending_txs import handle_txs_task, pending_txs_subprocess
from aleph.jobs.reconnect_ipfs import reconnect_ipfs_job
from aleph.services.ipfs import IpfsService
from aleph.toolkit.heartbeat import (
    FETCH_MESSAGES_JOB,
    PROCESS_MESSAGES_JOB,
    PROCESS_TXS_JOB,
)
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger("jobs")


@dataclass
class JobsRunner:
    processes: List[Process] = field(default_factory=list)
    tasks: List[Coroutine] = field(default_factory=list)
    # Heartbeat name of each entry of `processes`, in the same order.
    job_names: List[str] = field(default_factory=list)

    async def stop(self, terminate_timeout: float = 10.0) -> None:
        """Send SIGTERM to every child process, then join with timeout.

        Joins all processes before returning so they are fully reaped by the OS.
        Falls back to SIGKILL for any process that does not exit within
        `terminate_timeout` seconds. Children are joined in parallel so total
        wall time is bounded by `terminate_timeout + 1.0` regardless of child
        count.
        """
        alive = [p for p in self.processes if p.is_alive()]
        if not alive:
            return

        for p in alive:
            LOGGER.info("Terminating subprocess %s (pid=%s)", p.name, p.pid)
            p.terminate()

        loop = asyncio.get_running_loop()
        await asyncio.gather(
            *(loop.run_in_executor(None, p.join, terminate_timeout) for p in alive)
        )

        still_alive = [p for p in alive if p.is_alive()]
        for p in still_alive:
            LOGGER.warning(
                "Subprocess %s (pid=%s) did not exit within %.1fs, killing",
                p.name,
                p.pid,
                terminate_timeout,
            )
            p.kill()

        if still_alive:
            await asyncio.gather(
                *(loop.run_in_executor(None, p.join, 1.0) for p in still_alive)
            )


def start_jobs(
    config,
    session_factory: DbSessionFactory,
    ipfs_service: IpfsService,
    use_processes: bool = True,
) -> JobsRunner:
    LOGGER.info("starting jobs")
    runner = JobsRunner()

    if use_processes:
        config_values = config.dump_values()
        heartbeat_directory = Path(config.aleph.jobs.heartbeat.directory.value)
        for target, job_name in (
            (fetch_pending_messages_subprocess, FETCH_MESSAGES_JOB),
            (pending_messages_subprocess, PROCESS_MESSAGES_JOB),
            (pending_txs_subprocess, PROCESS_TXS_JOB),
        ):
            # A heartbeat left by a previous run would make a job that never
            # starts look alive to `pyaleph-healthcheck`.
            (heartbeat_directory / job_name).unlink(missing_ok=True)
            p = Process(target=target, args=(config_values,), name=target.__name__)
            p.start()
            runner.processes.append(p)
            runner.job_names.append(job_name)
    else:
        runner.tasks.append(fetch_and_process_messages_task(config=config))
        runner.tasks.append(handle_txs_task(config))

    if config.ipfs.enabled.value:
        runner.tasks.append(
            reconnect_ipfs_job(
                config=config,
                session_factory=session_factory,
                ipfs_service=ipfs_service,
            )
        )

    return runner
