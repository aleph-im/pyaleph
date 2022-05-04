import logging
from multiprocessing import Process
from typing import Dict, List, Coroutine

from aleph.jobs.garbage_collector import garbage_collector_subprocess
from aleph.jobs.process_pending_messages import (
    pending_messages_subprocess,
    retry_messages_task,
)
from aleph.jobs.process_pending_txs import pending_txs_subprocess, handle_txs_task
from aleph.jobs.reconnect_ipfs import reconnect_ipfs_job

LOGGER = logging.getLogger("jobs")


def start_jobs(
    config,
    shared_stats: Dict,
    api_servers: List[str],
    use_processes=True,
) -> List[Coroutine]:
    LOGGER.info("starting jobs")
    tasks: List[Coroutine] = []

    if use_processes:
        config_values = config.dump_values()
        pending_messages_job = Process(
            target=pending_messages_subprocess,
            args=(
                config_values,
                shared_stats,
                api_servers,
            ),
        )
        pending_txs_job = Process(
            target=pending_txs_subprocess,
            args=(config_values, api_servers),
        )

        garbage_collector_job = Process(
            target=garbage_collector_subprocess, args=(config_values,)
        )
        pending_messages_job.start()
        pending_txs_job.start()
        garbage_collector_job.start()
    else:
        tasks.append(retry_messages_task(shared_stats=shared_stats))
        tasks.append(handle_txs_task())

    if config.ipfs.enabled.value:
        tasks.append(reconnect_ipfs_job(config))

    return tasks
