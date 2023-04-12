import logging
from multiprocessing import Process
from typing import Dict, List, Coroutine

from aleph.jobs.fetch_pending_messages import fetch_pending_messages_subprocess
from aleph.jobs.process_pending_messages import (
    pending_messages_subprocess,
    fetch_and_process_messages_task,
)
from aleph.jobs.process_pending_txs import pending_txs_subprocess, handle_txs_task
from aleph.jobs.reconnect_ipfs import reconnect_ipfs_job
from aleph.services.ipfs import IpfsService
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger("jobs")


def start_jobs(
    config,
    session_factory: DbSessionFactory,
    ipfs_service: IpfsService,
    use_processes=True,
) -> List[Coroutine]:
    LOGGER.info("starting jobs")
    tasks: List[Coroutine] = []

    if use_processes:
        config_values = config.dump_values()
        p1 = Process(
            target=fetch_pending_messages_subprocess,
            args=(config_values,),
        )
        p2 = Process(
            target=pending_messages_subprocess,
            args=(config_values,),
        )
        p3 = Process(
            target=pending_txs_subprocess,
            args=(config_values,),
        )
        p1.start()
        p2.start()
        p3.start()
    else:
        tasks.append(
            fetch_and_process_messages_task(config=config)
        )
        tasks.append(handle_txs_task(config))

    if config.ipfs.enabled.value:
        tasks.append(
            reconnect_ipfs_job(
                config=config,
                session_factory=session_factory,
                ipfs_service=ipfs_service,
            )
        )

    return tasks
