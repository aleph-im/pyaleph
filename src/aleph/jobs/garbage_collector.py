"""
Job in charge of deleting files from IPFS and local storage when they are scheduled for deletion.
"""

import asyncio
import datetime as dt
import logging
from typing import Dict

import sentry_sdk
from setproctitle import setproctitle

from aleph.logging import setup_logging
from aleph.model.hashes import delete_value as delete_gridfs_file
from aleph.model.scheduled_deletions import ScheduledDeletion, ScheduledDeletionInfo
from .job_utils import prepare_loop

LOGGER = logging.getLogger("jobs.garbage_collector")


async def delete_file(file_to_delete: ScheduledDeletionInfo) -> None:
    await delete_gridfs_file(key=file_to_delete.filename)
    LOGGER.info("Deleted '%s' from local storage", file_to_delete.filename)


async def garbage_collector_task(job_period: int):
    while True:
        try:
            async for file_to_delete in ScheduledDeletion.files_to_delete(
                delete_by=dt.datetime.utcnow()
            ):
                try:
                    await delete_file(file_to_delete)
                finally:
                    ScheduledDeletion.collection.delete_one(
                        {"_id": file_to_delete.object_id}
                    )

        except Exception:
            LOGGER.exception("Error in garbage collector job")
            # Sleep to avoid overloading the logs in case of a repeating error
            await asyncio.sleep(5)

        await asyncio.sleep(job_period)


def garbage_collector_subprocess(config_values: Dict):
    setproctitle("aleph.jobs.garbage_collector")
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/aleph_ccn_garbage_collector.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    loop.run_until_complete(
        garbage_collector_task(job_period=config.storage.garbage_collector.period.value)
    )
