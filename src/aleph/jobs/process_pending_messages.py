"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from logging import getLogger
from typing import List, Dict, Tuple

import sentry_sdk
from aleph_message.models import MessageType
from pymongo import DeleteOne, DeleteMany, ASCENDING
from setproctitle import setproctitle

from aleph.chains.common import incoming, IncomingStatus
from aleph.logging import setup_logging
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.pending import PendingMessage
from aleph.services.p2p import singleton
from aleph.types import ItemType
from .job_utils import prepare_loop, gather_and_perform_db_operations

LOGGER = getLogger("jobs.pending_messages")


async def handle_pending_message(
    pending: Dict,
    seen_ids: Dict[Tuple, int],
) -> List[DbBulkOperation]:
    status, operations = await incoming(
        pending["message"],
        chain_name=pending["source"].get("chain_name"),
        tx_hash=pending["source"].get("tx_hash"),
        height=pending["source"].get("height"),
        seen_ids=seen_ids,
        check_message=pending["source"].get("check_message", True),
        retrying=True,
        existing_id=pending["_id"],
    )

    if status != IncomingStatus.RETRYING_LATER:
        operations.append(
            DbBulkOperation(PendingMessage, DeleteOne({"_id": pending["_id"]}))
        )

    return operations


async def join_pending_message_tasks(tasks):
    await gather_and_perform_db_operations(
        tasks,
        on_error=lambda e: LOGGER.error("Error while processing message: %s", e),
    )
    tasks.clear()


async def process_pending_messages(shared_stats: Dict):
    """
    Processes all the messages in the pending message queue.
    """

    seen_ids: Dict[Tuple, int] = dict()
    gtasks: List[asyncio.Task] = []
    tasks: List[asyncio.Task] = []
    i: int = 0
    j: int = 0
    find_params: Dict = {}

    while await PendingMessage.collection.count_documents(find_params):
        async for pending in PendingMessage.collection.find(find_params).sort(
            [("retries", ASCENDING), ("message.time", ASCENDING)]
        ).batch_size(256):
            LOGGER.debug(
                f"retry_message_job len_seen_ids={len(seen_ids)} "
                f"len_gtasks={len(gtasks)} len_tasks={len(tasks)}"
            )

            shared_stats["retry_messages_job_seen_ids"] = len(seen_ids)
            shared_stats["retry_messages_job_gtasks"] = len(gtasks)
            shared_stats["retry_messages_job_tasks"] = len(tasks)
            shared_stats["retry_messages_job_i"] = i
            shared_stats["retry_messages_job_j"] = j

            if pending.get("message") is None:
                LOGGER.warning(
                    "Found PendingMessage with empty message, this should be caught before insertion"
                )
                await PendingMessage.collection.delete_one({"_id": pending["_id"]})
                continue

            if not isinstance(pending["message"], dict):
                raise ValueError(
                    "Pending message is not a dictionary and cannot be read."
                )

            if (
                pending["message"]["item_type"] == ItemType.IPFS
                or pending["message"]["type"] == MessageType.store
            ):
                i += 15
                j += 100
            else:
                i += 1
                j += 1

            tasks.append(asyncio.create_task(handle_pending_message(pending, seen_ids)))

            if j >= 20000:
                # Group tasks using asyncio.gather in `gtasks`.
                gtasks.append(
                    asyncio.create_task(
                        join_pending_message_tasks(
                            tasks,
                        )
                    )
                )
                tasks = []
                i = 0
                j = 0

            if i >= 1024:
                await join_pending_message_tasks(tasks)
                tasks = []
                i = 0

        gtasks.append(asyncio.create_task(join_pending_message_tasks(tasks)))

        await asyncio.gather(*gtasks, return_exceptions=True)
        gtasks = []

        if await PendingMessage.collection.count_documents(find_params) > 100000:
            LOGGER.info("Cleaning messages")
            clean_actions = []
            # big collection, try to remove dups.
            for key, height in seen_ids.items():
                clean_actions.append(
                    DeleteMany(
                        {
                            "message.item_hash": key[0],
                            "message.sender": key[1],
                            "source.chain_name": key[2],
                            "source.height": {"$gt": height},
                        }
                    )
                )
            result = await PendingMessage.collection.bulk_write(clean_actions)
            LOGGER.info(repr(result))


async def retry_messages_task(shared_stats: Dict):
    """Handle message that were added to the pending queue"""
    await asyncio.sleep(4)
    while True:
        try:
            await process_pending_messages(shared_stats=shared_stats)
            await asyncio.sleep(5)

        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        LOGGER.debug("Waiting 5 seconds for new pending messages...")
        await asyncio.sleep(5)


def pending_messages_subprocess(
    config_values: Dict, shared_stats: Dict, api_servers: List
):
    """
    Background task that processes all the messages received by the node.

    :param config_values: Application configuration, as a dictionary.
    :param shared_stats: Dictionary of application metrics. This dictionary is updated by othe
                         processes and must be allocated from shared memory.
    :param api_servers: List of Core Channel Nodes with an HTTP interface found on the network.
                        This list is updated by other processes and must be allocated from
                        shared memory by the caller.
    """

    setproctitle("aleph.jobs.messages_task_loop")
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/messages_task_loop.log",
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(retry_messages_task(shared_stats))
