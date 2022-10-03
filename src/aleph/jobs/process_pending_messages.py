"""
Job in charge of (re-) processing Aleph messages waiting in the pending queue.
"""

import asyncio
from functools import partial
from logging import getLogger
from typing import Any, Dict, List, Set, Tuple, cast

import sentry_sdk
from aleph_message.models import MessageType
from configmanager import Config, NotFound
from pymongo import DeleteOne, DeleteMany, ASCENDING
from setproctitle import setproctitle

from aleph.chains.chain_service import ChainService
from aleph.chains.common import incoming, IncomingStatus
from aleph.exceptions import InvalidMessageError
from aleph.handlers.message_handler import MessageHandler, IncomingStatus
from aleph.logging import setup_logging
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.pending import PendingMessage
from aleph.schemas.pending_messages import parse_message
from aleph.services.ipfs import IpfsService
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.p2p import singleton
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from .job_utils import prepare_loop, process_job_results
from ..chains.tx_context import TxContext

LOGGER = getLogger("jobs.pending_messages")


def _init_semaphores(config: Config) -> Dict[MessageType, asyncio.BoundedSemaphore]:
    semaphores = {}
    config_section = config.aleph.jobs.pending_messages
    max_concurrency = config_section.max_concurrency.value

    for message_type in MessageType:
        try:
            sem_value = getattr(config_section, message_type.lower()).value
        except NotFound:
            sem_value = max_concurrency

        LOGGER.debug("%s: sem_value=%d", message_type, sem_value)
        semaphores[cast(MessageType, message_type)] = asyncio.BoundedSemaphore(
            sem_value
        )

    return semaphores


async def handle_pending_message(
    pending: Dict,
    sem: asyncio.Semaphore,
    seen_ids: Dict[Tuple, int],
) -> List[DbBulkOperation]:

    delete_pending_message_op = DbBulkOperation(
        PendingMessage, DeleteOne({"_id": pending["_id"]})
    )

    try:
        message = parse_message(pending["message"])
    except InvalidMessageError:
        # If an invalid message somehow ended in pending messages, drop it.
        return [delete_pending_message_op]

    tx_context_dict = pending.get("tx_context")
    tx_context = TxContext.parse_obj(tx_context_dict) if tx_context_dict else None

    async with sem:
        status, operations = await incoming(
            pending_message=message,
            tx_context=tx_context,
            seen_ids=seen_ids,
            check_message=pending["source"].get("check_message", True),
            retrying=True,
            existing_id=pending["_id"],
        )

    if status != IncomingStatus.RETRYING_LATER:
        operations.append(delete_pending_message_op)

    return operations


async def process_message_job_results(
    finished_tasks: Set[asyncio.Task],
    task_message_dict: Dict[asyncio.Task, Dict],
    shared_stats: Dict[str, Any],
    processing_messages: Set[Tuple],
):
    await process_job_results(
        finished_tasks,
        on_error=partial(LOGGER.exception, "Error while processing message: %s"),
    )

    for message_task in finished_tasks:
        pending = task_message_dict[message_task]
        message_type = MessageType(pending["message"]["type"])
        shared_stats["retry_messages_job_tasks"] -= 1
        shared_stats["message_jobs"][message_type] -= 1

        pending_message_id = _get_pending_message_id(pending)
        processing_messages.remove(pending_message_id)

        del task_message_dict[message_task]


def _validate_pending_message(pending: Dict):
    if pending.get("message") is None:
        raise ValueError(
            f"Found PendingMessage with empty message: {pending['_id']}. "
            "This should be caught before insertion"
        )

    if not isinstance(pending["message"], dict):
        raise ValueError(
            f"Pending message is not a dictionary and cannot be read: {pending['_id']}."
        )


def _get_pending_message_id(pending_message: Dict) -> Tuple:
    source = pending_message.get("source", {})
    chain_name = source.get("chain_name", None)
    height = source.get("height", None)

    return (
        pending_message["message"]["item_hash"],
        pending_message["message"]["sender"],
        chain_name,
        height,
    )


class PendingMessageProcessor:
    def __init__(self, message_handler: MessageHandler):
        self.message_handler = message_handler

    async def _handle_pending_message(
        self,
        pending: Dict,
        sem: asyncio.Semaphore,
        seen_ids: Dict[Tuple, int],
    ) -> List[DbBulkOperation]:

        delete_pending_message_op = DbBulkOperation(
            PendingMessage, DeleteOne({"_id": pending["_id"]})
        )

        try:
            message = parse_message(pending["message"])
        except InvalidMessageError:
            # If an invalid message somehow ended in pending messages, drop it.
            return [delete_pending_message_op]

        async with sem:
            status, operations = await self.message_handler.incoming(
                pending_message=message,
                chain_name=pending["source"].get("chain_name"),
                tx_hash=pending["source"].get("tx_hash"),
                height=pending["source"].get("height"),
                seen_ids=seen_ids,
                check_message=pending["source"].get("check_message", True),
                retrying=True,
                existing_id=pending["_id"],
            )

        if status != IncomingStatus.RETRYING_LATER:
            operations.append(delete_pending_message_op)

        return operations

    @staticmethod
    async def _process_message_job_results(
        finished_tasks: Set[asyncio.Task],
        task_message_dict: Dict[asyncio.Task, Dict],
        shared_stats: Dict[str, Any],
        processing_messages: Set[Tuple],
    ):
        await process_job_results(
            finished_tasks,
            on_error=partial(LOGGER.exception, "Error while processing message: %s"),
        )

        for message_task in finished_tasks:
            pending = task_message_dict[message_task]
            message_type = MessageType(pending["message"]["type"])
            shared_stats["retry_messages_job_tasks"] -= 1
            shared_stats["message_jobs"][message_type] -= 1

            pending_message_id = _get_pending_message_id(pending)
            processing_messages.remove(pending_message_id)

            del task_message_dict[message_task]

    async def process_pending_messages(self, config: Config, shared_stats: Dict):
        """
        Processes all the messages in the pending message queue.
        """

        seen_ids: Dict[Tuple, int] = dict()
        processing_messages = set()
        find_params: Dict = {}

        max_concurrent_tasks = config.aleph.jobs.pending_messages.max_concurrency.value
        semaphores = _init_semaphores(config)

        # Reset stats to avoid nonsensical values if the job restarts
        shared_stats["retry_messages_job_tasks"] = 0
        for message_type in MessageType:
            shared_stats["message_jobs"][message_type] = 0

        # Using a set is required as asyncio.wait takes and returns sets.
        pending_tasks: Set[asyncio.Task] = set()
        task_message_dict: Dict[asyncio.Task, Dict] = {}

        while await PendingMessage.collection.count_documents(find_params):

            async for pending in PendingMessage.collection.find(find_params).sort(
                [("retries", ASCENDING), ("message.time", ASCENDING)]
            ).batch_size(max_concurrent_tasks):

                # Check if the message is already processing
                pending_message_id = _get_pending_message_id(pending)
                if pending_message_id in processing_messages:
                    # Skip the message, we're already processing it
                    continue

                processing_messages.add(pending_message_id)

                if len(pending_tasks) == max_concurrent_tasks:
                    finished_tasks, pending_tasks = await asyncio.wait(
                        pending_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    await self._process_message_job_results(
                        finished_tasks,
                        task_message_dict,
                        shared_stats,
                        processing_messages,
                    )

                _validate_pending_message(pending)
                message_type = MessageType(pending["message"]["type"])

                shared_stats["retry_messages_job_seen_ids"] = len(seen_ids)
                shared_stats["retry_messages_job_tasks"] += 1
                shared_stats["message_jobs"][message_type] += 1

                message_task = asyncio.create_task(
                    self._handle_pending_message(
                        pending=pending, sem=semaphores[message_type], seen_ids=seen_ids
                    )
                )
                pending_tasks.add(message_task)
                task_message_dict[message_task] = pending

            # This synchronization point is required when a few pending messages remain.
            # We wait for at least one task to finish; the remaining tasks will be collected
            # on the next iterations of the loop.
            if pending_tasks:
                finished_tasks, pending_tasks = await asyncio.wait(
                    pending_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                await self._process_message_job_results(
                    finished_tasks, task_message_dict, shared_stats, processing_messages
                )

            # TODO: move this to a dedicated job and/or check unicity on insertion
            #       in pending messages
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


async def retry_messages_task(config: Config, shared_stats: Dict):
    """Handle message that were added to the pending queue"""
    await asyncio.sleep(4)

    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
    )
    chain_service = ChainService(storage_service=storage_service)
    message_handler = MessageHandler(
        chain_service=chain_service, storage_service=storage_service
    )
    pending_message_handler = PendingMessageProcessor(message_handler=message_handler)

    while True:
        try:
            await pending_message_handler.process_pending_messages(
                config=config, shared_stats=shared_stats
            )
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
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(
        retry_messages_task(config=config, shared_stats=shared_stats)
    )
