"""
Job in charge of loading messages stored on-chain and put them in the pending message queue.
"""

import asyncio
import logging
import time
from typing import List, Dict, Optional
from typing import Set

import sentry_sdk
from configmanager import Config
from pymongo import DeleteOne, InsertOne
from pymongo.errors import CursorNotFound
from setproctitle import setproctitle

from aleph.chains.common import get_chaindata_messages
from aleph.chains.tx_context import TxContext
from aleph.exceptions import InvalidMessageError
from aleph.logging import setup_logging
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.pending import PendingMessage, PendingTX
from aleph.services.p2p import singleton
from .job_utils import prepare_loop, process_job_results
from ..schemas.pending_messages import parse_message

LOGGER = logging.getLogger("jobs.pending_txs")


async def handle_pending_tx(
    pending_tx, seen_ids: Optional[List] = None
) -> List[DbBulkOperation]:

    db_operations: List[DbBulkOperation] = []
    tx_context = TxContext(**pending_tx["context"])
    LOGGER.info("%s Handling TX in block %s", tx_context.chain, tx_context.height)

    messages = await get_chaindata_messages(
        pending_tx["content"], tx_context, seen_ids=seen_ids
    )
    if messages:
        for i, message_dict in enumerate(messages):
            reception_time = time.time()
            # TODO: this update of the time field is unwanted, but needed to preserve
            #       the behavior of aggregates. Use the correct time field in aggregates
            #       and then remove this line.
            message_dict["time"] = tx_context.time + (i / 1000)

            try:
                # we don't check signatures yet.
                message = parse_message(message_dict)
            except InvalidMessageError as error:
                LOGGER.warning(error)
                continue

            message.time = tx_context.time + (i / 1000)  # force order

            # we add it to the message queue... bad idea? should we process it asap?
            db_operations.append(
                DbBulkOperation(
                    collection=PendingMessage,
                    operation=InsertOne(
                        {
                            "message": message.dict(exclude={"content"}),
                            "tx_context": tx_context.dict(),
                            "reception_time": reception_time,
                            "check_message": True,
                        }
                    ),
                )
            )
            await asyncio.sleep(0)

    else:
        LOGGER.debug("TX contains no message")

    if messages is not None:
        # bogus or handled, we remove it.
        db_operations.append(
            DbBulkOperation(
                collection=PendingTX, operation=DeleteOne({"_id": pending_tx["_id"]})
            )
        )

    return db_operations


async def process_tx_job_results(tasks: Set[asyncio.Task]):
    await process_job_results(
        tasks,
        on_error=lambda e: LOGGER.exception(
            "error in incoming txs task",
            exc_info=(type(e), e, e.__traceback__),
        ),
    )


async def process_pending_txs(max_concurrent_tasks: int):
    """
    Process chain transactions in the Pending TX queue.
    """

    tasks: Set[asyncio.Task] = set()

    seen_offchain_hashes = set()
    seen_ids: List[str] = []
    LOGGER.info("handling TXs")
    async for pending_tx in PendingTX.collection.find().sort([("context.time", 1)]):
        if pending_tx["content"]["protocol"] == "aleph-offchain":
            if pending_tx["content"]["content"] in seen_offchain_hashes:
                continue

        if len(tasks) == max_concurrent_tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            await process_tx_job_results(done)

        seen_offchain_hashes.add(pending_tx["content"]["content"])
        tx_task = asyncio.create_task(handle_pending_tx(pending_tx, seen_ids=seen_ids))
        tasks.add(tx_task)

    # Wait for the last tasks
    if tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        await process_tx_job_results(done)


async def handle_txs_task(config: Config):
    max_concurrent_tasks = config.aleph.jobs.pending_txs.max_concurrency.value

    await asyncio.sleep(4)
    while True:
        try:
            await process_pending_txs(max_concurrent_tasks)
            await asyncio.sleep(5)
        except CursorNotFound:
            LOGGER.exception("Cursor error in pending txs job ")
        except Exception:
            LOGGER.exception("Error in pending txs job")

        await asyncio.sleep(0.01)


def pending_txs_subprocess(config_values: Dict, api_servers: List):
    setproctitle("aleph.jobs.txs_task_loop")
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/txs_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(handle_txs_task(config))
