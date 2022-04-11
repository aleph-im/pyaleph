"""
Job in charge of loading messages stored on-chain and put them in the pending message queue.
"""

import asyncio
import logging
from typing import List, Dict, Optional

import sentry_sdk
from pymongo import DeleteOne, InsertOne
from pymongo.errors import CursorNotFound
from setproctitle import setproctitle

from aleph.chains.common import get_chaindata_messages
from aleph.chains.tx_context import TxContext
from aleph.exceptions import InvalidMessageError
from aleph.logging import setup_logging
from aleph.model.pending import PendingMessage, PendingTX
from aleph.network import check_message
from aleph.services.p2p import singleton
from .job_utils import prepare_loop, gather_and_perform_db_operations
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.toolkit.batch import async_batch

LOGGER = logging.getLogger("jobs.pending_txs")


async def handle_pending_tx(
    pending_tx, seen_ids: Optional[List] = None
) -> List[DbBulkOperation]:

    db_operations = []
    tx_context = TxContext(**pending_tx["context"])
    LOGGER.info("%s Handling TX in block %s", tx_context.chain_name, tx_context.height)

    messages = await get_chaindata_messages(
        pending_tx["content"], tx_context, seen_ids=seen_ids
    )
    if messages:
        for i, message in enumerate(messages):
            message["time"] = tx_context.time + (i / 1000)  # force order

            try:
                message = await check_message(
                    message, trusted=True
                )  # we don't check signatures yet.
            except InvalidMessageError as error:
                LOGGER.warning(error)
                continue

            # we add it to the message queue... bad idea? should we process it asap?
            db_operations.append(
                DbBulkOperation(
                    collection=PendingMessage,
                    operation=InsertOne(
                        {
                            "message": message,
                            "source": dict(
                                chain_name=tx_context.chain_name,
                                tx_hash=tx_context.tx_hash,
                                height=tx_context.height,
                                check_message=True,  # should we store this?
                            ),
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


async def join_pending_txs_tasks(tasks):
    await gather_and_perform_db_operations(
        tasks,
        on_error=lambda e: LOGGER.exception(
            "error in incoming txs task",
            exc_info=(type(e), e, e.__traceback__),
        ),
    )


async def process_pending_txs():
    """
    Process chain transactions in the Pending TX queue.
    """

    batch_size = 200

    seen_offchain_hashes = set()
    seen_ids = []
    LOGGER.info("handling TXs")
    async for pending_tx_batch in async_batch(
        PendingTX.collection.find().sort([("context.time", 1)]), batch_size
    ):
        tasks = []
        for pending_tx in pending_tx_batch:
            if pending_tx["content"]["protocol"] == "aleph-offchain":
                if pending_tx["content"]["content"] in seen_offchain_hashes:
                    continue

            seen_offchain_hashes.add(pending_tx["content"]["content"])
            tasks.append(handle_pending_tx(pending_tx, seen_ids=seen_ids))

        await join_pending_txs_tasks(tasks)


async def handle_txs_task():
    await asyncio.sleep(4)
    while True:
        try:
            await process_pending_txs()
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
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(handle_txs_task())
