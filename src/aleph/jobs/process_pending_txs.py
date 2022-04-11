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
from .job_utils import prepare_loop

LOGGER = logging.getLogger("jobs.pending_txs")


async def handle_pending_tx(
    pending, actions_list: List, seen_ids: Optional[List] = None
):
    tx_context = TxContext(**pending["context"])
    LOGGER.info("%s Handling TX in block %s", tx_context.chain_name, tx_context.height)

    messages = await get_chaindata_messages(
        pending["content"], tx_context, seen_ids=seen_ids
    )
    if messages:
        message_actions = list()
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
            message_actions.append(
                InsertOne(
                    {
                        "message": message,
                        "source": dict(
                            chain_name=tx_context.chain_name,
                            tx_hash=tx_context.tx_hash,
                            height=tx_context.height,
                            check_message=True,  # should we store this?
                        ),
                    }
                )
            )
            await asyncio.sleep(0)

        if message_actions:
            await PendingMessage.collection.bulk_write(message_actions)
    else:
        LOGGER.debug("TX contains no message")

    if messages is not None:
        # bogus or handled, we remove it.
        actions_list.append(DeleteOne({"_id": pending["_id"]}))


async def join_pending_txs_tasks(tasks, actions_list):
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, BaseException):
            LOGGER.exception(
                "error in incoming txs task",
                exc_info=(type(result), result, result.__traceback__),
            )

    tasks.clear()

    if len(actions_list):
        await PendingTX.collection.bulk_write(actions_list)
        actions_list.clear()


async def process_pending_txs():
    """Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""
    if not await PendingTX.collection.count_documents({}):
        await asyncio.sleep(5)
        return

    actions = []
    tasks = []
    seen_offchain_hashes = []
    seen_ids = []
    i = 0
    LOGGER.info("handling TXs")
    async for pending in PendingTX.collection.find().sort([("context.time", 1)]):
        if pending["content"]["protocol"] == "aleph-offchain":
            if pending["content"]["content"] not in seen_offchain_hashes:
                seen_offchain_hashes.append(pending["content"]["content"])
            else:
                continue

        i += 1
        tasks.append(handle_pending_tx(pending, actions, seen_ids=seen_ids))

        if i > 200:
            await join_pending_txs_tasks(tasks, actions)
            i = 0

    await join_pending_txs_tasks(tasks, actions)


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
