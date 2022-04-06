import asyncio
import logging
from logging import getLogger
from multiprocessing import Process
from typing import Coroutine, List, Dict, Optional, Tuple

import aioipfs
import sentry_sdk
from pymongo import DeleteOne, InsertOne, DeleteMany, UpdateOne, ASCENDING
from pymongo.errors import CursorNotFound
from setproctitle import setproctitle

from aleph.chains.common import incoming, get_chaindata_messages, IncomingStatus
from aleph.model.messages import Message, CappedMessage
from aleph.model.p2p import get_peers
from aleph.model.pending import PendingMessage, PendingTX
from aleph.network import check_message
from aleph.services.ipfs.common import connect_ipfs_peer
from aleph.services.p2p import singleton
from aleph.types import ItemType
from aleph.exceptions import InvalidMessageError
from aleph.logging import setup_logging

LOGGER = getLogger("JOBS")


async def handle_pending_message(
    pending: Dict,
    seen_ids: Dict[Tuple, int],
    actions_list: List[DeleteOne],
    messages_actions_list: List[UpdateOne],
):
    result = await incoming(
        pending["message"],
        chain_name=pending["source"].get("chain_name"),
        tx_hash=pending["source"].get("tx_hash"),
        height=pending["source"].get("height"),
        seen_ids=seen_ids,
        check_message=pending["source"].get("check_message", True),
        retrying=True,
        bulk_operation=True,
        existing_id=pending["_id"],
    )

    if result == IncomingStatus.RETRYING_LATER:
        return

    if not isinstance(result, IncomingStatus):
        assert isinstance(result, UpdateOne)
        messages_actions_list.append(result)

    actions_list.append(DeleteOne({"_id": pending["_id"]}))


async def join_pending_message_tasks(
    tasks, actions_list=None, messages_actions_list=None
):
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
        LOGGER.exception("error in incoming task")
    tasks.clear()

    if messages_actions_list is not None and len(messages_actions_list):
        await Message.collection.bulk_write(messages_actions_list)
        await CappedMessage.collection.bulk_write(messages_actions_list)
        messages_actions_list.clear()

    if actions_list is not None and len(actions_list):
        await PendingMessage.collection.bulk_write(actions_list)
        actions_list.clear()


async def retry_messages_job(shared_stats: Dict):
    """Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""

    seen_ids: Dict[Tuple, int] = dict()
    actions: List[DeleteOne] = []
    messages_actions: List[UpdateOne] = []
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
            shared_stats["retry_messages_job_actions"] = len(actions)
            shared_stats["retry_messages_job_messages_actions"] = len(messages_actions)
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
                or pending["message"]["type"] == "STORE"
            ):
                i += 15
                j += 100
            else:
                i += 1
                j += 1

            tasks.append(
                asyncio.create_task(
                    handle_pending_message(pending, seen_ids, actions, messages_actions)
                )
            )

            if j >= 20000:
                # Group tasks using asyncio.gather in `gtasks`.
                gtasks.append(
                    asyncio.create_task(
                        join_pending_message_tasks(
                            tasks,
                            actions_list=actions,
                            messages_actions_list=messages_actions,
                        )
                    )
                )
                tasks = []
                actions = []
                messages_actions = []
                i = 0
                j = 0

            if i >= 1024:
                await join_pending_message_tasks(tasks)
                # gtasks.append(asyncio.create_task(join_pending_message_tasks(tasks)))
                tasks = []
                i = 0

        gtasks.append(
            asyncio.create_task(
                join_pending_message_tasks(
                    tasks, actions_list=actions, messages_actions_list=messages_actions
                )
            )
        )

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

        await asyncio.sleep(5)


async def retry_messages_task(shared_stats: Dict):
    """Handle message that were added to the pending queue"""
    await asyncio.sleep(4)
    while True:
        try:
            await retry_messages_job(shared_stats=shared_stats)
        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        LOGGER.debug("Waiting 5 seconds for new pending messages...")
        await asyncio.sleep(5)


async def handle_pending_tx(
    pending, actions_list: List, seen_ids: Optional[List] = None
):
    LOGGER.info(
        "%s Handling TX in block %s"
        % (pending["context"]["chain_name"], pending["context"]["height"])
    )
    messages = await get_chaindata_messages(
        pending["content"], pending["context"], seen_ids=seen_ids
    )
    if isinstance(messages, list):
        message_actions = list()
        for i, message in enumerate(messages):
            message["time"] = pending["context"]["time"] + (i / 1000)  # force order

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
                            chain_name=pending["context"]["chain_name"],
                            tx_hash=pending["context"]["tx_hash"],
                            height=pending["context"]["height"],
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


async def handle_txs_job():
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
            await handle_txs_job()
            await asyncio.sleep(5)
        except CursorNotFound:
            LOGGER.exception("Cursor error in pending txs job ")
        except Exception:
            LOGGER.exception("Error in pending txs job")

        await asyncio.sleep(0.01)


def prepare_loop(config_values: Dict) -> asyncio.AbstractEventLoop:
    from aleph.model import init_db
    from aleph.web import app
    from configmanager import Config
    from aleph.config import get_defaults
    from aleph.services.ipfs.common import get_ipfs_api
    from aleph.services.p2p import http, init_p2p_client

    http.SESSION = None  # type:ignore

    loop = asyncio.get_event_loop()

    config = Config(schema=get_defaults())
    app["config"] = config
    config.load_values(config_values)

    init_db(config, ensure_indexes=False)
    loop.run_until_complete(get_ipfs_api(timeout=2, reset=True))
    _ = init_p2p_client(config)
    return loop


def txs_task_loop(config_values: Dict, api_servers: List):
    setproctitle("aleph.jobs.txs_task_loop")
    sentry_sdk.init(
        dsn=config_values["sentry"]["dsn"],
        traces_sample_rate=config_values["sentry"]["traces_sample_rate"],
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config_values["logging"]["level"],
        filename="/tmp/txs_task_loop.log",
    )
    singleton.api_servers = api_servers

    loop = prepare_loop(config_values)
    loop.run_until_complete(handle_txs_task())


def messages_task_loop(config_values: Dict, shared_stats: Dict, api_servers: List):
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
    sentry_sdk.init(
        dsn=config_values["sentry"]["dsn"],
        traces_sample_rate=config_values["sentry"]["traces_sample_rate"],
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config_values["logging"]["level"],
        filename="/tmp/messages_task_loop.log",
    )
    singleton.api_servers = api_servers

    loop = prepare_loop(config_values)
    loop.run_until_complete(asyncio.gather(retry_messages_task(shared_stats)))


async def reconnect_ipfs_job(config):
    from aleph.services.utils import get_IP

    my_ip = await get_IP()
    await asyncio.sleep(2)
    while True:
        try:
            LOGGER.info("Reconnecting to peers")
            for peer in config.ipfs.peers.value:
                try:
                    ret = await connect_ipfs_peer(peer)
                    if "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

            async for peer in get_peers(peer_type="IPFS"):
                if peer in config.ipfs.peers.value:
                    continue

                if my_ip in peer:
                    continue

                try:
                    ret = await connect_ipfs_peer(peer)
                    if ret and "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.ipfs.reconnect_delay.value)


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
        p1 = Process(
            target=messages_task_loop,
            args=(
                config_values,
                shared_stats,
                api_servers,
            ),
        )
        p2 = Process(
            target=txs_task_loop,
            args=(config_values, api_servers),
        )
        p1.start()
        p2.start()
    else:
        tasks.append(retry_messages_task(shared_stats=shared_stats))
        tasks.append(handle_txs_task())

    if config.ipfs.enabled.value:
        tasks.append(reconnect_ipfs_job(config))

    return tasks
