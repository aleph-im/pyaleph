import asyncio
import logging
from logging import getLogger
from multiprocessing import Process
from multiprocessing.managers import SyncManager, RemoteError
from typing import Coroutine, List, Dict, Optional, Tuple

import aioipfs
import sentry_sdk
from pymongo import DeleteOne, InsertOne, DeleteMany, UpdateOne
from pymongo.errors import CursorNotFound
from setproctitle import setproctitle

from aleph.chains.common import incoming, get_chaindata_messages, IncomingStatus
from aleph.model.messages import Message, CappedMessage
from aleph.model.p2p import get_peers
from aleph.model.pending import PendingMessage, PendingTX
from aleph.network import check_message
from aleph.services import filestore
from aleph.services.ipfs.common import connect_ipfs_peer
from aleph.types import ItemType, InvalidMessageError

LOGGER = getLogger("JOBS")

MANAGER = None

RETRY_LOCK = asyncio.Lock()


class DBManager(SyncManager):
    pass


async def handle_pending_message(
    pending: Dict, seen_ids: Dict[Tuple, int], actions_list: List[DeleteOne], messages_actions_list: List[UpdateOne]
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


async def retry_messages_job(shared_stats: Optional[Dict]):
    """Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""

    seen_ids: Dict[Tuple, int] = dict()
    actions: List[DeleteOne] = []
    messages_actions: List[UpdateOne] = []
    gtasks: List[asyncio.Task] = []
    tasks: List[asyncio.Task] = []
    loop = asyncio.get_event_loop()
    i: int = 0
    j: int = 0
    find_params: Dict = {}
    # if await PendingTX.collection.count_documents({}) > 500:
    #     find_params = {'message.item_type': 'inline'}

    while await PendingMessage.collection.count_documents(find_params):
        async for pending in PendingMessage.collection.find(find_params).sort(
            [("message.time", 1)]
        ).batch_size(256):
            LOGGER.debug(
                f"retry_message_job len_seen_ids={len(seen_ids)} "
                f"len_gtasks={len(gtasks)} len_tasks={len(tasks)}"
            )

            if shared_stats is not None:
                shared_stats["retry_messages_job_seen_ids"] = len(seen_ids)
                shared_stats["retry_messages_job_gtasks"] = len(gtasks)
                shared_stats["retry_messages_job_tasks"] = len(tasks)
                shared_stats["retry_messages_job_actions"] = len(actions)
                shared_stats["retry_messages_job_messages_actions"] = len(
                    messages_actions
                )
                shared_stats["retry_messages_job_i"] = i
                shared_stats["retry_messages_job_j"] = j

            if pending["message"] is None:
                LOGGER.warning("Found PendingMessage with empty message, this should be caught before insertion")
                await PendingMessage.collection.delete_one({"_id": pending["_id"]})

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
                # await join_pending_message_tasks(tasks, actions_list=actions, messages_actions_list=messages_actions)
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
    # async for pending in PendingMessage.collection.find(
    #     {'message.item_content': { "$exists": False } }).sort([('message.time', 1)]).limit(100):
    #     i += 1
    #     tasks.append(asyncio.shield(handle_pending_message(pending, seen_ids, actions)))

    #     # if (i > 100):
    #     #     await join_pending_message_tasks(tasks, actions)
    #     #     i = 0


async def retry_messages_task(shared_stats: Optional[Dict]):
    """Handle message that were added to the pending queue"""
    await asyncio.sleep(4)
    while True:
        try:
            await retry_messages_job(shared_stats=shared_stats)
        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        LOGGER.debug("Waiting 5 seconds for new pending messages...")
        await asyncio.sleep(5)


async def handle_pending_tx(pending, actions_list: List, seen_ids: Optional[List] = None):
    LOGGER.info(
        "%s Handling TX in block %s"
        % (pending["context"]["chain_name"], pending["context"]["height"])
    )
    messages = await get_chaindata_messages(pending["content"], pending["context"], seen_ids=seen_ids)
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

    if messages is not None:
        # bogus or handled, we remove it.
        actions_list.append(DeleteOne({"_id": pending["_id"]}))
    # LOGGER.info("%s Handled TX in block %s" % (pending['context']['chain_name'], pending['context']['height']))


async def join_pending_txs_tasks(tasks, actions_list):
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
        LOGGER.exception("error in incoming txs task")
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


def function_proxy(manager, funcname):
    def func_call(*args, **kwargs):
        rvalue = getattr(manager, funcname)(*args, **kwargs)

        try:
            value = rvalue._getvalue()

        except RemoteError:
            value = rvalue

        return value

    return func_call


def initialize_db_process(config_values):
    from aleph.web import app
    from configmanager import Config
    from aleph.config import get_defaults

    config = Config(schema=get_defaults())
    app["config"] = config
    config.load_values(config_values)

    filestore.init_store(config)


def prepare_manager(config_values) -> DBManager:
    from aleph.services import filestore
    from aleph.services.filestore import __get_value, __set_value

    DBManager.register("_set_value", __set_value)
    DBManager.register("_get_value", __get_value)
    manager = DBManager()
    server = manager.get_server()
    # server.serve_forever()

    manager.start(initialize_db_process, [config_values])
    filestore._set_value = function_proxy(manager, "_set_value")
    filestore._get_value = function_proxy(manager, "_get_value")
    return manager


def prepare_loop(config_values, manager=None, idx=1):
    from aleph.model import init_db
    from aleph.web import app
    from configmanager import Config
    from aleph.config import get_defaults
    from aleph.services.ipfs.common import get_ipfs_api
    from aleph.services.p2p import init_p2p, http
    from aleph.services import filestore

    # uvloop.install()

    # manager = NodeManager()
    # manager.start()

    if isinstance(manager, tuple):
        manager_info = manager
        DBManager.register("_set_value")
        DBManager.register("_get_value")
        manager = DBManager(address=manager_info[0], authkey=manager_info[1])
        manager.connect()

    filestore._set_value = function_proxy(manager, "_set_value")
    filestore._get_value = function_proxy(manager, "_get_value")
    http.SESSION = None

    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    loop = asyncio.get_event_loop()

    config = Config(schema=get_defaults())
    app["config"] = config
    config.load_values(config_values)

    init_db(config, ensure_indexes=False)
    loop.run_until_complete(get_ipfs_api(timeout=2, reset=True))
    tasks = loop.run_until_complete(init_p2p(config, listen=False, port_id=idx))
    return loop, tasks


def txs_task_loop(config_values, manager):
    setproctitle('aleph.jobs.txs_task_loop')
    sentry_sdk.init(
        dsn=config_values["sentry"]["dsn"],
        traces_sample_rate=config_values["sentry"]["traces_sample_rate"],
        ignore_errors=[KeyboardInterrupt],
    )
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/txs_task_loop.log',
    )
    loop, tasks = prepare_loop(config_values, manager, idx=1)
    loop.run_until_complete(asyncio.gather(*tasks, handle_txs_task()))


def messages_task_loop(config_values, manager, shared_stats: Optional[Dict]):
    setproctitle('aleph.jobs.messages_task_loop')
    sentry_sdk.init(
        dsn=config_values["sentry"]["dsn"],
        traces_sample_rate=config_values["sentry"]["traces_sample_rate"],
        ignore_errors=[KeyboardInterrupt],
    )
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/messages_task_loop.log',
    )
    loop, tasks = prepare_loop(config_values, manager, idx=2)
    loop.run_until_complete(asyncio.gather(*tasks, retry_messages_task(shared_stats)))


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
                    if "Strings" in ret:
                        LOGGER.info("\n".join(ret["Strings"]))
                except aioipfs.APIError:
                    LOGGER.warning("Can't reconnect to %s" % peer)

        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.ipfs.reconnect_delay.value)


def start_jobs(
        config,
        shared_stats: Optional[Dict],
        manager: Optional[DBManager]=None,
        use_processes=True
) -> List[Coroutine]:
    LOGGER.info("starting jobs")
    tasks: List[Coroutine] = []

    if use_processes:
        config_values = config.dump_values()
        p1 = Process(
            target=messages_task_loop,
            args=(
                config_values,
                manager and (manager._address, manager._authkey) or None,
                shared_stats,
            ),
        )
        p2 = Process(
            target=txs_task_loop,
            args=(
                config_values,
                manager and (manager._address, manager._authkey) or None,
            ),
        )
        p1.start()
        p2.start()
    else:
        tasks.append(retry_messages_task(shared_stats=shared_stats))
        tasks.append(handle_txs_task())
    # loop.run_in_executor(executor, messages_task_loop, config_values)
    # loop.run_in_executor(executor, txs_task_loop, config_values)

    if config.ipfs.enabled.value:
        tasks.append(reconnect_ipfs_job(config))
    # loop.create_task(reconnect_p2p_job(config))

    return tasks
