from logging import getLogger
import asyncio
import aioipfs
import uvloop
from aleph.chains.common import incoming, get_chaindata_messages
from aleph.model.pending import PendingMessage, PendingTX
from aleph.model.messages import Message
from aleph.model.p2p import get_peers
from aleph.network import connect_ipfs_peer, check_message
from pymongo import DeleteOne, InsertOne, DeleteMany
from concurrent.futures import ProcessPoolExecutor

LOGGER = getLogger("JOBS")

RETRY_LOCK = asyncio.Lock()


async def handle_pending_message(pending, seen_ids, actions_list, messages_actions_list):
    result = await incoming(
        pending['message'],
        chain_name=pending['source'].get('chain_name'),
        tx_hash=pending['source'].get('tx_hash'),
        height=pending['source'].get('height'),
        seen_ids=seen_ids,
        check_message=pending['source'].get('check_message', True),
        retrying=True, bulk_operation=True)

    if result is not None:  # message handled (valid or not, we don't care)
        # Is it valid to add to a list passed this way? to be checked.
        if result is not True:
            messages_actions_list.append(result)
        actions_list.append(DeleteOne({'_id': pending['_id']}))


async def join_pending_message_tasks(tasks, actions_list, messages_actions_list):
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception:
        LOGGER.exception("error in incoming task")
    tasks.clear()

    if len(actions_list):
        await PendingMessage.collection.bulk_write(actions_list)
        actions_list.clear()

    if len(messages_actions_list):
        await Message.collection.bulk_write(messages_actions_list)
        messages_actions_list.clear()


async def retry_messages_job():
    """ Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""
    
    seen_ids = {}
    actions = []
    messages_actions = []
    tasks = []
    loop = asyncio.get_event_loop()
    i = 0
    while await PendingMessage.collection.count_documents({}):
        async for pending in PendingMessage.collection.find().sort([('message.item_type', 1)]).limit(4000):
            if pending['message']['item_type'] == 'ipfs':
                i += 15
            else:
                i += 1
                
            tasks.append(asyncio.shield(handle_pending_message(pending, seen_ids, actions, messages_actions)))

            if (i >= 64):
                await join_pending_message_tasks(tasks, actions, messages_actions)
                i = 0
        await join_pending_message_tasks(tasks, actions, messages_actions)
        
        if await PendingMessage.collection.count_documents({}) > 100000:
            LOGGER.info('Cleaning messages')
            clean_actions = []
            # big collection, try to remove dups.
            for key, height in seen_ids.items():
                clean_actions.append(DeleteMany({
                    'message.item_hash': key[0],
                    'message.sender': key[1],
                    'source.chain_name': key[2],
                    'source.height': {'$gt': height}
                }))
            result = await PendingMessage.collection.bulk_write(clean_actions)
            LOGGER.info(repr(result))
    # async for pending in PendingMessage.collection.find(
    #     {'message.item_content': { "$exists": False } }).sort([('message.time', 1)]).limit(100):
    #     i += 1
    #     tasks.append(asyncio.shield(handle_pending_message(pending, seen_ids, actions)))

    #     # if (i > 100):
    #     #     await join_pending_message_tasks(tasks, actions)
    #     #     i = 0



async def retry_messages_task():
    while True:
        try:
            await retry_messages_job()
        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        await asyncio.sleep(5)
        

async def handle_pending_tx(pending, actions_list):
    messages = await get_chaindata_messages(pending['content'], pending['context'])
    LOGGER.info("%s Handling TX in block %s" % (pending['context']['chain_name'], pending['context']['height']))
    if isinstance(messages, list):
        message_actions = list()
        for i, message in enumerate(messages):
            message['time'] = pending['context']['time'] + (i/1000) # force order
            
            message = await check_message(message, trusted=True) # we don't check signatures yet.
            if message is None:
                continue
            
            # we add it to the message queue... bad idea? should we process it asap?
            message_actions.append(InsertOne({
                'message': message,
                'source': dict(
                    chain_name=pending['context']['chain_name'],
                    tx_hash=pending['context']['tx_hash'],
                    height=pending['context']['height'],
                    check_message=True  # should we store this?
                )
            }))
            
        if message_actions:
            await PendingMessage.collection.bulk_write(message_actions)
            
    if messages is not None:
        # bogus or handled, we remove it.
        actions_list.append(DeleteOne({'_id': pending['_id']}))
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
    """ Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""
    if not await PendingTX.collection.count_documents({}):
        await asyncio.sleep(5)
        return
    
    actions = []
    tasks = []
    i = 0
    LOGGER.info("handling TXs")
    async for pending in PendingTX.collection.find().sort([('context.time', 1)]):
        i += 1
        tasks.append(asyncio.shield(handle_pending_tx(pending, actions)))

        if (i > 200):
            await join_pending_txs_tasks(tasks, actions)
            i = 0

    await join_pending_txs_tasks(tasks, actions)


async def handle_txs_task():
    while True:
        try:
            await handle_txs_job()
        except Exception:
            LOGGER.exception("Error in pending txs job")

        await asyncio.sleep(0.01)
        

def prepare_loop(config_values):
    from aleph.model import init_db
    from aleph.web import app
    from configmanager import Config
    from aleph.config import get_defaults
    from aleph.storage import get_ipfs_api
    
    uvloop.install()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    config = Config(schema=get_defaults())
    app['config'] = config
    app.config = config
    config.load_values(config_values)
    
    init_db(config, ensure_indexes=False)
    loop.run_until_complete(get_ipfs_api(timeout=2, reset=True))
    return loop

def txs_task_loop(config_values):
    loop = prepare_loop(config_values)
    loop.run_until_complete(handle_txs_task())

def messages_task_loop(config_values):
    loop = prepare_loop(config_values)
    loop.run_until_complete(retry_messages_task())
    
async def reconnect_job(config):
    while True:
        try:
            LOGGER.info("Reconnecting to peers")
            for peer in config.ipfs.peers.value:
                try:
                    ret = await connect_ipfs_peer(peer)
                    if 'Strings' in ret:
                        LOGGER.info('\n'.join(ret['Strings']))
                except aioipfs.APIError:
                    LOGGER.exception("Can't reconnect to %s" % peer)
                    
            async for peer in get_peers():
                try:
                    ret = await connect_ipfs_peer(peer)
                    if 'Strings' in ret:
                        LOGGER.info('\n'.join(ret['Strings']))
                except aioipfs.APIError:
                    LOGGER.exception("Can't reconnect to %s" % peer)
                
        except Exception:
            LOGGER.exception("Error reconnecting to peers")

        await asyncio.sleep(config.ipfs.reconnect_delay.value)

def start_jobs(config):
    LOGGER.info("starting jobs")
    # executor = ProcessPoolExecutor()
    loop = asyncio.get_event_loop()
    config_values = config.dump_values()
    # loop.run_in_executor(executor, messages_task_loop, config_values)
    # loop.run_in_executor(executor, txs_task_loop, config_values)
    loop.create_task(retry_messages_task())
    loop.create_task(handle_txs_task())
    loop.create_task(reconnect_job(config))
    # loop.create_task(loop.run_in_executor(executor, messages_task_loop, config))
    # loop.create_task()
