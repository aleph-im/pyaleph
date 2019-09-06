from logging import getLogger
import asyncio
from aleph.chains.common import incoming, get_chaindata_messages
from aleph.model.pending import PendingMessage, PendingTX
from aleph.model.messages import Message
from pymongo import DeleteOne, InsertOne, DeleteMany

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
    i = 0
    while await PendingMessage.collection.count_documents({}):
        async for pending in PendingMessage.collection.find().sort([('message.time', 1)]).limit(20000):
            i += 1
            tasks.append(asyncio.shield(handle_pending_message(pending, seen_ids, actions, messages_actions)))

            if (i >= 2000):
                await join_pending_message_tasks(tasks, actions, messages_actions)
                i = 0
        

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

    await join_pending_message_tasks(tasks, actions, messages_actions)


async def retry_messages_task():
    while True:
        try:
            await retry_messages_job()
        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        await asyncio.sleep(.01)
        

async def handle_pending_tx(pending, actions_list):
    messages = await get_chaindata_messages(pending['content'], pending['context'])
    LOGGER.info("%s Handling TX in block %s" % (pending['context']['chain_name'], pending['context']['height']))
    if isinstance(messages, list):
        message_actions = list()
        for message in messages:
            message['time'] = pending['context']['time']
            
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

    actions = []
    tasks = []
    i = 0
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
            LOGGER.info("handling TXs")
            await handle_txs_job()
        except Exception:
            LOGGER.exception("Error in pending txs job")

        await asyncio.sleep(0.01)

def start_jobs():
    LOGGER.info("starting jobs")
    loop = asyncio.get_event_loop()
    loop.create_task(retry_messages_task())
    loop.create_task(handle_txs_task())
