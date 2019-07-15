from logging import getLogger
import asyncio
from aleph.chains.common import incoming
from aleph.model.pending import PendingMessage
from pymongo import DeleteOne

LOGGER = getLogger("JOBS")

RETRY_LOCK = asyncio.Lock()


async def handle_pending(pending, actions_list):
    result = await incoming(
        pending['message'],
        chain_name=pending['source'].get('chain_name'),
        tx_hash=pending['source'].get('tx_hash'),
        height=pending['source'].get('height'),
        check_message=pending['source'].get('check_message', True),
        retrying=True)

    if result is True:  # message handled (valid or not, we don't care)
        # Is it valid to add to a list passed this way? to be checked.
        actions_list.append(DeleteOne({'_id': pending['_id']}))


async def join_pending_message_tasks(tasks, actions_list):
    try:
        await asyncio.gather(*tasks)
    except Exception:
        LOGGER.exception("error in incoming task")
    tasks.clear()

    if len(actions_list):
        await PendingMessage.collection.bulk_write(actions_list)
        actions_list.clear()


async def retry_job():
    """ Each few minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""

    actions = []
    tasks = []
    i = 0
    async for pending in PendingMessage.collection.find():
        i += 1
        tasks.append(handle_pending(pending, actions))

        if (i > 500):
            await join_pending_message_tasks(tasks, actions)
            i = 0

    await join_pending_message_tasks(tasks, actions)


async def retry_task():
    while True:
        try:
            await retry_job()
        except Exception:
            LOGGER.exception("Error in pending messages retry job")

        await asyncio.sleep(60)


def start_jobs():
    LOGGER.info("starting jobs")
    loop = asyncio.get_event_loop()
    loop.create_task(retry_task())
