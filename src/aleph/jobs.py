from logging import getLogger
import aiocron
import asyncio
from datetime import date, time, timezone, datetime, timedelta
from aleph.chains.common import incoming
from aleph.model.pending import PendingMessage
from pymongo import DeleteOne

LOGGER = getLogger("JOBS")

RETRY_LOCK = asyncio.Lock()

@aiocron.crontab('*/30 * * * *', start=False)
async def retry_job():
    """ Each 30 minutes, try to handle message that were added to the
    pending queue (Unavailable messages)."""

    col = PendingMessage.collection
    if RETRY_LOCK.locked():
        # Don't do the work twice.
        return

    async with RETRY_LOCK:
        actions = []
        async for pending in col.find():
            result = await incoming(
                pending['message'],
                chain_name=pending['source'].get('chain_name'),
                tx_hash=pending['source'].get('tx_hash'),
                height=pending['source'].get('height'),
                check_message=pending['source'].get('check_message', True),
                retrying=True)

            if result is True:  # message handled (valid or not, we don't care)
                actions.append(DeleteOne({'_id': pending['_id']}))

        if len(actions):
            await col.bulk_write(actions)


def start_jobs():
    retry_job.start()
