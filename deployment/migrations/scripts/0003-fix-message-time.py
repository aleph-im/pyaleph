"""
This migration fixes the "time" field of messages. It was incorrectly updated when fetching
messages from the on-chain storage.

We now store the original message time in the "time" field, and we store the confirmation time
in the confirmations array. To achieve this, we must fetch the whole message history and
process it again.
"""

import logging

from configmanager import Config

from aleph.model.chains import Chain
from aleph.model.messages import Message

logger = logging.getLogger()


async def must_run_migration() -> bool:
    nb_documents = Message.collection.count_documents(
        filter={"content.time": {"$exists": 1}, "$expr": {"$ne": ["$time", "$content.time"]}}
    )
    return bool(nb_documents)


async def upgrade(config: Config, **kwargs):
    if await must_run_migration():
        logger.info("Messages with inconsistent times found, running migration.")
        start_height = config.ethereum.start_height.value
        await Chain.set_last_height("ETH", start_height)
    else:
        logger.info("Message times already set to the correct value, skipping migration.")

    logger.info("Some queries may take a while to execute.")

    # First, update all the messages that have a valid content.time field.
    # This represents 99.99% of the messages in the DB, the only exception
    # being forgotten messages.
    filter = {"content.time": {"$exists": 1}}
    update = [{"$set": {"time": "$content.time"}}]

    logger.info("Resetting the time field on messages. This operation may take a while.")
    await Message.collection.update_many(filter=filter, update=update)
    logger.info("Reset message times to their original value.")


def downgrade(**kwargs):
    # Nothing to do, we do not wish to revert this migration.
    pass
