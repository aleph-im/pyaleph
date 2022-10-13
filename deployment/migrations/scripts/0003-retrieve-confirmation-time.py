"""
This migration retrieves additional metadata regarding chain confirmation of messages,
including the block timestamp. We reset the TX height of the node to reprocess
all the chain data messages and insert additional values
"""


import logging
import os
from configmanager import Config
from aleph.model.chains import Chain
from aleph.model.pending import PendingMessage, PendingTX
from aleph.model.messages import Message

logger = logging.getLogger(os.path.basename(__file__))


async def upgrade(config: Config, **kwargs):
    logger.info("Resetting chain height to re-fetch all chaindata...")
    start_height = config.ethereum.start_height.value
    await Chain.set_last_height("ETH", start_height)

    logger.info("Dropping all pending transactions...")
    await PendingTX.collection.delete_many({})

    logger.info(
        "Dropping all pending confirmation messages "
        "(they will be reinserted automatically)..."
    )
    await PendingMessage.collection.delete_many({"source.chain_name": {"$ne": None}})

    logger.info("Removing confirmation data for all messages...")
    # Confirmations will be automatically added again by the pending TX processor.
    # By removing the confirmation entirely, we make sure to avoid intermediate states
    # if a message was confirmed in an unexpected way.
    await Message.collection.update_many(
        {"confirmed": True},
        {
            "$set": {
                "confirmed": False,
            },
            "$unset": {"confirmations": 1},
        },
    )


async def downgrade(**kwargs):
    raise NotImplementedError("Downgrading this migration is not supported.")
