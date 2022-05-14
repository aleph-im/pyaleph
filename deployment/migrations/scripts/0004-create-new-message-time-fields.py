"""
This migration adds the `confirmation_time` and `reception_time` fields.
`confirmation_time` serves as a cache of the first confirmation message seen
in on-chain data.
`reception_time` represents the first time the node became aware of
the message, confirmed or not.
"""


import logging
import os

from configmanager import Config

from aleph.model.messages import Message

logger = logging.getLogger(os.path.basename(__file__))


async def upgrade(config: Config, **kwargs):
    logger.info("Creating confirmation_time field for messages...")
    await Message.collection.update_many(
        {"confirmed": True},
        [{"$set": {"confirmation_time": {"$min": "$confirmations.time"}}}],
    )


async def downgrade(**kwargs):
    logger.info("Creating confirmation_time field for messages...")
    await Message.collection.update_many(
        {"$unset": {"confirmation_time": 1, "reception_time": 1}}
    )
