"""
This migration resets the chain height in order to ensure that a PermanentPin is present for every chaindata
committed on a chain. It has to be run after the introduction of PermanentPin in version 0.2.0.
"""

import asyncio
import logging
from typing import Optional

from configmanager import Config

from aleph.config import get_defaults
from aleph.model import PermanentPin, init_db
from aleph.model.chains import Chain

logger = logging.getLogger()


async def async_upgrade(config_file: Optional[str], **kwargs):
    config = Config(schema=get_defaults())
    if config_file is not None:
        config.yaml.load(config_file)

    init_db(config=config, ensure_indexes=False)

    # We measure over 5000 permanent pins on new nodes that did process all chaindata.
    # We therefore use this value to estimate if a node did process all chaindata already or not.
    expected_permanent_pins = 5000

    if (await PermanentPin.count(filter={})) < expected_permanent_pins:
        logger.info("PermanentPin documents missing, fetching chaindata again")
        start_height = config.ethereum.start_height.value
        await Chain.set_last_height("ETH", start_height)
    else:
        logger.info(
            "PermanentPin documents already present, no need to re-fetch chaindata"
        )


def upgrade(config_file: str, **kwargs):
    asyncio.run(async_upgrade(config_file=config_file, **kwargs))


def downgrade(**kwargs):
    # Nothing to do, processing the chain data multiple times only adds some load on the node.
    pass
