import asyncio
import logging
from aleph.chains.register import OUTGOING_WORKERS, INCOMING_WORKERS

logger = logging.getLogger(__name__)

try:
    from aleph.chains import nuls
except ModuleNotFoundError as error:
    logger.warning("Can't load NULS: %s", error.msg)
try:
    from aleph.chains import nuls2
except ModuleNotFoundError as error:
    logger.warning("Can't load NULS2: %s", error.msg)
try:
    from aleph.chains import ethereum
except ModuleNotFoundError as error:
    logger.warning("Can't load ETH: %s", error.msg)
try:
    from aleph.chains import binance
except ModuleNotFoundError as error:
    logger.warning(f"Can't load BNB: %s", error.msg)
try:
    from aleph.chains import neo
except ModuleNotFoundError as error:
    logger.warning("Can't load NEO: %s", error.msg)

try:
    from aleph.chains import substrate
except (ModuleNotFoundError, ImportError) as error:
    logger.warning("Can't load DOT: %s", error.msg)

try:
    from aleph.chains import cosmos
except ModuleNotFoundError as error:
    logger.warning("Can't load CSDK: %s", error.msg)


def start_connector(config, outgoing=True):
    loop = asyncio.get_event_loop()

    for worker in INCOMING_WORKERS.values():
        loop.create_task(worker(config))

    if outgoing:
        for worker in OUTGOING_WORKERS.values():
            loop.create_task(worker(config))
