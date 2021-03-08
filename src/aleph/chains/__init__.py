import logging
from typing import Coroutine, List

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
    from aleph.chains import substrate
except (ModuleNotFoundError, ImportError) as error:
    logger.warning("Can't load DOT: %s", error.msg)

try:
    from aleph.chains import cosmos
except ModuleNotFoundError as error:
    logger.warning("Can't load CSDK: %s", error.msg)

try:
    from aleph.chains import solana
except ModuleNotFoundError as error:
    logger.warning("Can't load SOL: %s", error.msg)

try:
    from aleph.chains import avalanche
except ModuleNotFoundError as error:
    logger.warning("Can't load AVAX: %s", error.msg)


def connector_tasks(config, outgoing=True) -> List[Coroutine]:
    tasks: List[Coroutine] = []
    for worker in INCOMING_WORKERS.values():
        tasks.append(worker(config))

    if outgoing:
        for worker in OUTGOING_WORKERS.values():
            tasks.append(worker(config))
    return tasks
