import asyncio
from typing import Dict
from typing import Tuple

import aleph.config
from aleph.model import init_db_globals
from aleph.services.ipfs.common import init_ipfs_globals
from aleph.services.p2p import init_p2p_client
from configmanager import Config


def prepare_loop(config_values: Dict) -> Tuple[asyncio.AbstractEventLoop, Config]:
    """
    Prepares all the global variables (sigh) needed to run an Aleph subprocess.

    :param config_values: Dictionary of config values, as provided by the main process.
    :returns: A preconfigured event loop, and the application config for convenience.
              Use the event loop as event loop of the process as it is used by Motor. Using another
              event loop will cause DB calls to fail.
    """

    loop = asyncio.get_event_loop()

    config = aleph.config.app_config
    config.load_values(config_values)

    init_db_globals(config)
    init_ipfs_globals(config)
    _ = init_p2p_client(config)
    return loop, config
