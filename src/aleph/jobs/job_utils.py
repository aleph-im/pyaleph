import asyncio
from typing import Dict


def prepare_loop(config_values: Dict) -> asyncio.AbstractEventLoop:
    from aleph.model import init_db
    from aleph.web import app
    from configmanager import Config
    from aleph.config import get_defaults
    from aleph.services.ipfs.common import get_ipfs_api
    from aleph.services.p2p import http, init_p2p_client

    http.SESSION = None  # type:ignore

    loop = asyncio.get_event_loop()

    config = Config(schema=get_defaults())
    app["config"] = config
    config.load_values(config_values)

    init_db(config, ensure_indexes=False)
    loop.run_until_complete(get_ipfs_api(timeout=2, reset=True))
    _ = init_p2p_client(config)
    return loop
