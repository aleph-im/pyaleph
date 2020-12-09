
import logging

import aioipfs

API = None
LOGGER = logging.getLogger("IPFS")

async def get_base_url(config):
    return 'http://{}:{}'.format(config.ipfs.host.value,
                                 config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return 'http://{}:{}/ipfs/{}'.format(
        config.ipfs.host.value,
        config.ipfs.gateway_port.value, hash)


async def get_ipfs_api(timeout=60, reset=False):
    global API
    if API is None or reset:
        from aleph.web import app
        host = app['config'].ipfs.host.value
        port = app['config'].ipfs.port.value

        API = aioipfs.AsyncIPFS(host=host, port=port,
                                read_timeout=timeout,
                                conns_max=100)

    return API

async def connect_ipfs_peer(peer):
    api = await get_ipfs_api(timeout=5)
    result = await api.swarm.connect(peer)
    return result
