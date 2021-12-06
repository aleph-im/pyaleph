import logging

import aioipfs

from aleph.services.utils import get_IP

API = None
LOGGER = logging.getLogger("IPFS")


async def get_base_url(config):
    return "http://{}:{}".format(config.ipfs.host.value, config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return "http://{}:{}/ipfs/{}".format(
        config.ipfs.host.value, config.ipfs.gateway_port.value, hash
    )


async def get_ipfs_api(timeout=5, reset=False):
    global API
    if API is None or reset:
        from aleph.web import app

        host = app["config"].ipfs.host.value
        port = app["config"].ipfs.port.value

        API = aioipfs.AsyncIPFS(
            host=host,
            port=port,
            read_timeout=timeout,
            conns_max=25,
            conns_max_per_host=10,
            debug=(app["config"]["logging"]["level"] <= logging.DEBUG)
        )

    return API


async def connect_ipfs_peer(peer):
    api = await get_ipfs_api(timeout=5)
    result = await api.swarm.connect(peer)
    return result


async def get_public_address():
    api = await get_ipfs_api()
    public_ip = await get_IP()

    addresses = (await api.id())["Addresses"]
    for address in addresses:
        if public_ip in address and "/tcp" in address and "/p2p" in address:
            return address

    # Fallback to first possible public...
    for address in addresses:
        if "127.0.0.1" not in address and "/tcp" in address and "/p2p" in address:
            return address

    # Still no public there, try ourselves.
    for address in addresses:
        if "127.0.0.1" in address and "/tcp" in address and "/p2p" in address:
            return address.replace("127.0.0.1", public_ip)
