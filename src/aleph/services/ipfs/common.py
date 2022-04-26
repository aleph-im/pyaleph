import logging

import aioipfs
import aleph.config
from aleph.services.utils import get_IP
from configmanager import Config

API = None
LOGGER = logging.getLogger("IPFS")


async def get_base_url(config):
    return "http://{}:{}".format(config.ipfs.host.value, config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return "http://{}:{}/ipfs/{}".format(
        config.ipfs.host.value, config.ipfs.gateway_port.value, hash
    )


def init_ipfs_globals(config: Config, timeout: int = 5) -> None:
    global API

    host = config.ipfs.host.value
    port = config.ipfs.port.value

    API = aioipfs.AsyncIPFS(
        host=host,
        port=port,
        read_timeout=timeout,
        conns_max=25,
        conns_max_per_host=10,
        debug=(config.logging.level.value <= logging.DEBUG)
    )


async def get_ipfs_api(timeout: int = 5, reset: bool = False):
    global API
    if API is None or reset:
        init_ipfs_globals(aleph.config.get_config(), timeout)

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


def get_cid_version(ipfs_hash: str) -> int:
    if ipfs_hash.startswith("Qm") and 44 <= len(ipfs_hash) <= 46:  # CIDv0
        return 0

    if ipfs_hash.startswith("bafy") and len(ipfs_hash) == 59:  # CIDv1
        return 1

    raise ValueError(f"Not a IPFS hash: '{ipfs_hash}'.")
