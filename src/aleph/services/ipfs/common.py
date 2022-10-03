import logging

import aioipfs
from configmanager import Config


async def get_base_url(config):
    return "http://{}:{}".format(config.ipfs.host.value, config.ipfs.port.value)


async def get_ipfs_gateway_url(config, hash):
    return "http://{}:{}/ipfs/{}".format(
        config.ipfs.host.value, config.ipfs.gateway_port.value, hash
    )


def make_ipfs_client(config: Config, timeout: int = 60) -> aioipfs.AsyncIPFS:
    host = config.ipfs.host.value
    port = config.ipfs.port.value

    return aioipfs.AsyncIPFS(
        host=host,
        port=port,
        read_timeout=timeout,
        conns_max=25,
        conns_max_per_host=10,
        debug=(config.logging.level.value <= logging.DEBUG),
    )


def get_cid_version(ipfs_hash: str) -> int:
    if ipfs_hash.startswith("Qm") and 44 <= len(ipfs_hash) <= 46:  # CIDv0
        return 0

    if ipfs_hash.startswith("bafy") and len(ipfs_hash) == 59:  # CIDv1
        return 1

    raise ValueError(f"Not a IPFS hash: '{ipfs_hash}'.")
