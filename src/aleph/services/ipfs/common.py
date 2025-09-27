import logging

import aioipfs
from configmanager import Config


async def get_base_url(config):
    return "http://{}:{}".format(config.ipfs.host.value, config.ipfs.port.value)


def make_ipfs_client(
    host: str,
    port: int,
    timeout: int = 60,
    scheme: str = "http",
    debug_level: int = logging.WARNING,
) -> aioipfs.AsyncIPFS:
    return aioipfs.AsyncIPFS(
        host=host,
        port=port,
        scheme=scheme,
        read_timeout=timeout,
        conns_max=25,
        conns_max_per_host=10,
        debug=debug_level,
    )


def make_ipfs_p2p_client(config: Config, timeout: int = 60) -> aioipfs.AsyncIPFS:
    """Create IPFS client for P2P operations (pubsub, content retrieval)."""
    # Always use main IPFS config for P2P operations
    host = config.ipfs.host.value
    port = int(config.ipfs.port.value)
    scheme = config.ipfs.scheme.value
    debug_level = config.logging.level.value <= logging.DEBUG

    return make_ipfs_client(host, port, timeout, scheme, debug_level)


def make_ipfs_pinning_client(config: Config, timeout: int = 60) -> aioipfs.AsyncIPFS:
    """Create IPFS client for pinning operations."""
    # Use pinning specific config if provided, otherwise use main config
    if (
        hasattr(config.ipfs, "pinning")
        and config.ipfs.pinning.host.value
        and config.ipfs.pinning.port.value
    ):
        host = config.ipfs.pinning.host.value
        port = int(config.ipfs.pinning.port.value)
        scheme = config.ipfs.pinning.scheme.value
    else:
        # Use main IPFS config as fallback
        host = config.ipfs.host.value
        port = int(config.ipfs.port.value)
        scheme = config.ipfs.scheme.value

    # Get pinning-specific timeout if available
    if hasattr(config.ipfs, "pinning") and hasattr(config.ipfs.pinning, "timeout"):
        timeout = int(config.ipfs.pinning.timeout.value)

    debug_level = config.logging.level.value <= logging.DEBUG

    return make_ipfs_client(host, port, timeout, scheme, debug_level)


def get_cid_version(ipfs_hash: str) -> int:
    if ipfs_hash.startswith("Qm") and 44 <= len(ipfs_hash) <= 46:  # CIDv0
        return 0

    if ipfs_hash.startswith("bafy") and len(ipfs_hash) == 59:  # CIDv1
        return 1

    raise ValueError(f"Not a IPFS hash: '{ipfs_hash}'.")
