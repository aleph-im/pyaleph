import logging

import aioipfs
from configmanager import Config


async def get_base_url(config):
    return "http://{}:{}".format(config.ipfs.host.value, config.ipfs.port.value)


def make_ipfs_client(
    host: str,
    port: int,
    scheme: str = "http",
    debug_level: int = logging.WARNING,
) -> aioipfs.AsyncIPFS:
    # We do not pass a read_timeout: aioipfs 0.7.1 ignores that argument.
    # AsyncIPFS.__init__ stores it in self._read_timeout but builds its client
    # session via get_session() called with no arguments, so the value never
    # reaches aiohttp. The effective timeouts are aioipfs's own session
    # defaults (total=1800s / 30 min, sock_read=600s / 10 min), which are
    # generous enough for large uploads. Passing a value here would be
    # silently discarded, so we keep the surface honest by not accepting one.
    return aioipfs.AsyncIPFS(
        host=host,
        port=port,
        scheme=scheme,
        conns_max=25,
        conns_max_per_host=10,
        debug=debug_level,
    )


def make_ipfs_p2p_client(config: Config) -> aioipfs.AsyncIPFS:
    """Create IPFS client for P2P operations (pubsub, content retrieval)."""
    # Always use main IPFS config for P2P operations
    host = config.ipfs.host.value
    port = int(config.ipfs.port.value)
    scheme = config.ipfs.scheme.value
    debug_level = config.logging.level.value <= logging.DEBUG

    return make_ipfs_client(host, port, scheme, debug_level)


def make_ipfs_pinning_client(config: Config) -> aioipfs.AsyncIPFS:
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

    debug_level = config.logging.level.value <= logging.DEBUG

    return make_ipfs_client(host, port, scheme, debug_level)


def get_cid_version(ipfs_hash: str) -> int:
    if ipfs_hash.startswith("Qm") and 44 <= len(ipfs_hash) <= 46:  # CIDv0
        return 0

    if ipfs_hash.startswith("bafy") and len(ipfs_hash) == 59:  # CIDv1
        return 1

    raise ValueError(f"Not a IPFS hash: '{ipfs_hash}'.")
