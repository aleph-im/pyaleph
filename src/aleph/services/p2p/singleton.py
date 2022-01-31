from typing import List, Optional, TypeVar

from p2pclient import Client as P2PClient

from .protocol import AlephProtocol

client: Optional[P2PClient] = None
streamer: Optional[AlephProtocol] = None
api_servers: Optional[List[str]] = None

T = TypeVar("T")


def _get_singleton(singleton: Optional[T], name: str) -> T:
    if singleton is None:
        raise ValueError(f"{name} is null!")
    return singleton


def get_p2p_client() -> P2PClient:
    return _get_singleton(client, "P2P client")


def get_streamer() -> AlephProtocol:
    return _get_singleton(streamer, "Streamer")
