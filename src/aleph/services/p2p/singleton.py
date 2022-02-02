from typing import Any, List, Optional

from p2pclient import Client as P2PClient

client: Optional[P2PClient] = None
# TODO: use the correct type once the circular dependency is resolved (next PR)
streamer: Optional[Any] = None
api_servers: Optional[List[str]] = None


def get_p2p_client() -> P2PClient:
    if client is None:
        raise ValueError("Client is null!")
    return client
