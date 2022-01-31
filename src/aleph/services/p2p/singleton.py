from typing import Optional

from p2pclient import Client as P2PClient

client: Optional[P2PClient] = None
streamer = None
api_servers = None


def get_p2p_client() -> P2PClient:
    if client is None:
        raise ValueError("Client is null!")
    return client
