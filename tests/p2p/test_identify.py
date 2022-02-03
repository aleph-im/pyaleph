from typing import Tuple

import pytest
from p2pclient import Client as P2PClient


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [1], indirect=True)
async def test_p2p_client_identify(p2p_clients: Tuple[P2PClient]):
    """Sanity check to make sure that the fixture deploys the P2P daemon and that the client can reach it."""

    assert len(p2p_clients) == 1
    client = p2p_clients[0]
    _peer_id, _maddrs = await client.identify()
