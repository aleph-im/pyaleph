"""Tests to validate the connection / reconnection features of the P2P service."""

from typing import Tuple

import pytest
from p2pclient import Client as P2PClient
from aleph.services.p2p.peers import connect_peer
from aleph.services.p2p.protocol import AlephProtocol


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_p2p_client_connect(p2p_clients: Tuple[P2PClient, P2PClient]):
    """
    Sanity check: verify that connecting two peers makes each peer appear in the peer list of the other peer.
    This test is redundant with some tests in p2pclient itself.
    """
    client1, client2 = p2p_clients
    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()
    await client2.connect(client1_peer_id, client1_maddrs)

    client1_peers = await client1.list_peers()
    client2_peers = await client2.list_peers()
    assert client1_peer_id in [peer.peer_id for peer in client2_peers]
    assert client2_peer_id in [peer.peer_id for peer in client1_peers]


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_connect_peer_no_streamer(p2p_clients: Tuple[P2PClient, P2PClient]):
    """
    Checks that we can connect to a peer using the custom connect_peer function, without managing protocol connections.
    """
    client1, client2 = p2p_clients
    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()

    peer_maddr = f"{client2_maddrs[0]}/p2p/{client2_peer_id}"

    await connect_peer(p2p_client=client1, streamer=None, peer_maddr=peer_maddr)

    assert client1_peer_id in [peer.peer_id for peer in await client2.list_peers()]
    assert client2_peer_id in [peer.peer_id for peer in await client1.list_peers()]


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_connect_peer_streamer(p2p_clients: Tuple[P2PClient, P2PClient]):
    """
    Checks that we can connect to a peer using the custom connect_peer function while managing protocol connections.
    """

    client1, client2 = p2p_clients
    streamer_client1 = await AlephProtocol.create(client1)
    # The receiver must have a handler registered for the protocol to be able to open a stream.
    # The handler is registered when creating the protocol streamer instance.
    streamer_client2 = await AlephProtocol.create(client2)

    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()

    peer_maddr = f"{client2_maddrs[0]}/p2p/{client2_peer_id}"

    await connect_peer(p2p_client=client1, streamer=streamer_client1, peer_maddr=peer_maddr)

    assert client1_peer_id in [peer.peer_id for peer in await client2.list_peers()]
    assert client2_peer_id in [peer.peer_id for peer in await client1.list_peers()]

    # Check that the peer was added successfully
    assert client2_peer_id in streamer_client1.peers


@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [1], indirect=True)
async def test_connect_to_self(p2p_clients: Tuple[P2PClient]):
    """Checks that nothing bad happens if we try to connect to ourselves."""
    client = p2p_clients[0]
    peer_id, maddrs = await client.identify()

    await connect_peer(p2p_client=client, streamer=None, peer_maddr=f"/p2p/{peer_id}")
