"""Tests to validate the sending and receiving of messages on pubsub P2P topics."""

import asyncio
from typing import Tuple

import pytest
from p2pclient import Client as P2PClient

from aleph.services.p2p.pubsub import publish, receive_pubsub_messages, subscribe


@pytest.mark.skip("Will not work anymore until P2P daemon is upgraded")
@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [2], indirect=True)
async def test_pubsub(p2p_clients: Tuple[P2PClient, P2PClient]):
    topic = "test-topic"

    client1, client2 = p2p_clients
    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()
    await client2.connect(client1_peer_id, client1_maddrs)

    # Check that the peers are connected
    assert client1_peer_id in [peer.peer_id for peer in await client2.list_peers()]
    assert client2_peer_id in [peer.peer_id for peer in await client1.list_peers()]

    # TODO: without this sleep, the test hangs randomly. Figure out why.
    await asyncio.sleep(1)

    stream = await subscribe(client2, topic)

    msg = "Hello, peer"
    await publish(client1, topic, msg)

    received_msg = await asyncio.wait_for(receive_pubsub_messages(stream).__anext__(), timeout=10)
    print(received_msg)
    assert received_msg.data.decode("UTF-8") == msg
    assert topic in received_msg.topicIDs


@pytest.mark.skip("Will not work anymore until P2P daemon is upgraded")
@pytest.mark.asyncio
@pytest.mark.parametrize("p2p_clients", [3], indirect=True)
async def test_pubsub_multiple_subscribers(p2p_clients):
    """
    Tests that a pubsub message can go through several peers to reach its destination.
    Note that the peers must all subscribe to the topic in order to publish the message to the other peers.
    The connection in this test is: Peer #3 -> Peer #1 -> Peer #2.
    """
    topic = "test-topic-multi"
    client1, client2, client3 = p2p_clients
    client1_peer_id, client1_maddrs = await client1.identify()
    client2_peer_id, client2_maddrs = await client2.identify()
    client3_peer_id, client3_maddrs = await client3.identify()
    await client2.connect(client1_peer_id, client1_maddrs)
    await client3.connect(client1_peer_id, client1_maddrs)

    # TODO: without this sleep, the test hangs randomly. Figure out why.
    await asyncio.sleep(1)

    # Check that the peers are connected
    assert {client2_peer_id, client3_peer_id}.issubset(peer.peer_id for peer in await client1.list_peers())
    assert client1_peer_id in [peer.peer_id for peer in await client2.list_peers()]
    assert client1_peer_id in [peer.peer_id for peer in await client3.list_peers()]

    stream_client1 = await subscribe(client1, topic)
    stream_client2 = await subscribe(client2, topic)
    msg = "Hello, distant peer"
    await publish(client3, topic, msg)

    # Check that the neighboring peer received the message
    received_msg_client1 = await asyncio.wait_for(receive_pubsub_messages(stream_client1).__anext__(), timeout=10)
    assert received_msg_client1.data.decode("UTF-8") == msg
    assert topic in received_msg_client1.topicIDs

    # Check that the distant peer also received the message
    received_msg_client2 = await asyncio.wait_for(receive_pubsub_messages(stream_client2).__anext__(), timeout=10)
    assert received_msg_client2.data.decode("UTF-8") == msg
    assert topic in received_msg_client2.topicIDs
