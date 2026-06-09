import grpc
import grpc.aio
import pytest
import pytest_asyncio

from aleph.services.p2p.client import (
    DialFailedException,
    DialWrongPeerException,
    P2PGrpcClient,
)
from aleph.services.p2p.grpc_generated import aleph_p2p_pb2 as pb
from aleph.services.p2p.grpc_generated import aleph_p2p_pb2_grpc as pb_grpc

LOCAL_PEER_ID = "QmLocalTestPeer"


class FakeP2PServicer(pb_grpc.AlephP2PServicer):
    def __init__(self):
        self.published = []
        self.preferred = []
        self.dialed = []

    async def Identify(self, request, context):
        return pb.IdentifyResponse(
            peer_id=LOCAL_PEER_ID,
            listen_multiaddrs=["/ip4/127.0.0.1/tcp/4025"],
            external_multiaddrs=[],
        )

    async def Dial(self, request, context):
        if request.peer_id == "QmWrongPeer":
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "peer id mismatch")
        if request.peer_id == "QmUnreachable":
            await context.abort(grpc.StatusCode.NOT_FOUND, "could not reach peer")
        self.dialed.append((request.peer_id, request.multiaddr))
        return pb.DialResponse()

    async def Publish(self, request, context):
        self.published.append((request.topic, request.payload, request.echo))
        return pb.PublishResponse()

    async def Subscribe(self, request, context):
        for i in range(3):
            yield pb.PubsubEnvelope(
                topic=request.topic,
                source_peer_id="QmRemotePeer",
                payload=b"message-%d" % i,
                received_at_millis=1000 + i,
            )

    async def SetPreferredPeers(self, request, context):
        self.preferred = [(p.peer_id, list(p.multiaddrs)) for p in request.peers]
        return pb.SetPreferredPeersResponse(accepted=len(request.peers), truncated=0)

    async def GetPeers(self, request, context):
        return pb.GetPeersResponse(
            peers=[
                pb.PeerInfo(
                    peer_id="QmRemotePeer",
                    multiaddrs=["/ip4/10.0.0.1/tcp/4025"],
                    preferred=True,
                    score=100.0,
                )
            ]
        )


@pytest_asyncio.fixture
async def fake_service():
    server = grpc.aio.server()
    servicer = FakeP2PServicer()
    pb_grpc.add_AlephP2PServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    yield "127.0.0.1", port, servicer
    await server.stop(None)


@pytest_asyncio.fixture
async def client(fake_service):
    host, port, _servicer = fake_service
    client = await P2PGrpcClient.connect(host=host, port=port)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_connect_fetches_local_peer_id(client):
    assert client.peer_id == LOCAL_PEER_ID


@pytest.mark.asyncio
async def test_identify_returns_node_info(client):
    node_info = await client.identify()
    assert node_info.peer_id == LOCAL_PEER_ID
    assert node_info.listen_multiaddrs == ["/ip4/127.0.0.1/tcp/4025"]


@pytest.mark.asyncio
async def test_publish_forwards_topic_payload_and_echo(client, fake_service):
    _, _, servicer = fake_service
    await client.publish(data=b"hello", topic="ALEPH-TEST", echo=True)
    assert servicer.published == [("ALEPH-TEST", b"hello", True)]


@pytest.mark.asyncio
async def test_receive_messages_yields_envelopes(client):
    received = []
    async for message in client.receive_messages("ALEPH-TEST"):
        received.append(message)
    assert len(received) == 3
    assert received[0].sender == "QmRemotePeer"
    assert received[0].data == b"message-0"
    assert received[0].topic == "ALEPH-TEST"


@pytest.mark.asyncio
async def test_dial_success(client, fake_service):
    _, _, servicer = fake_service
    await client.dial(peer_id="QmSomePeer", multiaddr="/ip4/10.0.0.2/tcp/4025")
    assert servicer.dialed == [("QmSomePeer", "/ip4/10.0.0.2/tcp/4025")]


@pytest.mark.asyncio
async def test_dial_wrong_peer_raises(client):
    with pytest.raises(DialWrongPeerException):
        await client.dial(peer_id="QmWrongPeer", multiaddr="/ip4/10.0.0.2/tcp/4025")


@pytest.mark.asyncio
async def test_dial_unreachable_raises(client):
    with pytest.raises(DialFailedException):
        await client.dial(peer_id="QmUnreachable", multiaddr="/ip4/10.0.0.2/tcp/4025")


@pytest.mark.asyncio
async def test_set_preferred_peers(client, fake_service):
    _, _, servicer = fake_service
    accepted, truncated = await client.set_preferred_peers(
        [("QmCcn1", ["/ip4/10.0.0.3/tcp/4025"]), ("QmCcn2", [])]
    )
    assert accepted == 2
    assert truncated == 0
    assert servicer.preferred == [
        ("QmCcn1", ["/ip4/10.0.0.3/tcp/4025"]),
        ("QmCcn2", []),
    ]


@pytest.mark.asyncio
async def test_get_peers(client):
    peers = await client.get_peers()
    assert len(peers) == 1
    assert peers[0].peer_id == "QmRemotePeer"
    assert peers[0].preferred is True
