import asyncio
import json
import logging
from urllib.parse import unquote

from aleph_p2p_client import AlephP2PServiceClient

from aleph.db.accessors.peers import upsert_peer
from aleph.db.models import PeerType
from aleph.services.ipfs import IpfsService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory

LOGGER = logging.getLogger(__name__)


async def handle_incoming_host(
    session_factory: DbSessionFactory,
    data: bytes,
    sender: str,
    source: PeerType,
):
    try:
        LOGGER.debug("New message received from %s", sender)
        message_data = data.decode("utf-8")
        content = json.loads(unquote(message_data))

        # TODO: replace this validation by marshaling (ex: Pydantic)
        peer_type = PeerType(content["peer_type"])
        if not isinstance(content["address"], str):
            raise ValueError("Bad address")
        if not isinstance(content["peer_type"], str):
            raise ValueError("Bad peer type")

        # TODO: handle interests and save it

        with session_factory() as session:
            upsert_peer(
                session=session,
                peer_id=sender,
                peer_type=peer_type,
                address=content["address"],
                source=source,
                last_seen=utc_now(),
            )
            session.commit()

    except Exception as e:
        if isinstance(e, ValueError):
            LOGGER.info("Received a bad peer info %s from %s" % (e.args[0], sender))
        else:
            LOGGER.exception("Exception in pubsub peers monitoring")


async def monitor_hosts_p2p(
    p2p_client: AlephP2PServiceClient,
    session_factory: DbSessionFactory,
    alive_topic: str,
) -> None:
    while True:
        try:
            await p2p_client.subscribe(alive_topic)
            async for alive_message in p2p_client.receive_messages(alive_topic):
                protocol, topic, peer_id = alive_message.routing_key.split(".")
                await handle_incoming_host(
                    session_factory=session_factory,
                    data=alive_message.body,
                    sender=peer_id,
                    source=PeerType.P2P,
                )

        except Exception:
            LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")

        await asyncio.sleep(2)


async def monitor_hosts_ipfs(
    ipfs_service: IpfsService, session_factory: DbSessionFactory, alive_topic: str
):
    while True:
        try:
            async for message in ipfs_service.sub(alive_topic):
                await handle_incoming_host(
                    session_factory=session_factory,
                    data=message["data"],
                    sender=message["from"],
                    source=PeerType.IPFS,
                )
        except Exception:
            LOGGER.exception("Exception in pubsub peers monitoring, resubscribing")
