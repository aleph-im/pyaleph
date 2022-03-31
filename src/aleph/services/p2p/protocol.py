import asyncio
import base64
import json
import logging
import random
from typing import Any, Dict, Optional, Set

from anyio.abc import SocketStream
from anyio.exceptions import IncompleteRead
from p2pclient import Client as P2PClient
from p2pclient.datastructures import StreamInfo
from p2pclient.exceptions import ControlFailure
from p2pclient.libp2p_stubs.peer.id import ID

from aleph import __version__
from aleph.exceptions import AlephStorageException, InvalidMessageError
from aleph.network import incoming_check
from aleph.services.utils import pubsub_msg_to_dict
from .pubsub import receive_pubsub_messages, subscribe

MAX_READ_LEN = 2 ** 32 - 1

LOGGER = logging.getLogger("P2P.protocol")

STREAM_COUNT = 5

HELLO_PACKET = {"command": "hello"}

CONNECT_LOCK = asyncio.Lock()


class AlephProtocol:
    p2p_client: P2PClient
    PROTOCOL_ID = "/aleph/p2p/0.1.0"

    def __init__(self, p2p_client: P2PClient, streams_per_host: int = 5):
        self.p2p_client = p2p_client
        self.streams_per_host = streams_per_host
        self.peers: Set[ID] = set()

    @classmethod
    async def create(
        cls, p2p_client: P2PClient, streams_per_host: int = 5
    ) -> "AlephProtocol":
        """
        Creates a new protocol instance. This factory coroutine must be called instead of calling the constructor
        directly in order to register the stream handlers.
        """
        protocol = cls(p2p_client=p2p_client, streams_per_host=streams_per_host)
        await p2p_client.stream_handler(cls.PROTOCOL_ID, cls.stream_request_handler)
        return protocol

    @staticmethod
    async def stream_request_handler(
        stream_info: StreamInfo, stream: SocketStream
    ) -> None:
        """
        Handles the reception of a message from another peer under the aleph protocol.

        Receives a message, performs the corresponding action and returns a result message to the sender.
        """

        from aleph.storage import get_hash_content

        read_bytes = await stream.receive_some(MAX_READ_LEN)
        if read_bytes is None:
            return

        result: Dict[str, Any]

        try:
            read_string = read_bytes.decode("utf-8")
            message_json = json.loads(read_string)
            if message_json["command"] == "hash_content":
                try:
                    content = await get_hash_content(
                        message_json["hash"], use_network=False, timeout=1
                    )
                except AlephStorageException:
                    result = {"status": "success", "content": None}
                else:
                    result = {
                        "status": "success",
                        "hash": message_json["hash"],
                        "content": base64.encodebytes(content.value).decode("utf-8"),
                    }
            elif message_json["command"] == "get_message":
                result = {"status": "error", "reason": "not implemented"}
            elif message_json["command"] == "publish_message":
                result = {"status": "error", "reason": "not implemented"}
            elif message_json["command"] == "hello":
                result = {
                    "status": "success",
                    "content": {"version": __version__},
                }
            else:
                result = {"status": "error", "reason": "unknown command"}
            LOGGER.debug(f"received {read_string}")
        except Exception as e:
            result = {"status": "error", "reason": repr(e)}
            LOGGER.exception("Error while reading data")

        await stream.send_all(json.dumps(result).encode("utf-8"))

    async def make_request(
        self, request_structure: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        peers = list(self.peers)
        # Randomize the list of peers to contact to distribute the load evenly
        random.shuffle(peers)

        for peer in peers:
            stream_info, stream = await self.p2p_client.stream_open(
                peer, (self.PROTOCOL_ID,)
            )
            msg = json.dumps(request_structure).encode("UTF-8")
            try:
                await stream.send_all(msg)
                response = await stream.receive_some(MAX_READ_LEN)
            finally:
                await stream.close()

            try:
                value = json.loads(response.decode("UTF-8"))
            except json.JSONDecodeError:
                logging.warning("Could not decode response from %s", peer)
                continue

            if value.get("content") is not None:
                return value

        logging.info("Could not retrieve content from any peer")
        return None

    async def request_hash(self, item_hash) -> Optional[bytes]:
        # this should be done better, finding best peers to query from.
        query = {"command": "hash_content", "hash": item_hash}
        item = await self.make_request(query)
        if (
            item is not None
            and item["status"] == "success"
            and item["content"] is not None
        ):
            # TODO: IMPORTANT /!\ verify the hash of received data!
            return base64.decodebytes(item["content"].encode("utf-8"))
        else:
            LOGGER.debug(f"can't get hash {item_hash}")
            return None

    async def add_peer(self, peer_id: ID) -> None:
        if peer_id not in self.peers:

            try:
                stream_info, stream = await self.p2p_client.stream_open(
                    peer_id, [self.PROTOCOL_ID]
                )
            except ControlFailure as error:
                LOGGER.debug("failed to add new peer %s, error %s", peer_id, error)
                return

            try:
                await stream.send_all(json.dumps(HELLO_PACKET).encode("utf-8"))
                _ = await stream.receive_some(MAX_READ_LEN)
            except Exception as error:
                LOGGER.debug("failed to add new peer %s, error %s", peer_id, error)
                return
            finally:
                await stream.close()

            self.peers.add(peer_id)


async def incoming_channel(p2p_client: P2PClient, topic: str) -> None:
    LOGGER.debug("incoming channel started...")
    from aleph.chains.common import delayed_incoming

    stream = await subscribe(p2p_client, topic)

    # The communication with the P2P daemon sometimes fails repeatedly, spamming
    # IncompleteRead exceptions. We still want to log these to Sentry without sending
    # thousands of logs.
    incomplete_read_threshold = 150
    incomplete_read_counter = 0

    while True:
        try:
            async for pubsub_message in receive_pubsub_messages(stream):
                try:
                    msg_dict = pubsub_msg_to_dict(pubsub_message)
                    LOGGER.debug("Received from P2P:", msg_dict)
                    # we should check the sender here to avoid spam
                    # and such things...
                    try:
                        message = await incoming_check(msg_dict)
                    except InvalidMessageError:
                        continue

                    LOGGER.debug("New message %r" % message)
                    await delayed_incoming(message)
                except Exception:
                    LOGGER.exception("Can't handle message")

        except IncompleteRead:
            if (incomplete_read_counter % incomplete_read_threshold) == 0:
                LOGGER.exception(
                    "Incomplete read (%d times), reconnecting. Try to restart the application.",
                    incomplete_read_counter,
                )
            incomplete_read_counter += 1

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")

        await asyncio.sleep(2)
