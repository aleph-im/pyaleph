import asyncio
import json
import logging
from typing import Dict, cast, Optional, Any, Mapping, List, Collection, Union

import aio_pika.abc
from aiohttp import web
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config
from pydantic import BaseModel, Field, ValidationError

import aleph.toolkit.json as aleph_json
from aleph.schemas.pending_messages import parse_message, BasePendingMessage
from aleph.services.ipfs import IpfsService
from aleph.services.p2p.pubsub import publish as pub_p2p
from aleph.types.message_status import (
    InvalidMessageException,
    MessageStatus,
    MessageProcessingStatus,
)
from aleph.types.protocol import Protocol
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_p2p_client_from_request,
    get_mq_conn_from_request,
)
from aleph.web.controllers.utils import mq_make_aleph_message_topic_queue

LOGGER = logging.getLogger(__name__)


class PublicationStatus(BaseModel):
    status: str
    failed: List[Protocol]

    @classmethod
    def from_failures(cls, failed_publications: List[Protocol]):
        status = {
            0: "success",
            1: "warning",
            2: "error",
        }[len(failed_publications)]
        return cls(status=status, failed=failed_publications)


def _validate_message_dict(message_dict: Mapping[str, Any]) -> BasePendingMessage:
    try:
        return parse_message(message_dict)
    except InvalidMessageException as e:
        raise web.HTTPUnprocessableEntity(body=str(e))


def _validate_request_data(config: Config, request_data: Dict) -> None:
    """
    Validates the content of a JSON pubsub message depending on the channel
    and raises a 422 error if the data does not match the expected format.

    :param config: Application configuration.
    :param request_data: Request JSON data, as a dictionary.
    """

    topic = request_data.get("topic")

    # Only accept publishing on the message topic.
    message_topic = config.aleph.queue_topic.value
    if topic != message_topic:
        raise web.HTTPForbidden(reason=f"Unauthorized P2P topic: {topic}. Use {message_topic}.")

    data = request_data.get("data")
    if not isinstance(data, str):
        raise web.HTTPUnprocessableEntity(reason="'data': expected a serialized JSON string.")

    try:
        message_dict = json.loads(cast(str, request_data.get("data")))
    except ValueError:
        raise web.HTTPUnprocessableEntity(reason="'data': must be deserializable as JSON.")

    _validate_message_dict(message_dict)


async def _pub_on_p2p_topics(
    p2p_client: AlephP2PServiceClient,
    ipfs_service: Optional[IpfsService],
    topic: str,
    payload: Union[str, bytes],
) -> List[Protocol]:
    failed_publications = []

    if ipfs_service:
        try:
            await asyncio.wait_for(ipfs_service.pub(topic, payload), 10)
        except Exception:
            LOGGER.exception("Can't publish on ipfs")
            failed_publications.append(Protocol.IPFS)

    try:
        await asyncio.wait_for(
            pub_p2p(
                p2p_client,
                topic,
                payload,
                loopback=True,
            ),
            10,
        )
    except Exception:
        LOGGER.exception("Can't publish on p2p")
        failed_publications.append(Protocol.P2P)

    return failed_publications


async def pub_json(request: web.Request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    request_data = await request.json()
    _validate_request_data(
        config=get_config_from_request(request), request_data=request_data
    )

    ipfs_service = get_ipfs_service_from_request(request)
    p2p_client = get_p2p_client_from_request(request)

    failed_publications = await _pub_on_p2p_topics(
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        topic=request_data.get("topic"),
        payload=request_data.get("data"),
    )
    pub_status = PublicationStatus.from_failures(failed_publications)

    return web.json_response(
        text=pub_status.json(),
        status=500 if pub_status == "error" else 200,
    )


async def _mq_read_one_message(
    queue: aio_pika.abc.AbstractQueue, timeout: float
) -> Optional[aio_pika.abc.AbstractIncomingMessage]:
    """
    Believe it or not, this is the only way I found to
    :return:
    """
    try:
        async with queue.iterator(timeout=timeout, no_ack=True) as queue_iter:
            async for message in queue_iter:
                return message

    except asyncio.TimeoutError:
        pass

    return None


def _processing_status_to_http_status(status: MessageProcessingStatus) -> int:
    mapping = {
        MessageProcessingStatus.PROCESSED_NEW_MESSAGE: 200,
        MessageProcessingStatus.PROCESSED_CONFIRMATION: 200,
        MessageProcessingStatus.FAILED_WILL_RETRY: 202,
        MessageProcessingStatus.FAILED_REJECTED: 422,
    }
    return mapping[status]


class PubMessageRequest(BaseModel):
    sync: bool = False
    message_dict: Dict[str, Any] = Field(alias="message")


class PubMessageResponse(BaseModel):
    publication_status: PublicationStatus
    message_status: Optional[MessageStatus]


async def pub_message(request: web.Request):
    try:
        request_data = PubMessageRequest.parse_obj(await request.json())
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(body=e.json(indent=4))

    pending_message = _validate_message_dict(request_data.message_dict)

    # In sync mode, wait for a message processing event. We need to create the queue
    # before publishing the message on P2P topics in order to guarantee that the event
    # will be picked up.
    config = get_config_from_request(request)

    if request_data.sync:
        mq_conn = get_mq_conn_from_request(request)
        mq_queue = await mq_make_aleph_message_topic_queue(
            mq_conn=mq_conn, config=config, routing_key=f"*.{pending_message.item_hash}"
        )
    else:
        mq_queue = None

    # We publish the message on P2P topics early, for 3 reasons:
    # 1. Just because this node is unable to process the message does not
    #    necessarily mean the message is incorrect (ex: bug in a new version).
    # 2. If the publication fails after the processing, we end up in a situation where
    #    a message exists without being propagated to the other nodes, ultimately
    #    causing sync issues on the network.
    # 3. The message is currently fed to this node using the P2P service client
    #    loopback mechanism.
    ipfs_service = get_ipfs_service_from_request(request)
    p2p_client = get_p2p_client_from_request(request)

    message_topic = config.aleph.queue_topic.value
    failed_publications = await _pub_on_p2p_topics(
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        topic=message_topic,
        payload=aleph_json.dumps(request_data.message_dict),
    )
    pub_status = PublicationStatus.from_failures(failed_publications)
    if pub_status.status == "error":
        return web.json_response(
            text=PubMessageResponse(
                publication_status=pub_status, message_status=None
            ).json(),
            status=500,
        )

    status = PubMessageResponse(
        publication_status=pub_status, message_status=MessageStatus.PENDING
    )

    # When publishing in async mode, just respond with 202 (Accepted).
    message_accepted_response = web.json_response(text=status.json(), status=202)
    if not request_data.sync:
        return message_accepted_response

    # Ignore type checking here, we know that mq_queue is set at this point
    assert mq_queue is not None
    response = await _mq_read_one_message(mq_queue, timeout=30)

    # Delete the queue immediately
    await mq_queue.delete(if_empty=False)

    # If the message was not processed before the timeout, return a 202.
    if response is None:
        return message_accepted_response

    routing_key = response.routing_key
    assert routing_key is not None  # again, for type checking
    status_str, _item_hash = routing_key.split(".")
    processing_status = MessageProcessingStatus(status_str)
    status_code = _processing_status_to_http_status(processing_status)

    status.message_status = processing_status.to_message_status()

    return web.json_response(text=status.json(), status=status_code)
