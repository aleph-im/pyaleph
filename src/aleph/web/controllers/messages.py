import json
import logging
from typing import Any, Dict, Iterable, List

import aio_pika.abc
import aiohttp.web_ws
from aiohttp import WSMsgType, web
from aleph_message.models import ItemHash, MessageType
from pydantic import ValidationError

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import (
    count_matching_hashes,
    get_forgotten_message,
    get_matching_hashes,
    get_matching_messages,
    get_message_by_item_hash,
    get_message_status,
    get_rejected_message,
    make_matching_messages_query,
)
from aleph.db.accessors.pending_messages import get_pending_messages
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.schemas.api.messages import (
    AlephMessage,
    ForgottenMessage,
    ForgottenMessageStatus,
    MessageHashes,
    MessageStatusInfo,
    MessageWithStatus,
    PendingMessage,
    PendingMessageStatus,
    PostMessage,
    ProcessedMessageStatus,
    RejectedMessageStatus,
    RemovedMessageStatus,
    RemovingMessageStatus,
    format_message,
    format_message_dict,
)
from aleph.schemas.messages_query_params import (
    MessageHashesQueryParams,
    MessageQueryParams,
    WsMessageQueryParams,
)
from aleph.toolkit.shield import shielded
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus, RemovedMessageReason
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_mq_ws_channel_from_request,
    get_node_cache_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import (
    get_item_hash_from_request,
    mq_make_aleph_message_topic_queue,
)

LOGGER = logging.getLogger(__name__)


def message_to_dict(message: MessageDb) -> Dict[str, Any]:
    message_dict = message.to_dict()
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)

    # TODO: Add this field in the response when we make sure it won't break any sdk schema checking
    # message_dict["status"] = message.status.status

    return message_dict


def format_response_dict(
    messages: List[Dict[str, Any]], pagination: int, page: int, total_messages: int
) -> Dict[str, Any]:
    return {
        "messages": messages,
        "pagination_per_page": pagination,
        "pagination_page": page,
        "pagination_total": total_messages,
        "pagination_item": "messages",
    }


def format_response(
    messages: Iterable[MessageDb], pagination: int, page: int, total_messages: int
) -> web.Response:
    formatted_messages = [message_to_dict(message) for message in messages]

    response = format_response_dict(
        messages=formatted_messages,
        pagination=pagination,
        page=page,
        total_messages=total_messages,
    )

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


async def view_messages_list(request: web.Request) -> web.Response:
    """Messages list view with filters"""

    try:
        query_params = MessageQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    # If called from the messages/page/{page}.json endpoint, override the page
    # parameters with the URL one
    if url_page_param := request.match_info.get("page"):
        query_params.page = int(url_page_param)

    find_filters = query_params.model_dump(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)

    with session_factory() as session:
        messages_query = make_matching_messages_query(
            include_confirmations=True, **find_filters
        )
        messages = (session.execute(messages_query)).scalars()
        total_msgs = await node_cache.count_messages(session, query_params)

        return format_response(
            messages,
            pagination=pagination_per_page,
            page=pagination_page,
            total_messages=total_msgs,
        )


async def _send_history_to_ws(
    ws: aiohttp.web_ws.WebSocketResponse,
    session_factory: DbSessionFactory,
    history: int,
    query_params: WsMessageQueryParams,
) -> None:
    with session_factory() as session:
        messages = get_matching_messages(
            session=session,
            pagination=history,
            include_confirmations=True,
            sort_order=SortOrder.ASCENDING,
            **query_params.model_dump(exclude_none=True),
        )
        for message in messages:
            await ws.send_str(format_message(message).model_dump_json())


def message_matches_filters(
    message: AlephMessage, query_params: WsMessageQueryParams
) -> bool:
    if message_type := query_params.message_type:
        if message.type != message_type:
            return False

    # For simple filters, this reduces the amount of boilerplate
    filters_by_message_field = {
        "sender": "addresses",
        "type": "message_type",
        "item_hash": "hashes",
        "chain": "chains",
        "channel": "channels",
    }

    if owners := query_params.owners:
        content_address = getattr(message.content, "address", None)
        if content_address not in owners:
            return False

    for message_field, query_field in filters_by_message_field.items():
        if user_filters := getattr(query_params, query_field):
            if not isinstance(user_filters, list):
                user_filters = [user_filters]
            if getattr(message, message_field) not in user_filters:
                return False

    # Process filters on content and content.content
    message_content = message.content
    if refs := query_params.refs:
        ref = getattr(message_content, "ref", None)
        if ref not in refs:
            return False

    if content_types := query_params.content_types:
        content_type = getattr(message_content, "type", None)
        if content_type not in content_types:
            return False

    if content_hashes := query_params.content_hashes:
        content_hash = getattr(message_content, "item_hash", None)
        if content_hash not in content_hashes:
            return False

    # For tags, we only need to match one filter
    if query_tags := query_params.tags:
        nested_content = getattr(message.content, "content")
        if not nested_content:
            return False

        content_tags = set(getattr(nested_content, "tags", []))
        if (content_tags & set(query_tags)) == set():
            return False

    return True


async def _start_mq_consumer(
    ws: aiohttp.web_ws.WebSocketResponse,
    mq_queue: aio_pika.abc.AbstractQueue,
    query_params: WsMessageQueryParams,
) -> aio_pika.abc.ConsumerTag:
    """
    Starts the consumer task responsible for forwarding new aleph.im messages from
    the processing pipeline to a websocket.

    :param ws: Websocket.
    :param mq_queue: Message queue object.
    :param query_params: Message filters specified by the caller.
    """

    async def _process_message(mq_message: aio_pika.abc.AbstractMessage):
        payload_bytes = mq_message.body
        payload_dict = aleph_json.loads(payload_bytes)
        message = format_message_dict(payload_dict["message"])

        if message_matches_filters(message=message, query_params=query_params):
            try:
                await ws.send_str(message.model_dump_json())
            except ConnectionResetError:
                # We can detect the WS closing in this task in addition to the main one.
                # The main task will also detect the close event.
                # We just ignore this exception to avoid the "task exception was never retrieved"
                # warning.
                LOGGER.info("Cannot send messages because the websocket is closed")

    # Note that we use the consume pattern here instead of using the `queue.iterator()`
    # pattern because cancelling the iterator attempts to close the queue and channel.
    # See discussion here: https://github.com/mosquito/aio-pika/issues/358
    consumer_tag = await mq_queue.consume(_process_message, no_ack=True)
    return consumer_tag


@shielded
async def messages_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    config = get_config_from_request(request)
    session_factory = get_session_factory_from_request(request)
    mq_channel = await get_mq_ws_channel_from_request(request=request, logger=LOGGER)

    try:
        query_params = WsMessageQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    history = query_params.history

    if history:
        try:
            await _send_history_to_ws(
                ws=ws,
                session_factory=session_factory,
                history=history,
                query_params=query_params,
            )
        except ConnectionResetError:
            LOGGER.info("Could not send history, aborting message websocket")
            return ws

    mq_queue = await mq_make_aleph_message_topic_queue(
        channel=mq_channel, config=config, routing_key="processed.*"
    )
    consumer_tag = None

    try:
        # Start a task to handle outgoing traffic to the websocket.
        consumer_tag = await _start_mq_consumer(
            ws=ws,
            mq_queue=mq_queue,
            query_params=query_params,
        )
        LOGGER.debug(
            "Started consuming mq %s for websocket. Consumer tag: %s",
            mq_queue.name,
            consumer_tag,
        )

        # Wait for the websocket to close.
        while not ws.closed:
            # Users can potentially send anything to the websocket. Ignore these messages
            # and only handle "close" messages.
            ws_msg = await ws.receive()
            LOGGER.debug("rx ws msg: %s", str(ws_msg))
            if ws_msg.type == WSMsgType.CLOSE:
                LOGGER.debug("ws close received")
                break

    finally:
        # In theory, we should cancel the consumer with `mq_queue.cancel()` before deleting the queue.
        # In practice, this sometimes leads to an RPC timeout that closes the channel.
        # To avoid this situation, we just delete the queue directly.
        # Note that even if the queue is in auto-delete mode, it will only be deleted automatically
        # once the channel closes. We delete it manually to avoid keeping queues around.
        if consumer_tag:
            LOGGER.info("Deleting consumer %s (queue: %s)", consumer_tag, mq_queue.name)
            await mq_queue.cancel(consumer_tag=consumer_tag)

        LOGGER.info("Deleting queue: %s", mq_queue.name)
        await mq_queue.delete(if_unused=False, if_empty=False)

    return ws


def _get_message_with_status(
    session: DbSession, status_db: MessageStatusDb
) -> MessageWithStatus:
    status = status_db.status
    item_hash = status_db.item_hash
    reception_time = status_db.reception_time
    if status == MessageStatus.PENDING:
        # There may be several instances of the same pending message, return the first.
        pending_messages_db = get_pending_messages(session=session, item_hash=item_hash)
        pending_messages = [
            PendingMessage.model_validate(m) for m in pending_messages_db
        ]
        return PendingMessageStatus(
            status=MessageStatus.PENDING,
            item_hash=item_hash,
            reception_time=reception_time,
            messages=pending_messages,
        )

    if status == MessageStatus.PROCESSED:
        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message_db:
            raise web.HTTPNotFound()

        message = format_message(message_db)
        return ProcessedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=message,
        )

    if status == MessageStatus.FORGOTTEN:
        forgotten_message_db = get_forgotten_message(
            session=session, item_hash=item_hash
        )
        if not forgotten_message_db:
            raise web.HTTPNotFound()

        return ForgottenMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=ForgottenMessage.model_validate(forgotten_message_db),
            forgotten_by=forgotten_message_db.forgotten_by,
        )

    if status == MessageStatus.REJECTED:
        rejected_message_db = get_rejected_message(session=session, item_hash=item_hash)
        if not rejected_message_db:
            raise web.HTTPNotFound()

        return RejectedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            error_code=rejected_message_db.error_code,
            details=rejected_message_db.details,
            message=rejected_message_db.message,
        )

    if status == MessageStatus.REMOVING:
        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message_db:
            raise web.HTTPNotFound()

        message = format_message(message_db)
        return RemovingMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=message,
            reason=RemovedMessageReason.BALANCE_INSUFFICIENT,
        )

    if status == MessageStatus.REMOVED:
        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message_db:
            raise web.HTTPNotFound()

        message = format_message(message_db)
        return RemovedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=message,
            reason=RemovedMessageReason.BALANCE_INSUFFICIENT,
        )

    raise NotImplementedError(f"Unknown message status: {status}")


async def view_message(request: web.Request):
    item_hash = get_item_hash_from_request(request)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status_db = get_message_status(session=session, item_hash=item_hash)
        if message_status_db is None:
            raise web.HTTPNotFound()
        message_with_status = _get_message_with_status(
            session=session, status_db=message_status_db
        )

    return web.json_response(text=message_with_status.model_dump_json())


async def view_message_content(request: web.Request):
    item_hash = get_item_hash_from_request(request)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status_db = get_message_status(session=session, item_hash=item_hash)
        if message_status_db is None:
            raise web.HTTPNotFound()
        message_with_status = _get_message_with_status(
            session=session, status_db=message_status_db
        )

    status = message_with_status.status
    if (
        status != MessageStatus.PROCESSED
        or not hasattr(message_with_status, "message")
        or not isinstance(message_with_status.message, PostMessage)
    ):
        raise web.HTTPUnprocessableEntity(
            text=f"Invalid message hash status {status} for hash {item_hash}"
        )

    message_type = message_with_status.message.type
    if message_type != MessageType.post:
        raise web.HTTPUnprocessableEntity(
            text=f"Invalid message hash type {message_type} for hash {item_hash}"
        )

    content = message_with_status.message.content.content
    return web.json_response(text=json.dumps(content))


async def view_message_status(request: web.Request):
    item_hash = get_item_hash_from_request(request)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status = get_message_status(session=session, item_hash=item_hash)
        if message_status is None:
            raise web.HTTPNotFound()

    status_info = MessageStatusInfo.model_validate(message_status)
    return web.json_response(text=status_info.model_dump_json())


async def view_message_hashes(request: web.Request):
    try:
        query_params = MessageHashesQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    find_filters = query_params.model_dump(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        hashes = get_matching_hashes(session, **find_filters)

        if find_filters["hash_only"]:
            formatted_hashes = [h for h in hashes]
        else:
            formatted_hashes = [MessageHashes.model_validate(h) for h in hashes]

        total_hashes = count_matching_hashes(session, **find_filters)
        response = {
            "hashes": formatted_hashes,
            "pagination_per_page": pagination_per_page,
            "pagination_page": pagination_page,
            "pagination_total": total_hashes,
            "pagination_item": "hashes",
        }

        return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))
