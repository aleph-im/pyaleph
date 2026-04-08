import asyncio
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Set

import aio_pika.abc
import aiohttp.web_ws
from aiohttp import WSCloseCode, WSMsgType, web
from aleph_message.models import ItemHash, MessageType
from configmanager import Config
from pydantic import ValidationError
from sqlalchemy.orm import defer

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import (
    count_matching_hashes,
    get_forgotten_message,
    get_matching_hashes,
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
from aleph.toolkit.cursor import decode_message_cursor, encode_message_cursor
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus, RemovedMessageReason
from aleph.web.controllers.app_state_getters import (
    APP_STATE_MESSAGE_BROADCASTER,
    get_config_from_request,
    get_node_cache_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import (
    get_item_hash_from_request,
    mq_make_aleph_message_topic_queue,
    validate_cursor_pagination,
)

LOGGER = logging.getLogger(__name__)


_SEND_BATCH_SIZE = 100
_HEALTH_CHECK_INTERVAL = 5


class _WsClient:
    """A connected WS client with its filter params."""

    def __init__(
        self,
        ws: web.WebSocketResponse,
        query_params: WsMessageQueryParams,
        exclude_content: bool,
    ):
        self.ws = ws
        self.query_params = query_params
        self.exclude_content = exclude_content


class MessageBroadcaster:
    """Single MQ consumer that fans out messages to all connected WS clients."""

    def __init__(
        self,
        mq_conn: aio_pika.abc.AbstractConnection,
        config: Config,
    ):
        self._mq_conn = mq_conn
        self._config = config
        self._clients: Set[_WsClient] = set()
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._queue: Optional[aio_pika.abc.AbstractQueue] = None
        self._consumer_tag: Optional[aio_pika.abc.ConsumerTag] = None
        self._health_task: Optional[asyncio.Task] = None

        # Connection limit
        self.max_connections: int = config.websocket.max_message_connections.value
        self._semaphore = asyncio.Semaphore(self.max_connections)

        # Metrics
        self.broadcast_total: int = 0
        self.connections_rejected_total: int = 0
        self.consumer_restarts_total: int = 0

    @property
    def active_connections(self) -> int:
        return len(self._clients)

    @property
    def is_at_capacity(self) -> bool:
        return self._semaphore.locked()

    def acquire_slot(self) -> asyncio.Semaphore:
        return self._semaphore

    async def _start_consumer(self):
        """Create channel, queue, and consumer."""
        self._channel = await self._mq_conn.channel()
        self._queue = await mq_make_aleph_message_topic_queue(
            channel=self._channel,
            config=self._config,
            routing_key="processed.*",
        )
        self._consumer_tag = await self._queue.consume(self._on_message, no_ack=True)
        LOGGER.info("MessageBroadcaster: started MQ consumer")

    async def _stop_consumer(self):
        """Stop the MQ consumer and clean up resources."""
        if self._consumer_tag and self._queue:
            try:
                await self._queue.cancel(self._consumer_tag)
            except Exception:
                LOGGER.warning("Failed to cancel broadcaster consumer", exc_info=True)
        self._consumer_tag = None

        if self._queue:
            try:
                await self._queue.delete(if_unused=False, if_empty=False)
            except Exception:
                LOGGER.warning("Failed to delete broadcaster queue", exc_info=True)
        self._queue = None

        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                LOGGER.warning("Failed to close broadcaster channel", exc_info=True)
        self._channel = None

        LOGGER.info("MessageBroadcaster: stopped MQ consumer")

    async def _restart_consumer(self):
        """Restart the consumer after a channel failure."""
        self.consumer_restarts_total += 1
        LOGGER.warning(
            "MessageBroadcaster: restarting consumer (restart #%d)",
            self.consumer_restarts_total,
        )
        await self._stop_consumer()
        try:
            await self._start_consumer()
        except Exception:
            LOGGER.exception("MessageBroadcaster: failed to restart consumer")

    async def _health_check_loop(self):
        """Periodic health check that restarts the consumer if the channel dies."""
        try:
            while self._clients:
                await asyncio.sleep(_HEALTH_CHECK_INTERVAL)
                if not self._clients:
                    break
                if self._channel is None or self._channel.is_closed:
                    await self._restart_consumer()
        except asyncio.CancelledError:
            LOGGER.debug("MessageBroadcaster: health check loop cancelled")

    async def add(self, client: _WsClient):
        self._clients.add(client)
        if self._consumer_tag is None:
            await self._start_consumer()
        if self._health_task is None or self._health_task.done():
            self._health_task = asyncio.create_task(self._health_check_loop())

    async def remove(self, client: _WsClient):
        self._clients.discard(client)
        if not self._clients:
            if self._health_task and not self._health_task.done():
                self._health_task.cancel()
                self._health_task = None
            await self._stop_consumer()

    async def shutdown(self):
        """Graceful shutdown — called from aiohttp on_cleanup."""
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            self._health_task = None
        await self._stop_consumer()
        self._clients.clear()

    async def _on_message(self, mq_message: aio_pika.abc.AbstractMessage):
        """Called for each message from RabbitMQ. Fan out to matching clients."""
        try:
            payload_bytes = mq_message.body
            payload_dict = aleph_json.loads(payload_bytes)
            message = format_message_dict(payload_dict["message"])
        except Exception:
            LOGGER.exception("MessageBroadcaster: failed to parse MQ message")
            return

        self.broadcast_total += 1
        clients = list(self._clients)
        if not clients:
            return

        # Lazy-serialize only the variants that clients actually need
        needs_full = any(not c.exclude_content for c in clients)
        needs_no_content = any(c.exclude_content for c in clients)
        json_full = message.model_dump_json() if needs_full else ""
        json_no_content = (
            message.model_dump_json(exclude={"content"}) if needs_no_content else ""
        )

        # Send in batches to avoid event loop starvation with many clients
        for i in range(0, len(clients), _SEND_BATCH_SIZE):
            batch = clients[i : i + _SEND_BATCH_SIZE]
            results = await asyncio.gather(
                *[
                    self._send_to_client(client, message, json_full, json_no_content)
                    for client in batch
                ],
                return_exceptions=True,
            )
            for client, result in zip(batch, results):
                if isinstance(result, Exception):
                    LOGGER.debug(
                        "MessageBroadcaster: error sending to client: %s",
                        result,
                    )
                    continue
                if result is False:
                    self._clients.discard(client)

    async def _send_to_client(
        self,
        client: _WsClient,
        message: AlephMessage,
        json_full: str,
        json_no_content: str,
    ) -> bool:
        """Send a message to a single client. Returns False if client is dead."""
        if client.ws.closed:
            return False
        if not message_matches_filters(message, client.query_params):
            return True
        try:
            payload = json_no_content if client.exclude_content else json_full
            await client.ws.send_str(payload)
            return True
        except (ConnectionResetError, ConnectionError):
            return False


def message_to_dict(
    message: MessageDb, exclude_content: bool = False
) -> Dict[str, Any]:
    if exclude_content:
        message_dict = message.to_dict(exclude={"content"})
    else:
        message_dict = message.to_dict()
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)

    # Remove denormalized columns from API response to avoid breaking SDKs
    for key in (
        "status",
        "reception_time",
        "owner",
        "content_type",
        "content_ref",
        "content_key",
        "content_item_hash",
        "first_confirmed_at",
        "first_confirmed_height",
        "payment_type",
    ):
        message_dict.pop(key, None)

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
    messages: Iterable[MessageDb],
    pagination: int,
    page: int,
    total_messages: int,
    exclude_content: bool = False,
) -> web.Response:
    formatted_messages = [
        message_to_dict(message, exclude_content=exclude_content)
        for message in messages
    ]

    response = format_response_dict(
        messages=formatted_messages,
        pagination=pagination,
        page=page,
        total_messages=total_messages,
    )

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


async def view_messages_list(request: web.Request) -> web.Response:
    """
    List messages with filters.

    ---
    summary: List messages
    tags:
      - Messages
    parameters:
      - name: sortBy
        in: query
        schema:
          type: string
          enum: [time, tx-time]
          default: time
      - name: sortOrder
        in: query
        schema:
          type: integer
          enum: [-1, 1]
          default: -1
      - name: msgType
        in: query
        schema:
          type: string
          enum: [POST, AGGREGATE, STORE, PROGRAM, INSTANCE, FORGET]
      - name: msgTypes
        in: query
        schema:
          type: string
      - name: msgStatuses
        in: query
        schema:
          type: string
      - name: addresses
        in: query
        schema:
          type: string
      - name: owners
        in: query
        schema:
          type: string
      - name: refs
        in: query
        schema:
          type: string
      - name: contentHashes
        in: query
        schema:
          type: string
      - name: contentKeys
        in: query
        schema:
          type: string
      - name: contentTypes
        in: query
        schema:
          type: string
      - name: chains
        in: query
        schema:
          type: string
      - name: channels
        in: query
        schema:
          type: string
      - name: tags
        in: query
        schema:
          type: string
      - name: hashes
        in: query
        schema:
          type: string
      - name: startDate
        in: query
        schema:
          type: number
          default: 0
      - name: endDate
        in: query
        schema:
          type: number
          default: 0
      - name: startBlock
        in: query
        schema:
          type: integer
          default: 0
      - name: endBlock
        in: query
        schema:
          type: integer
          default: 0
      - name: excludeContent
        in: query
        schema:
          type: boolean
          default: false
        description: If true, omit the 'content' field from each message.
      - name: pagination
        in: query
        schema:
          type: integer
          default: 20
          minimum: 0
      - name: page
        in: query
        schema:
          type: integer
          default: 1
          minimum: 1
    responses:
      '200':
        description: Paginated list of messages
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PaginatedMessages'
      '422':
        description: Validation error
    """

    try:
        query_params = MessageQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    # If called from the messages/page/{page}.json endpoint, override the page
    # parameters with the URL one
    if url_page_param := request.match_info.get("page"):
        query_params.page = int(url_page_param)

    find_filters = query_params.model_dump(exclude_none=True)

    exclude_content = find_filters.pop("exclude_content", False)
    pagination_per_page = query_params.pagination
    cursor = find_filters.pop("cursor", None)

    session_factory = get_session_factory_from_request(request)

    if cursor is not None:
        # Cursor mode: no count needed
        pagination_per_page = validate_cursor_pagination(cursor, pagination_per_page)

        after_time, after_hash = None, None
        if cursor:
            try:
                after_time, after_hash = decode_message_cursor(cursor)
            except ValueError as e:
                raise web.HTTPUnprocessableEntity(text=str(e))

        messages_query = make_matching_messages_query(
            include_confirmations=True,
            after_time=after_time,
            after_hash=after_hash,
            cursor_mode=True,
            **find_filters,
        )

        if exclude_content:
            messages_query = messages_query.options(defer(MessageDb.content))

        with session_factory() as session:
            messages = list(session.execute(messages_query).scalars())

        has_more = len(messages) > pagination_per_page
        if has_more:
            messages = messages[:pagination_per_page]

        formatted = [
            message_to_dict(m, exclude_content=exclude_content) for m in messages
        ]
        next_cursor = None
        if has_more and messages:
            last = messages[-1]
            next_cursor = encode_message_cursor(last.time, last.item_hash)

        return web.json_response(
            text=aleph_json.dumps(
                {
                    "messages": formatted,
                    "pagination_per_page": pagination_per_page,
                    "next_cursor": next_cursor,
                }
            ).decode("utf-8")
        )
    else:
        # Legacy page mode (backward compat)
        pagination_page = query_params.page

        with session_factory() as session:
            messages_query = make_matching_messages_query(
                include_confirmations=True, **find_filters
            )
            if exclude_content:
                messages_query = messages_query.options(defer(MessageDb.content))
            messages = list(session.execute(messages_query).scalars())

        # If the result set is smaller than the page size, we already know
        # the total count without running a separate COUNT query.
        if pagination_per_page and len(messages) < pagination_per_page:
            total_msgs = (pagination_page - 1) * pagination_per_page + len(messages)
        else:
            node_cache = get_node_cache_from_request(request)
            total_msgs = await node_cache.count_messages(session_factory, query_params)

        return format_response(
            messages,
            pagination=pagination_per_page,
            page=pagination_page,
            total_messages=total_msgs,
            exclude_content=exclude_content,
        )


async def _send_history_to_ws(
    ws: aiohttp.web_ws.WebSocketResponse,
    session_factory: DbSessionFactory,
    history: int,
    query_params: WsMessageQueryParams,
) -> None:
    find_filters = query_params.model_dump(exclude_none=True)
    exclude_content = find_filters.pop("exclude_content", False)

    messages_query = make_matching_messages_query(
        pagination=history,
        include_confirmations=True,
        **find_filters,
    )
    if exclude_content:
        messages_query = messages_query.options(defer(MessageDb.content))

    with session_factory() as session:
        messages = list(session.execute(messages_query).scalars())

    for message in reversed(messages):
        msg_dict = message_to_dict(message, exclude_content=exclude_content)
        await ws.send_str(aleph_json.dumps(msg_dict).decode("utf-8"))


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


async def messages_ws(request: web.Request) -> web.WebSocketResponse:
    config = get_config_from_request(request)
    heartbeat = config.websocket.heartbeat.value

    ws = web.WebSocketResponse(heartbeat=float(heartbeat))
    await ws.prepare(request)

    broadcaster: MessageBroadcaster = request.app[APP_STATE_MESSAGE_BROADCASTER]

    if broadcaster.is_at_capacity:
        broadcaster.connections_rejected_total += 1
        LOGGER.warning(
            "WebSocket connection limit reached (%d)", broadcaster.max_connections
        )
        await ws.close(
            code=WSCloseCode.TRY_AGAIN_LATER, message=b"Too many connections"
        )
        return ws

    async with broadcaster.acquire_slot():
        session_factory = get_session_factory_from_request(request)

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

        client = _WsClient(ws, query_params, query_params.exclude_content)
        await broadcaster.add(client)

        try:
            while not ws.closed:
                ws_msg = await ws.receive()
                LOGGER.debug("rx ws msg: %s", str(ws_msg))
                if ws_msg.type in (
                    WSMsgType.CLOSE,
                    WSMsgType.ERROR,
                    WSMsgType.CLOSING,
                ):
                    break
        finally:
            await broadcaster.remove(client)
            if not ws.closed:
                await ws.close()

    return ws


def _get_message_with_status(
    session: DbSession,
    status_db: MessageStatusDb,
    message_db: Optional[MessageDb] = None,
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
    """
    Get a single message by item hash.

    ---
    summary: Get message
    tags:
      - Messages
    parameters:
      - name: item_hash
        in: path
        required: true
        schema:
          type: string
    responses:
      '200':
        description: Message with status
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/MessageWithStatus'
      '404':
        description: Message not found
    """
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
    """
    Get the content of a POST message by item hash.

    ---
    summary: Get message content
    tags:
      - Messages
    parameters:
      - name: item_hash
        in: path
        required: true
        schema:
          type: string
    responses:
      '200':
        description: Message content (JSON)
        content:
          application/json:
            schema:
              type: object
      '404':
        description: Message not found
      '422':
        description: Invalid message type or status
    """
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
    """
    Get the processing status of a message.

    ---
    summary: Get message status
    tags:
      - Messages
    parameters:
      - name: item_hash
        in: path
        required: true
        schema:
          type: string
    responses:
      '200':
        description: Message status info
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/MessageStatusInfo'
      '404':
        description: Message not found
    """
    item_hash = get_item_hash_from_request(request)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status = get_message_status(session=session, item_hash=item_hash)
        if message_status is None:
            raise web.HTTPNotFound()

    status_info = MessageStatusInfo.model_validate(message_status)
    return web.json_response(text=status_info.model_dump_json())


async def view_message_hashes(request: web.Request):
    """
    List message hashes with filters.

    ---
    summary: List message hashes
    tags:
      - Messages
    parameters:
      - name: status
        in: query
        schema:
          type: string
      - name: page
        in: query
        schema:
          type: integer
          default: 1
          minimum: 1
      - name: pagination
        in: query
        schema:
          type: integer
          default: 20
          minimum: 0
      - name: startDate
        in: query
        schema:
          type: number
          default: 0
      - name: endDate
        in: query
        schema:
          type: number
          default: 0
      - name: sortOrder
        in: query
        schema:
          type: integer
          enum: [-1, 1]
          default: -1
      - name: hash_only
        in: query
        schema:
          type: boolean
          default: true
    responses:
      '200':
        description: Paginated list of message hashes
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/MessageHashes'
      '422':
        description: Validation error
    """
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
