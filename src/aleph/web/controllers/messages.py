import asyncio
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import aio_pika.abc
import aiohttp.web_ws
from aiohttp import WSCloseCode, WSMsgType, web
from aleph_message.models import ItemHash, MessageType, PaymentType
from configmanager import Config
from pydantic import ValidationError
from sqlalchemy.orm import defer

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import (
    count_matching_forgotten_messages,
    count_matching_hashes,
    count_matching_removed_messages,
    get_forgotten_message,
    get_matching_hashes,
    get_message_by_item_hash,
    get_message_status,
    get_rejected_message,
    get_removed_message,
    make_matching_forgotten_messages_query,
    make_matching_messages_query,
    make_matching_removed_messages_query,
)
from aleph.db.accessors.pending_messages import get_pending_messages
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.db.models.messages import ForgottenMessageDb, RemovedMessageDb
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
    RemovedMessage,
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
from aleph.services.cache.node_cache import NodeCache
from aleph.toolkit.cursor import decode_message_cursor, encode_message_cursor
from aleph.types.content_format import ContentFormat
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
    get_path_page,
    mq_make_aleph_message_topic_queue,
    validate_cursor_pagination,
)

LOGGER = logging.getLogger(__name__)


_SEND_BATCH_SIZE = 100
_HEALTH_CHECK_INTERVAL = 5

# Redis keys for WS message metrics. Shared across gunicorn workers so
# Prometheus sees cluster-wide state regardless of which worker serves /metrics.
WS_MESSAGES_BROADCAST_TOTAL_KEY = "pyaleph_ws_messages_broadcast_total"
WS_MESSAGES_CONNECTIONS_ACTIVE_KEY = "pyaleph_ws_messages_connections_active"
WS_MESSAGES_CONNECTIONS_REJECTED_KEY = "pyaleph_ws_messages_connections_rejected_total"
WS_BROADCASTER_CONSUMER_RESTARTS_KEY = "pyaleph_ws_broadcaster_consumer_restarts_total"


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
        node_cache: NodeCache,
    ):
        self._mq_conn = mq_conn
        self._config = config
        self._node_cache = node_cache
        self._clients: Set[_WsClient] = set()
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._queue: Optional[aio_pika.abc.AbstractQueue] = None
        self._consumer_tag: Optional[aio_pika.abc.ConsumerTag] = None
        self._health_task: Optional[asyncio.Task] = None
        # Serializes the consumer lifecycle (start/stop/restart). Without it,
        # two clients connecting concurrently could both observe
        # _consumer_tag is None and each start a consumer, binding a second
        # queue to processed.* so every message gets fanned out more than once.
        self._consumer_lock = asyncio.Lock()

        # Connection limit (same on every worker — from config).
        self.max_connections: int = config.websocket.max_message_connections.value
        self._semaphore = asyncio.Semaphore(self.max_connections)
        # Note: counter state lives in Redis (see WS_*_KEY constants). This
        # class never holds it locally so that all gunicorn workers share
        # the same observed values.

    async def record_rejection(self) -> None:
        """Increment the shared 'connection rejected' counter."""
        await self._node_cache.incr(WS_MESSAGES_CONNECTIONS_REJECTED_KEY)

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
        await self._node_cache.incr(WS_BROADCASTER_CONSUMER_RESTARTS_KEY)
        LOGGER.warning("MessageBroadcaster: restarting consumer")
        async with self._consumer_lock:
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
        if client in self._clients:
            return
        self._clients.add(client)
        await self._node_cache.incr(WS_MESSAGES_CONNECTIONS_ACTIVE_KEY)
        # Start the shared consumer and health task exactly once, even when
        # many clients connect concurrently. The lock makes the
        # check-and-start atomic so a second queue is never bound.
        async with self._consumer_lock:
            if self._consumer_tag is None:
                await self._start_consumer()
            if self._health_task is None or self._health_task.done():
                self._health_task = asyncio.create_task(self._health_check_loop())

    async def remove(self, client: _WsClient):
        # Guard against double-remove: Redis DECR must be idempotent-safe.
        if client not in self._clients:
            return
        self._clients.discard(client)
        await self._node_cache.decr(WS_MESSAGES_CONNECTIONS_ACTIVE_KEY)
        if not self._clients:
            if self._health_task and not self._health_task.done():
                self._health_task.cancel()
                self._health_task = None
            async with self._consumer_lock:
                await self._stop_consumer()

    async def shutdown(self):
        """Graceful shutdown — called from aiohttp on_cleanup."""
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            self._health_task = None
        async with self._consumer_lock:
            await self._stop_consumer()
        # Decrement the shared active counter once per client this worker
        # owned, so the Redis value reflects only clients still connected
        # through other workers.
        if self._clients:
            await self._node_cache.decrby(
                WS_MESSAGES_CONNECTIONS_ACTIVE_KEY, len(self._clients)
            )
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

        await self._node_cache.incr(WS_MESSAGES_BROADCAST_TOTAL_KEY)
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
        dead: List[_WsClient] = []
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
                    dead.append(client)

        # Route dead clients through remove() so the Redis active counter is
        # decremented and the MQ consumer is torn down when the last client
        # drops. A bare set discard here would leave the counter drifting up.
        for client in dead:
            await self.remove(client)

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
        except ConnectionError:
            return False


# Per-type reduced "headers" content: (output key in content, MessageDb attribute).
# `address` is emitted for every type from the `owner` column and is handled
# separately. All source attributes are denormalized columns, so building the
# reduced content never touches the (deferred) content JSONB.
_HEADERS_FIELDS: Dict[MessageType, List[Tuple[str, str]]] = {
    MessageType.post: [("type", "content_type"), ("ref", "content_ref")],
    MessageType.aggregate: [("key", "content_key")],
    MessageType.store: [("item_hash", "content_item_hash"), ("ref", "content_ref")],
    MessageType.program: [],
    MessageType.instance: [],
    MessageType.forget: [],
    MessageType.v_program: [],
}


def build_headers_content(message: MessageDb) -> Dict[str, Any]:
    """Reduced ``content`` for ``contentFormat=headers``, built from columns.

    `address` is always included (from ``owner``); the per-type fields in
    ``_HEADERS_FIELDS`` are included when their column value is not ``None``.
    """
    content: Dict[str, Any] = {"address": message.owner}
    for output_key, attr in _HEADERS_FIELDS.get(message.type, []):
        value = getattr(message, attr)
        if value is not None:
            content[output_key] = value
    return content


def message_to_dict(
    message: MessageDb, content_format: ContentFormat = ContentFormat.FULL
) -> Dict[str, Any]:
    if content_format == ContentFormat.FULL:
        message_dict = message.to_dict()
    else:
        message_dict = message.to_dict(exclude={"content"})
        if content_format == ContentFormat.HEADERS:
            message_dict["content"] = build_headers_content(message)
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)

    # Remove denormalized columns from API response to avoid breaking SDKs
    for key in MessageDb.DENORMALIZED_COLUMNS:
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


def forgotten_message_to_dict(message: ForgottenMessageDb) -> Dict[str, Any]:
    # The ForgottenMessage schema owns the serialization rules (epoch-float
    # time fields), so both the single-message endpoint and this list share
    # one serializer.
    message_dict = ForgottenMessage.model_validate(message).model_dump()
    message_dict["status"] = MessageStatus.FORGOTTEN
    message_dict["forgotten_by"] = message.forgotten_by
    return message_dict


def _validate_terminal_status_query_params(
    query_params: MessageQueryParams, status_value: str
) -> None:
    """
    Shared validation for forgotten/removed-only queries: cursor pagination
    and filters over data these queries cannot narrow are rejected instead of
    being silently ignored.
    """
    if query_params.cursor is not None:
        raise web.HTTPBadRequest(
            text=f"Cursor pagination is not supported for {status_value} "
            "message queries"
        )
    unsupported_filters = {
        "refs": query_params.refs,
        "contentTypes": query_params.content_types,
        "contentHashes": query_params.content_hashes,
        "contentKeys": query_params.content_keys,
        "tags": query_params.tags,
        "startBlock": query_params.start_block,
        "endBlock": query_params.end_block,
    }
    offending = [name for name, value in unsupported_filters.items() if value]
    if offending:
        raise web.HTTPBadRequest(
            text=f"Filters not supported for {status_value} message queries: "
            + ", ".join(offending)
        )


def _list_forgotten_messages(
    request: web.Request, query_params: MessageQueryParams
) -> web.Response:
    """
    Forgotten-only variant of the message list, served from the
    forgotten_messages table. Date filters and time sorting apply to
    `forgotten_at` (deletion time), not `time`.
    """
    find_filters = query_params.model_dump(exclude_none=True)
    # Consumed by the caller or meaningless for forgotten queries.
    for key in ("content_format", "exclude_content", "cursor", "message_statuses"):
        find_filters.pop(key, None)

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        forgotten_messages = list(
            session.execute(
                make_matching_forgotten_messages_query(**find_filters)
            ).scalars()
        )

        # If the result set is smaller than the page size, we already know
        # the total count without running a separate COUNT query. Known
        # trade-off (shared with the live-message listing): a page past the
        # end of the result set overestimates the total.
        if (
            query_params.pagination
            and len(forgotten_messages) < query_params.pagination
        ):
            total_msgs = (query_params.page - 1) * query_params.pagination + len(
                forgotten_messages
            )
        else:
            total_msgs = count_matching_forgotten_messages(
                session=session, **find_filters
            )

        response = format_response_dict(
            messages=[forgotten_message_to_dict(fm) for fm in forgotten_messages],
            pagination=query_params.pagination,
            page=query_params.page,
            total_messages=total_msgs,
        )

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


def removed_message_to_dict(message: RemovedMessageDb) -> Dict[str, Any]:
    # The RemovedMessage schema owns the serialization rules (epoch-float
    # time fields), so both the single-message endpoint and this list share
    # one serializer.
    message_dict = RemovedMessage.model_validate(message).model_dump()
    message_dict["status"] = MessageStatus.REMOVED
    return message_dict


def _list_removed_messages(
    request: web.Request, query_params: MessageQueryParams
) -> web.Response:
    """
    Removed-only variant of the message list, served from the
    removed_messages snapshots (the messages rows are deleted at removal).
    Date filters and time sorting apply to `removed_at` (node-local removal
    time), not `time`.
    """
    find_filters = query_params.model_dump(exclude_none=True)
    # Consumed by the caller or meaningless for removed queries.
    for key in ("content_format", "exclude_content", "cursor", "message_statuses"):
        find_filters.pop(key, None)

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        removed_messages = list(
            session.execute(
                make_matching_removed_messages_query(**find_filters)
            ).scalars()
        )

        # If the result set is smaller than the page size, we already know
        # the total count without running a separate COUNT query. Known
        # trade-off (shared with the live-message listing): a page past the
        # end of the result set overestimates the total.
        if query_params.pagination and len(removed_messages) < query_params.pagination:
            total_msgs = (query_params.page - 1) * query_params.pagination + len(
                removed_messages
            )
        else:
            total_msgs = count_matching_removed_messages(
                session=session, **find_filters
            )

        response = format_response_dict(
            messages=[removed_message_to_dict(rm) for rm in removed_messages],
            pagination=query_params.pagination,
            page=query_params.page,
            total_messages=total_msgs,
        )

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


def format_response(
    messages: Iterable[MessageDb],
    pagination: int,
    page: int,
    total_messages: int,
    content_format: ContentFormat = ContentFormat.FULL,
) -> web.Response:
    formatted_messages = [
        message_to_dict(message, content_format=content_format) for message in messages
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
          enum: [POST, AGGREGATE, STORE, PROGRAM, INSTANCE, FORGET, V-PROGRAM]
      - name: msgTypes
        in: query
        schema:
          type: string
      - name: msgStatuses
        in: query
        schema:
          type: string
        description: >-
          Accepted message statuses (comma-separated). 'forgotten' and
          'removed' are terminal statuses that must be the only status
          requested (otherwise 400): both switch the query to the
          corresponding snapshot table (forgotten_messages /
          removed_messages — the messages row is deleted at termination)
          and return message skeletons without content. Supported filters
          are then msgTypes, addresses, owners, chains, channels, hashes,
          paymentTypes and startDate/endDate; date filters and time sorting
          apply to the termination time (forgotten_at / removed_at), not to
          the message time. forgotten_at is the sender-supplied FORGET time
          — the same declared-time semantics as the default time sort and
          cursors of the live list; removed_at is the node-local removal
          time (not deterministic across nodes). Legacy rows without a
          termination time are excluded by date filters and sorted last
          otherwise. The 'size' field of both skeletons is the file-size
          snapshot preserved for billing (not the message content size).
          Unsupported narrowing filters (refs, contentTypes, contentHashes,
          contentKeys, tags, startBlock/endBlock) and cursor pagination are
          rejected with 400.
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
        deprecated: true
        schema:
          type: boolean
          default: false
        description: >-
          Deprecated: use contentFormat=none. If true (and contentFormat is not
          set), omit the 'content' field from each message.
      - name: contentFormat
        in: query
        schema:
          type: string
          enum: [full, headers, none]
          default: full
        description: >-
          Level of content detail. 'full' (default) returns the complete
          content. 'headers' returns a reduced per-type metadata subset
          (address; plus type/ref for POST, key for AGGREGATE, item_hash/ref for
          STORE) without reading the content JSONB. 'none' omits content
          entirely. Takes precedence over excludeContent.
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
    if (url_page := get_path_page(request)) is not None:
        query_params.page = url_page

    # Forgotten/removed-only queries run against dedicated tables (or joins)
    # and sort on the termination time. Mixing them with other statuses
    # would require a UNION across heterogeneous shapes, so it is rejected.
    message_statuses = set(query_params.message_statuses or [])
    terminal_statuses = message_statuses & {
        MessageStatus.FORGOTTEN,
        MessageStatus.REMOVED,
    }
    if terminal_statuses:
        if len(message_statuses) > 1:
            raise web.HTTPBadRequest(
                text="msgStatuses=forgotten/removed cannot be combined with "
                "other statuses"
            )
        status = next(iter(terminal_statuses))
        _validate_terminal_status_query_params(query_params, status.value)
        if status == MessageStatus.FORGOTTEN:
            return _list_forgotten_messages(request, query_params)
        return _list_removed_messages(request, query_params)

    find_filters = query_params.model_dump(exclude_none=True)

    content_format: ContentFormat = query_params.content_format or ContentFormat.FULL
    # Both keys are consumed here; neither is a query filter.
    find_filters.pop("content_format", None)
    find_filters.pop("exclude_content", None)
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

        if content_format != ContentFormat.FULL:
            messages_query = messages_query.options(defer(MessageDb.content))

        with session_factory() as session:
            messages = list(session.execute(messages_query).scalars())

        has_more = len(messages) > pagination_per_page
        if has_more:
            messages = messages[:pagination_per_page]

        formatted = [
            message_to_dict(m, content_format=content_format) for m in messages
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
            if content_format != ContentFormat.FULL:
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
            content_format=content_format,
        )


async def _send_history_to_ws(
    ws: aiohttp.web_ws.WebSocketResponse,
    session_factory: DbSessionFactory,
    history: int,
    query_params: WsMessageQueryParams,
) -> None:
    find_filters = query_params.model_dump(exclude_none=True)
    content_format: ContentFormat = query_params.content_format or ContentFormat.FULL
    find_filters.pop("content_format", None)
    find_filters.pop("exclude_content", None)

    # The websocket payload supports two states only: content present or absent.
    # `headers` is not implemented here, so it degrades to `none`.
    if content_format == ContentFormat.HEADERS:
        content_format = ContentFormat.NONE

    messages_query = make_matching_messages_query(
        pagination=history,
        include_confirmations=True,
        **find_filters,
    )
    if content_format != ContentFormat.FULL:
        messages_query = messages_query.options(defer(MessageDb.content))

    with session_factory() as session:
        messages = list(session.execute(messages_query).scalars())

    for message in reversed(messages):
        msg_dict = message_to_dict(message, content_format=content_format)
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
        await broadcaster.record_rejection()
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
            except ConnectionError:
                LOGGER.info("Could not send history, aborting message websocket")
                return ws

        client = _WsClient(
            ws, query_params, query_params.content_format != ContentFormat.FULL
        )
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


def _removal_reason(payment_type: Optional[str]) -> RemovedMessageReason:
    """Reason matching the message's payment type.

    Credit-paid messages are removed by the credit balance cron when credits run
    out; hold/superfluid messages by the token balance cron. Reporting the matching
    reason avoids surfacing ``balance_insufficient`` for a credit shortfall.

    Uses the persisted ``payment_type`` column (set at ingestion, and copied to
    the removed_messages snapshot at removal) rather than re-parsing the content,
    so it reflects what was stored and avoids a redundant content validation on
    every removed-message read.
    """
    if payment_type == PaymentType.credit.value:
        return RemovedMessageReason.CREDIT_INSUFFICIENT
    return RemovedMessageReason.BALANCE_INSUFFICIENT


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
            reason=_removal_reason(message_db.payment_type),
        )

    if status == MessageStatus.REMOVED:
        # The messages row is deleted at removal (mirroring forgotten
        # messages): the snapshot is the only record left.
        removed_message_db = get_removed_message(session=session, item_hash=item_hash)
        if not removed_message_db:
            raise web.HTTPGone(body=f"This message has been removed: {item_hash}")

        return RemovedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=RemovedMessage.model_validate(removed_message_db),
            reason=_removal_reason(removed_message_db.payment_type),
            removed_at=removed_message_db.removed_at,
            size=removed_message_db.size,
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
    Get the content of a processed message by item hash.

    For POST messages, returns the nested user content (content.content).
    For all other message types, returns the full message content.

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
        description: Message is not in a PROCESSED state
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

    if not isinstance(message_with_status, ProcessedMessageStatus):
        raise web.HTTPUnprocessableEntity(
            text=(
                f"Message {item_hash} is not processed "
                f"(status: {message_with_status.status})"
            )
        )

    # Serialize the full message once; extract the content portion below.
    # Using model_dump keeps mypy happy through the AlephMessage union.
    message_dict = message_with_status.message.model_dump(mode="json")
    content = message_dict["content"]
    if isinstance(message_with_status.message, PostMessage):
        # POST messages wrap the user payload in content.content
        content = content["content"]

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
