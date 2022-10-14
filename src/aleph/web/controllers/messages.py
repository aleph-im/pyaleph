import asyncio
import logging
from enum import IntEnum
from typing import Any, List, Optional, Mapping

from aiohttp import web
from aleph_message.models import MessageType, ItemHash, Chain
from bson.objectid import ObjectId
from pydantic import BaseModel, Field, validator, ValidationError, root_validator
from pymongo.cursor import CursorType

from aleph.model.messages import CappedMessage, Message
from aleph.web.controllers.utils import (
    LIST_FIELD_SEPARATOR,
    Pagination,
    cond_output,
    make_date_filters,
)

LOGGER = logging.getLogger(__name__)


DEFAULT_MESSAGES_PER_PAGE = 20
DEFAULT_PAGE = 1
DEFAULT_WS_HISTORY = 10


class SortOrder(IntEnum):
    ASCENDING = 1
    DESCENDING = -1


class BaseMessageQueryParams(BaseModel):
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )
    message_type: Optional[MessageType] = Field(
        default=None, alias="msgType", description="Message type."
    )
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    content_hashes: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentHashes",
        description="Accepted values for the 'content.item_hash' field.",
    )
    content_keys: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentKeys",
        description="Accepted values for the 'content.keys' field.",
    )
    content_types: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentTypes",
        description="Accepted values for the 'content.type' field.",
    )
    chains: Optional[List[Chain]] = Field(
        default=None, description="Accepted values for the 'chain' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )

    @root_validator
    def validate_field_dependencies(cls, values):
        start_date = values.get("start_date")
        end_date = values.get("end_date")
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")
        return values

    @validator(
        "addresses",
        "content_hashes",
        "content_keys",
        "content_types",
        "chains",
        "channels",
        "tags",
        pre=True,
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v

    def to_filter_list(self) -> List[Mapping[str, Any]]:
        filters: List[Mapping[str, Any]] = []

        if self.message_type is not None:
            filters.append({"type": self.message_type})

        if self.addresses is not None:
            filters.append(
                {
                    "$or": [
                        {"content.address": {"$in": self.addresses}},
                        {"sender": {"$in": self.addresses}},
                    ]
                }
            )

        if self.content_hashes is not None:
            filters.append({"content.item_hash": {"$in": self.content_hashes}})
        if self.content_keys is not None:
            filters.append({"content.key": {"$in": self.content_keys}})
        if self.content_types is not None:
            filters.append({"content.type": {"$in": self.content_types}})
        if self.refs is not None:
            filters.append({"content.ref": {"$in": self.refs}})
        if self.tags is not None:
            filters.append({"content.content.tags": {"$elemMatch": {"$in": self.tags}}})
        if self.chains is not None:
            filters.append({"chain": {"$in": self.chains}})
        if self.channels is not None:
            filters.append({"channel": {"$in": self.channels}})
        if self.hashes is not None:
            filters.append(
                {
                    "$or": [
                        {"item_hash": {"$in": self.hashes}},
                        {"tx_hash": {"$in": self.hashes}},
                    ]
                }
            )

        return filters

    def to_mongodb_filters(self) -> Mapping[str, Any]:
        filters = self.to_filter_list()
        return self._make_and_filter(filters)

    @staticmethod
    def _make_and_filter(filters: List[Mapping[str, Any]]) -> Mapping[str, Any]:
        and_filter: Mapping[str, Any] = {}
        if filters:
            and_filter = {"$and": filters} if len(filters) > 1 else filters[0]

        return and_filter


class MessageQueryParams(BaseMessageQueryParams):
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )

    start_date: float = Field(
        default=0,
        ge=0,
        alias="startDate",
        description="Start date timestamp. If specified, only messages with "
        "a time field greater or equal to this value will be returned.",
    )
    end_date: float = Field(
        default=0,
        ge=0,
        alias="endDate",
        description="End date timestamp. If specified, only messages with "
        "a time field lower than this value will be returned.",
    )

    def to_filter_list(self) -> List[Mapping[str, Any]]:
        filters = super().to_filter_list()
        date_filters = make_date_filters(
            start=self.start_date, end=self.end_date, filter_key="time"
        )
        if date_filters:
            filters.append(date_filters)
        return filters


class WsMessageQueryParams(BaseMessageQueryParams):
    history: Optional[int] = Field(
        DEFAULT_WS_HISTORY,
        ge=10,
        lt=200,
        description="Accepted values for the 'item_hash' field.",
    )


async def view_messages_list(request):
    """Messages list view with filters"""

    try:
        query_params = MessageQueryParams.parse_obj(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(body=e.json(indent=4))

    # If called from the messages/page/{page}.json endpoint, override the page
    # parameters with the URL one
    if url_page_param := request.match_info.get("page"):
        query_params.page = int(url_page_param)

    find_filters = query_params.to_mongodb_filters()

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination
    pagination_skip = (query_params.page - 1) * query_params.pagination

    messages = [
        msg
        async for msg in Message.collection.find(
            filter=find_filters,
            projection={"_id": 0},
            limit=pagination_per_page,
            skip=pagination_skip,
            sort=[("time", query_params.sort_order.value)],
        )
    ]

    context = {"messages": messages}

    if pagination_per_page is not None:
        if find_filters:
            total_msgs = await Message.collection.count_documents(find_filters)
        else:
            total_msgs = await Message.collection.estimated_document_count()

        query_string = request.query_string
        pagination = Pagination(
            pagination_page,
            pagination_per_page,
            total_msgs,
            url_base="/messages/posts/page/",
            query_string=query_string,
        )

        context.update(
            {
                "pagination": pagination,
                "pagination_page": pagination_page,
                "pagination_total": total_msgs,
                "pagination_per_page": pagination_per_page,
                "pagination_item": "messages",
            }
        )

    return cond_output(request, context, "TODO.html")


async def messages_ws(request: web.Request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    collection = CappedMessage.collection
    last_id = None

    query_params = WsMessageQueryParams.parse_obj(request.query)
    find_filters = query_params.to_mongodb_filters()

    initial_count = query_params.history

    items = [
        item
        async for item in collection.find(find_filters)
        .sort([("$natural", -1)])
        .limit(initial_count)
    ]
    for item in reversed(items):
        item["_id"] = str(item["_id"])

        last_id = item["_id"]
        await ws.send_json(item)

    closing = False

    while not closing:
        try:
            cursor = collection.find(
                {"_id": {"$gt": ObjectId(last_id)}},
                cursor_type=CursorType.TAILABLE_AWAIT,
            )
            while cursor.alive:
                async for item in cursor:
                    if ws.closed:
                        closing = True
                        break
                    item["_id"] = str(item["_id"])

                    last_id = item["_id"]
                    await ws.send_json(item)

                await asyncio.sleep(1)

                if closing:
                    break

        except ConnectionResetError:
            break

        except Exception:
            if ws.closed:
                break

            LOGGER.exception("Error processing")
            await asyncio.sleep(1)
