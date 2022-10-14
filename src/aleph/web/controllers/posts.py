from typing import Optional, List, Mapping, Any

from aiohttp import web
from aleph_message.models import ItemHash
from pydantic import BaseModel, Field, root_validator, validator, ValidationError

from aleph.model.messages import Message, get_merged_posts
from aleph.web.controllers.utils import (
    DEFAULT_MESSAGES_PER_PAGE,
    DEFAULT_PAGE,
    LIST_FIELD_SEPARATOR,
    Pagination,
    cond_output,
    make_date_filters,
)


class PostQueryParams(BaseModel):
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    post_types: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.type' field."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
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
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
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
        "hashes",
        "refs",
        "post_types",
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

        if self.addresses is not None:
            filters.append(
                {"content.address": {"$in": self.addresses}},
            )
        if self.post_types is not None:
            filters.append({"content.type": {"$in": self.post_types}})
        if self.refs is not None:
            filters.append({"content.ref": {"$in": self.refs}})
        if self.tags is not None:
            filters.append({"content.content.tags": {"$elemMatch": {"$in": self.tags}}})
        if self.hashes is not None:
            filters.append(
                {
                    "$or": [
                        {"item_hash": {"$in": self.hashes}},
                        {"tx_hash": {"$in": self.hashes}},
                    ]
                }
            )
        if self.channels is not None:
            filters.append({"channel": {"$in": self.channels}})

        date_filters = make_date_filters(
            start=self.start_date, end=self.end_date, filter_key="time"
        )
        if date_filters:
            filters.append(date_filters)

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


async def view_posts_list(request):
    """Posts list view with filters
    TODO: return state with amended posts
    """

    find_filters = {}
    query_string = request.query_string

    try:
        query_params = PostQueryParams.parse_obj(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(body=e.json(indent=4))

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination
    pagination_skip = (query_params.page - 1) * query_params.pagination

    posts = [
        msg
        async for msg in await get_merged_posts(
            find_filters, limit=pagination_per_page, skip=pagination_skip
        )
    ]

    context = {"posts": posts}

    if pagination_per_page is not None:
        total_msgs = await Message.collection.count_documents(filter=find_filters)

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
                "pagination_item": "posts",
            }
        )

    return cond_output(request, context, "TODO.html")
