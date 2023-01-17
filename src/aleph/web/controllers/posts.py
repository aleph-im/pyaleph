from typing import Optional, List, Any, Dict

from aiohttp import web
from aleph_message.models import ItemHash
from pydantic import BaseModel, Field, root_validator, validator, ValidationError

from aleph.db.accessors.posts import (
    get_matching_posts,
    MergedPost,
    count_matching_posts,
)
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder
from aleph.web.controllers.utils import (
    DEFAULT_MESSAGES_PER_PAGE,
    DEFAULT_PAGE,
    LIST_FIELD_SEPARATOR,
    Pagination,
    cond_output,
    get_path_page,
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
        default=None,
        alias="types",
        description="Accepted values for the 'content.type' field.",
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
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
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


def merged_post_to_dict(merged_post: MergedPost) -> Dict[str, Any]:
    return {
        "item_hash": merged_post.item_hash,
        "content": merged_post.content,
        "original_item_hash": merged_post.original_item_hash,
        "original_type": merged_post.original_type,
        "owner": merged_post.owner,
        "ref": merged_post.ref,
        "channel": merged_post.channel,
        "created": merged_post.created.isoformat(),
        "last_updated": merged_post.last_updated.isoformat(),
    }


async def view_posts_list(request):
    """Posts list view with filters"""

    query_string = request.query_string

    try:
        query_params = PostQueryParams.parse_obj(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(body=e.json(indent=4))

    path_page = get_path_page(request)
    if path_page:
        query_params.page = path_page

    find_filters = query_params.dict(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        # TODO: should return the count of matching posts
        total_posts = count_matching_posts(session=session, **find_filters)
        results = get_matching_posts(session=session, **find_filters)
        posts = [merged_post_to_dict(post) for post in results]

    context = {"posts": posts}

    if pagination_per_page is not None:

        pagination = Pagination(
            pagination_page,
            pagination_per_page,
            total_posts,
            url_base="/messages/posts/page/",
            query_string=query_string,
        )

        context.update(
            {
                "pagination": pagination,
                "pagination_page": pagination_page,
                "pagination_total": total_posts,
                "pagination_per_page": pagination_per_page,
                "pagination_item": "posts",
            }
        )

    return cond_output(request, context, "TODO.html")
