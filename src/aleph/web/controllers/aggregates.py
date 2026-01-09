import datetime as dt
import logging
from typing import Dict, List, Optional

from aiohttp import web
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import select

from aleph.db.accessors.aggregates import (
    get_aggregates_by_owner,
    refresh_aggregate,
    get_aggregates,
    count_aggregates,
)
from aleph.db.models import AggregateDb
from aleph.schemas.messages_query_params import (
    LIST_FIELD_SEPARATOR,
    DEFAULT_MESSAGES_PER_PAGE,
)
from aleph.types.sort_order import SortOrder, SortByAggregate
from aleph.web.controllers.app_state_getters import get_session_factory_from_request

LOGGER = logging.getLogger(__name__)

DEFAULT_LIMIT = 1000


class AggregatesQueryParams(BaseModel):
    keys: Optional[List[str]] = None
    limit: int = DEFAULT_LIMIT
    with_info: bool = Field(default=False, alias="with_info")
    value_only: bool = Field(default=False, alias="value_only")

    @field_validator("keys", mode="before")
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


class AggregatesListQueryParams(BaseModel):
    keys: Optional[List[str]] = None
    addresses: Optional[List[str]] = None
    sort_by: SortByAggregate = Field(default=SortByAggregate.LAST_MODIFIED, alias="sortBy")
    sort_order: SortOrder = Field(default=SortOrder.DESCENDING, alias="sortOrder")
    pagination: int = Field(default=DEFAULT_MESSAGES_PER_PAGE, alias="pagination")
    page: int = Field(default=1, alias="page")

    @field_validator("keys", "addresses", mode="before")
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


async def address_aggregate(request: web.Request) -> web.Response:
    """Returns the aggregate of an address.
    TODO: handle filter on a single key, or even subkey.
    """

    address: str = request.match_info["address"]

    try:
        query_params = AggregatesQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )
    session_factory = request.app["session_factory"]
    with session_factory() as session:
        dirty_aggregates = session.execute(
            select(AggregateDb.key).where(
                (AggregateDb.owner == address)
                & (AggregateDb.owner == address)
                & AggregateDb.dirty
            )
        ).scalars()
        for key in dirty_aggregates:
            LOGGER.info("Refreshing dirty aggregate %s/%s", address, key)
            refresh_aggregate(session=session, owner=address, key=key)
            session.commit()

        aggregates = list(
            get_aggregates_by_owner(
                session=session,
                owner=address,
                with_info=query_params.with_info,
                keys=query_params.keys,
            )
        )

    if not aggregates:
        raise web.HTTPNotFound(text="No aggregate found for this address")

    if query_params.value_only and query_params.keys and len(query_params.keys) == 1:
        output = {}
        target_key = query_params.keys[0]
        for result in aggregates:
            output[result[0]] = result[1]

        return web.json_response(output[target_key])

    output = {
        "address": address,
        "data": {},
    }

    info: Dict = {}
    data: Dict = {}

    for result in aggregates:
        data[result[0]] = result[1]
        if query_params.with_info:
            (
                aggregate_key,
                content,
                created,
                last_updated,
                original_item_hash,
                last_update_item_hash,
            ) = result

            if isinstance(created, dt.datetime):
                created = created.isoformat()
            if isinstance(last_updated, dt.datetime):
                last_updated = last_updated.isoformat()
            info[aggregate_key] = {
                "created": str(created),
                "last_updated": str(last_updated),
                "original_item_hash": str(original_item_hash),
                "last_update_item_hash": str(last_update_item_hash),
            }

    output["data"] = data
    output["info"] = info

    return web.json_response(output)


async def view_aggregates_list(request: web.Request) -> web.Response:
    try:
        query_params = AggregatesListQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        aggregates = get_aggregates(
            session=session,
            keys=query_params.keys,
            addresses=query_params.addresses,
            sort_by=query_params.sort_by,
            sort_order=query_params.sort_order,
            page=query_params.page,
            pagination=query_params.pagination,
        )

        total_aggregates = count_aggregates(
            session=session,
            keys=query_params.keys,
            addresses=query_params.addresses,
        )

        output = {
            "aggregates": [
                {
                    "address": aggregate.owner,
                    "key": aggregate.key,
                    "content": aggregate.content,
                    "created": aggregate.creation_datetime.isoformat(),
                    "last_updated": aggregate.last_revision.creation_datetime.isoformat(),
                }
                for aggregate in aggregates
            ],
            "pagination_per_page": query_params.pagination,
            "pagination_page": query_params.page,
            "pagination_total": total_aggregates,
            "pagination_item": "aggregates",
        }

    return web.json_response(output)
