import logging
import datetime as dt
from typing import List, Optional, Any, Dict, Tuple, Literal, Sequence

from aiohttp import web
from pydantic import BaseModel, validator, ValidationError
from sqlalchemy import select

from aleph.db.accessors.aggregates import (
    get_aggregates_by_owner,
    refresh_aggregate,
)
from aleph.db.models import AggregateDb
from .utils import LIST_FIELD_SEPARATOR

LOGGER = logging.getLogger(__name__)

DEFAULT_LIMIT = 1000


class AggregatesQueryParams(BaseModel):
    keys: Optional[List[str]] = None
    limit: int = DEFAULT_LIMIT
    with_info: bool = False

    @validator(
        "keys",
        pre=True,
    )
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
        query_params = AggregatesQueryParams.parse_obj(request.query)
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
                session=session, owner=address, with_info=query_params.with_info
            )
        )

        if not aggregates:
            return web.HTTPNotFound(text="No aggregate found for this address")

    output = {
        "address": address,
        "data": {result[0]: result[1] for result in aggregates},
    }
    info = {}
    if query_params.with_info:
        for result in aggregates:
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
                "created": created,
                "last_updated": last_updated,
                "original_item_hash": original_item_hash,
                "last_update_item_hash": last_update_item_hash,
            }
        output["info"] = info

    return web.json_response(output)
