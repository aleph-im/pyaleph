from typing import List, Optional

from aiohttp import web
from pydantic import BaseModel, validator, ValidationError

from aleph.model.messages import get_computed_address_aggregates
from .utils import LIST_FIELD_SEPARATOR


DEFAULT_LIMIT = 1000


class AggregatesQueryParams(BaseModel):
    keys: Optional[List[str]] = None
    limit: int = DEFAULT_LIMIT

    @validator(
        "keys",
        pre=True,
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


async def address_aggregate(request):
    """Returns the aggregate of an address.
    TODO: handle filter on a single key, or even subkey.
    """

    address = request.match_info["address"]

    try:
        query_params = AggregatesQueryParams.parse_obj(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    aggregates = await get_computed_address_aggregates(
        address_list=[address], key_list=query_params.keys, limit=query_params.limit
    )

    if not aggregates.get(address):
        return web.HTTPNotFound(text="No aggregate found for this address")

    output = {"address": address, "data": aggregates.get(address, {})}
    return web.json_response(output)
