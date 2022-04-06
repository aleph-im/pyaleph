import json

from aiohttp import web
from aleph_message.models import MessageType
from bson import json_util
from pydantic import BaseModel, ValidationError

from aleph.model.messages import Message
from aleph.utils import trim_mongo_id


class GetProgramQueryFields(BaseModel):
    sort_order: int = -1

    class Config:
        extra = "forbid"


async def get_programs_on_message(request: web.Request) -> web.Response:
    try:
        query = GetProgramQueryFields(**request.query)
    except ValidationError as error:
        return web.json_response(
            data=error.json(), status=web.HTTPBadRequest.status_code
        )

    messages = [
        trim_mongo_id(msg)
        async for msg in Message.collection.find(
            filter={
                "type": MessageType.program,
                "content.on.message": {"$exists": True, "$not": {"$size": 0}},
            },
            sort=[("time", query.sort_order)],
            projection={
                "item_hash": 1,
                "content.on.message": 1,
            },
        )
    ]

    response = web.json_response(data=messages)
    response.enable_compression()
    return response
