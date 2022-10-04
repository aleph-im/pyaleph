import json
from itertools import groupby
from typing import Any, Dict

from aiohttp import web
from aleph_message.models import MessageType

from aleph.db.accessors.messages import get_message_stats_by_sender
from aleph.types.db_session import DbSessionFactory


def make_stats_dict(stats) -> Dict[str, Any]:
    stats_dict = {}

    sorted_stats = sorted(stats, key=lambda s: s.sender)
    for sender, sender_stats in groupby(sorted_stats, key=lambda s: s.sender):
        nb_messages_by_type = {s.type: s.nb_messages for s in sender_stats}
        stats_dict[sender] = {
            "messages": sum(val for val in nb_messages_by_type.values()),
            "aggregates": nb_messages_by_type.get(MessageType.aggregate, 0),
            "posts": nb_messages_by_type.get(MessageType.post, 0),
            "programs": nb_messages_by_type.get(MessageType.program, 0),
            "stores": nb_messages_by_type.get(MessageType.store, 0),
        }

    return stats_dict


async def addresses_stats_view(request):
    """Returns the stats of some addresses."""

    addresses = request.query.getall("addresses[]", [])
    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        stats = get_message_stats_by_sender(session=session, addresses=addresses)

    stats_dict = make_stats_dict(stats)

    output = {"data": stats_dict}
    return web.json_response(
        output, dumps=lambda v: json.dumps(v)
    )
