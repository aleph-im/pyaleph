from aleph_message.models import MessageType

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.models import MessageDb
from aleph.types.db_session import AsyncDbSession


async def check_sender_authorization(
    session: AsyncDbSession, message: MessageDb
) -> bool:
    """Checks a content against a message to verify if sender is authorized.

    TODO: implement "security" aggregate key check.
    """

    content = message.parsed_content

    sender = message.sender
    address = content.address

    # if sender is the content address, all good.
    if sender == address:
        return True

    aggregate = await get_aggregate_by_key(
        session=session, key="security", owner=address
    )  # do we need anything else here?

    if not aggregate:
        return False

    authorizations = aggregate.content.get("authorizations", [])

    for auth in authorizations:
        if auth.get("address", "") != sender:
            continue  # not applicable, move on.

        if auth.get("chain") and message.chain != auth.get("chain"):
            continue

        channels = auth.get("channels", [])
        mtypes = auth.get("types", [])
        ptypes = auth.get("post_types", [])
        akeys = auth.get("aggregate_keys", [])

        if len(channels) and message.channel not in channels:
            continue

        if len(mtypes) and message.type not in mtypes:
            continue

        if message.type == MessageType.post:
            if len(ptypes) and message.parsed_content.type not in ptypes:
                continue

        if message.type == MessageType.aggregate:
            if len(akeys) and message.parsed_content.key not in akeys:
                continue

        return True

    return False
