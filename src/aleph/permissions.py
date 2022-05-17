from aleph.model.messages import get_computed_address_aggregates
from aleph.schemas.validated_message import (
    BaseValidatedMessage,
    ValidatedPostMessage,
    ValidatedAggregateMessage,
)


async def check_sender_authorization(message: BaseValidatedMessage) -> bool:
    """Checks a content against a message to verify if sender is authorized.

    TODO: implement "security" aggregate key check.
    """

    sender = message.sender
    address = message.content.address

    # if sender is the content address, all good.
    if sender == address:
        return True

    aggregates = await get_computed_address_aggregates(
        address_list=[address], key_list=["security"]
    )  # do we need anything else here?

    aggregate = aggregates.get(address, {})
    security_key = aggregate.get("security", {})
    authorizations = security_key.get("authorizations", [])

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

        if isinstance(message, ValidatedPostMessage):
            if len(ptypes) and message.content.type not in ptypes:
                continue

        if isinstance(message, ValidatedAggregateMessage):
            if len(akeys) and message.content.key not in akeys:
                continue

        return True

    return False
