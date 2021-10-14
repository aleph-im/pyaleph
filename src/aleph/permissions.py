from typing import Dict

from aleph.model.messages import get_computed_address_aggregates


async def check_sender_authorization(message: Dict, content: Dict) -> bool:
    """Checks a content against a message to verify if sender is authorized.

    TODO: implement "security" aggregate key check.
    """

    sender = message["sender"]
    address = content["address"]

    # if sender is the content address, all good.
    if sender == address:
        return True

    aggregates = await get_computed_address_aggregates(
        address_list=[address], key_list=["security"]
    )  # do we need anything else here?

    aggregate = aggregates.get(address, {})
    security_key = aggregate.get("security", {})
    authorizations = security_key.get("authorizations")

    for auth in authorizations:
        if auth.get("address", "") != sender:
            continue  # not applicable, move on.

        if auth.get("chain") and message["chain"] != auth.get("chain"):
            continue

        channels = auth.get("channels", [])
        mtypes = auth.get("types", [])
        ptypes = auth.get("post_types", [])
        akeys = auth.get("aggregate_keys", [])

        if len(channels) and message["channel"] not in channels:
            continue

        if len(mtypes) and message["type"] not in mtypes:
            continue

        if message["type"] == "POST":
            if len(ptypes) and content["type"] not in ptypes:
                continue

        if message["type"] == "AGGREGATE":
            if len(akeys) and content["key"] not in akeys:
                continue

        return True

    return False
