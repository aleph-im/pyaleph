from aleph_message.models import ItemHash, MessageType, PostContent

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.models import MessageDb
from aleph.types.db_session import DbSession


def is_sender_authorized_for_owner(
    session: DbSession, sender: str, owner_address: str, message: MessageDb
) -> bool:
    """Check whether `sender` is authorized to act for `owner_address` per
    the security aggregate.

    `message` is the message whose attributes scope the authorization
    filters: the aggregate's `types`, `channels`, `chain`, `post_types`,
    and `aggregate_keys` are matched against `message.type`,
    `message.channel`, `message.chain`, and (for POST/AGGREGATE)
    `message.parsed_content.type` / `.key`. Two patterns are valid:

    1. Authorize an inbound operation. Pass the message being submitted
       (e.g. a POST being created). The check answers: "is `sender`
       allowed to submit this on behalf of `owner_address`?" This is
       how `check_sender_authorization` uses it.

    2. Authorize an action against an existing message. Pass the target
       message (e.g. the message being forgotten). The check answers:
       "would `sender` have been authorized to create this target under
       `owner_address`'s aggregate?" This is how
       `ForgetMessageHandler.check_permissions` uses it, expressing the
       rule that the right to forget content follows the right to
       create it.

    In both cases the self-equality short-circuit at the top applies:
    if `sender == owner_address`, the sender is acting for themselves
    and no aggregate lookup is needed.
    """

    if sender.lower() == owner_address.lower():
        return True

    aggregate = get_aggregate_by_key(
        session=session, key="security", owner=owner_address
    )

    if not aggregate:
        return False

    authorizations = aggregate.content.get("authorizations", [])

    for auth in authorizations:
        if auth.get("address", "").lower() != sender.lower():
            continue

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


async def check_sender_authorization(session: DbSession, message: MessageDb) -> bool:
    """Checks a content against a message to verify if sender is authorized.

    For POST messages with type="amend", this function checks permissions against
    the original post message instead of the amend message itself. This ensures
    that delegated accounts can only amend posts they originally had permission
    to create or that were created by accounts they have delegation for.

    Special behavior for amend messages:
    - If the message is a POST with type="amend" and has a ref to an original post,
      the function recursively checks authorization against the original message
    - If the original message is not found, it falls back to standard permission checking
    - No special "amend" permission is required; if you can post as an address,
      you can amend posts from that address

    Args:
        session: Database session for querying
        message: The message to check authorization for

    Returns:
        True if the sender is authorized, False otherwise
    """

    content = message.parsed_content

    sender = message.sender
    address = content.address

    # if sender is the content address, all good.
    if sender.lower() == address.lower():
        return True

    # Special handling for POST amend messages
    if (
        message.type == MessageType.post
        and isinstance(content, PostContent)
        and content.type == "amend"
    ):
        # For amends, we need to check if the current sender has permissions for the original post's address
        if content.ref is not None:
            ref_item_hash: ItemHash = (
                content.ref.item_hash
                if hasattr(content.ref, "item_hash")
                else ItemHash(content.ref)
            )
            original_message = get_message_by_item_hash(
                session=session, item_hash=ref_item_hash
            )

            if original_message is not None:
                # Create a mock message with the current sender but original message's content address
                # This allows us to check if the current sender has permission for the original address
                original_content = original_message.parsed_content
                if hasattr(original_content, "address"):
                    # Check permissions for current sender against original post's address
                    original_address = original_content.address

                    # Check new owner is the same than the original
                    if address.lower() != original_address.lower():
                        return False

                    # Check delegated permissions for original address
                    return is_sender_authorized_for_owner(
                        session=session,
                        sender=sender,
                        owner_address=original_address,
                        message=original_message,
                    )

    return is_sender_authorized_for_owner(
        session=session, sender=sender, owner_address=address, message=message
    )
