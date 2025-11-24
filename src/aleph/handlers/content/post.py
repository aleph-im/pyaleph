import datetime as dt
import logging
from typing import Any, Dict, List, Mapping, Optional, Set, Union

from aleph_message.models import Chain, ChainRef, PostContent
from sqlalchemy import update

from aleph.db.accessors.balances import get_credit_balance
from aleph.db.accessors.balances import update_balances as update_balances_db
from aleph.db.accessors.balances import (
    update_credit_balances_distribution as update_credit_balances_distribution_db,
)
from aleph.db.accessors.balances import (
    update_credit_balances_expense as update_credit_balances_expense_db,
)
from aleph.db.accessors.balances import (
    update_credit_balances_transfer as update_credit_balances_transfer_db,
)
from aleph.db.accessors.balances import validate_credit_transfer_balance
from aleph.db.accessors.posts import (
    delete_amends,
    delete_post,
    get_matching_posts,
    get_original_post,
    refresh_latest_amend,
)
from aleph.db.models.messages import MessageDb
from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    AmendTargetNotFound,
    CannotAmendAmend,
    InternalError,
    InvalidMessageFormat,
    NoAmendTarget,
    PermissionDenied,
)

from .content_handler import ContentHandler

LOGGER = logging.getLogger(__name__)


def get_post_content(message: MessageDb) -> PostContent:
    content = message.parsed_content
    if not isinstance(content, PostContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for post message: {message.item_hash}"
        )
    return content


def update_balances(session: DbSession, content: Mapping[str, Any]) -> None:
    try:
        chain = Chain(content["chain"])
        height = content["main_height"]
    except KeyError:
        raise InvalidMessageFormat(
            "Missing field(s) chain and/or main_height for balance post"
        )
    dapp = content.get("dapp")

    LOGGER.info("Updating balances for %s (dapp: %s)", chain, dapp)

    balances: Dict[str, float] = content["balances"]
    update_balances_db(
        session=session,
        chain=chain,
        dapp=dapp,
        eth_height=height,
        balances=balances,
    )


def update_credit_balances_distribution(
    session: DbSession,
    content: Mapping[str, Any],
    message_hash: str,
    message_timestamp: dt.datetime,
) -> None:
    try:
        distribution = content["distribution"]
        credits_list = distribution["credits"]
        token = distribution["token"]
        chain = distribution["chain"]
    except KeyError as e:
        raise InvalidMessageFormat(
            f"Missing field '{e.args[0]}' for credit balance post"
        )

    LOGGER.info("Updating credit balances for %d addresses", len(credits_list))

    update_credit_balances_distribution_db(
        session=session,
        credits_list=credits_list,
        token=token,
        chain=chain,
        message_hash=message_hash,
        message_timestamp=message_timestamp,
    )


def update_credit_balances_expense(
    session: DbSession,
    content: Mapping[str, Any],
    message_hash: str,
    message_timestamp: dt.datetime,
) -> None:
    try:
        expense = content["expense"]
        credits_list = expense["credits"]
    except KeyError as e:
        raise InvalidMessageFormat(
            f"Missing field '{e.args[0]}' for credit expense post"
        )

    LOGGER.info("Updating credit balances expense for %d addresses", len(credits_list))

    update_credit_balances_expense_db(
        session=session,
        credits_list=credits_list,
        message_hash=message_hash,
        message_timestamp=message_timestamp,
    )


def update_credit_balances_transfer(
    session: DbSession,
    content: Mapping[str, Any],
    message_hash: str,
    message_timestamp: dt.datetime,
    sender_address: str,
    whitelisted_addresses: List[str],
) -> None:
    try:
        transfer = content["transfer"]
        credits_list = transfer["credits"]
    except KeyError as e:
        raise InvalidMessageFormat(
            f"Missing field '{e.args[0]}' for credit transfer post"
        )

    # Only validate if sender is not in the whitelisted addresses
    if sender_address not in whitelisted_addresses:
        # Calculate total transfer amount for validation
        total_amount = sum(int(credit["amount"]) for credit in credits_list)

        if not validate_credit_transfer_balance(session, sender_address, total_amount):
            raise InvalidMessageFormat(
                f"Insufficient credit balance for transfer. Required: {total_amount}, Available: {get_credit_balance(session, sender_address)}"
            )

    LOGGER.info(
        "Updating credit balances transfer for %d recipients", len(credits_list)
    )

    update_credit_balances_transfer_db(
        session=session,
        credits_list=credits_list,
        sender_address=sender_address,
        whitelisted_addresses=whitelisted_addresses,
        message_hash=message_hash,
        message_timestamp=message_timestamp,
    )


def get_post_content_ref(ref: Optional[Union[ChainRef, str]]) -> Optional[str]:
    return ref.item_hash if isinstance(ref, ChainRef) else ref


class PostMessageHandler(ContentHandler):
    """
    Handler for POST messages. Posts are simple JSON objects posted by users.
    They can be updated (=amended) by subsequent POSTs using the following rules:

    * the amending post replaces the content of the original entirely
    * the content.type field of the amending post is set to "amend"
    * the content.ref field of the amending post is set to the item hash of
      the original post.

    These rules make POSTs slightly different from AGGREGATEs as the whole content
    is overwritten by amending messages. This handler unpacks the content of each
    POST message and puts it in the `posts` table. Readers are expected to find
    the last version of a post on their own using a DB query. We keep each amend
    in case a user decides to delete a version with a FORGET.
    """

    def __init__(
        self,
        balances_addresses: List[str],
        balances_post_type: str,
        credit_balances_addresses: List[str],
        credit_balances_post_types: List[str],
        credit_balances_channels: List[str],
    ):
        self.balances_addresses = balances_addresses
        self.balances_post_type = balances_post_type
        self.credit_balances_addresses = credit_balances_addresses
        self.credit_balances_post_types = credit_balances_post_types
        self.credit_balances_channels = credit_balances_channels

    async def check_dependencies(self, session: DbSession, message: MessageDb):
        content = get_post_content(message)

        # For amends, ensure that the original message exists
        if content.type == "amend":
            ref = get_post_content_ref(content.ref)

            if ref is None:
                raise NoAmendTarget()

            original_post = get_original_post(session=session, item_hash=ref)
            if not original_post:
                raise AmendTargetNotFound()

            if original_post.type == "amend":
                raise CannotAmendAmend()

    async def check_permissions(self, session: DbSession, message: MessageDb) -> None:
        """
        Check permissions for POST messages.

        For amend messages, ensures that the amend message has the same content address
        as the original post being amended. This prevents users from amending posts
        that don't belong to the same address.
        """
        # First, perform the standard authorization check
        await super().check_permissions(session=session, message=message)

        content = get_post_content(message)

        # Additional check for amend messages: ensure same owner
        if content.type == "amend":
            ref = get_post_content_ref(content.ref)

            if ref is not None:
                original_post = get_original_post(session=session, item_hash=ref)

                if original_post is not None:
                    # Check that the amend message has the same address as the original post
                    if original_post.owner != content.address:
                        raise PermissionDenied(
                            f"Cannot amend post {ref}: amend message address {content.address} "
                            f"does not match original post owner {original_post.owner}"
                        )

    async def process_post(self, session: DbSession, message: MessageDb):
        content = get_post_content(message)

        creation_datetime = timestamp_to_datetime(content.time)
        ref = get_post_content_ref(content.ref)

        post = PostDb(
            item_hash=message.item_hash,
            owner=content.address,
            type=content.type,
            ref=ref,
            amends=ref if content.type == "amend" else None,
            channel=message.channel,
            content=content.content,
            creation_datetime=creation_datetime,
        )
        session.add(post)

        if content.type == "amend":
            [amended_post] = get_matching_posts(session=session, hashes=[ref])

            if amended_post.last_updated < creation_datetime:
                session.execute(
                    update(PostDb)
                    .where(PostDb.item_hash == ref)
                    .values(latest_amend=message.item_hash)
                )

        if (
            content.type == self.balances_post_type
            and content.address in self.balances_addresses
            and content.content
        ):
            LOGGER.info("Updating balances...")
            update_balances(session=session, content=content.content)
            LOGGER.info("Done updating balances")

        if (
            content.type in self.credit_balances_post_types
            and content.address in self.credit_balances_addresses
            and (
                not self.credit_balances_channels
                or message.channel in self.credit_balances_channels
            )
            and content.content
        ):
            LOGGER.info("Updating credit balances...")
            if content.type == "aleph_credit_distribution":
                update_credit_balances_distribution(
                    session=session,
                    content=content.content,
                    message_hash=message.item_hash,
                    message_timestamp=creation_datetime,
                )
            elif content.type == "aleph_credit_expense":
                update_credit_balances_expense(
                    session=session,
                    content=content.content,
                    message_hash=message.item_hash,
                    message_timestamp=creation_datetime,
                )
            elif content.type == "aleph_credit_transfer":
                update_credit_balances_transfer(
                    session=session,
                    content=content.content,
                    message_hash=message.item_hash,
                    message_timestamp=creation_datetime,
                    sender_address=content.address,
                    whitelisted_addresses=self.credit_balances_addresses,
                )
            LOGGER.info("Done updating credit balances")

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:

        for message in messages:
            await self.process_post(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = get_post_content(message)

        LOGGER.debug("Deleting post %s...", message.item_hash)
        amend_hashes = delete_amends(session=session, item_hash=message.item_hash)
        delete_post(session=session, item_hash=message.item_hash)

        if content.type == "amend":
            original_post = get_original_post(session, str(content.ref))
            if original_post is None:
                raise InternalError(
                    f"Could not find original post ({content.ref} for amend ({message.item_hash})."
                )

            if original_post.latest_amend == message.item_hash:
                refresh_latest_amend(session, original_post.item_hash)

        return set(amend_hashes)
