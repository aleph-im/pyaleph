import logging
from typing import Any, Dict, List, Mapping, Optional, Set, Union

from aleph_message.models import Chain, ChainRef, PostContent
from sqlalchemy import update

from aleph.db.accessors.balances import update_balances as update_balances_db
from aleph.db.accessors.balances import (
    update_credit_balances as update_credit_balances_db,
)
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


def update_credit_balances(session: DbSession, content: Mapping[str, Any]) -> None:
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

    update_credit_balances_db(
        session=session,
        credits_list=credits_list,
        token=token,
        chain=chain,
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
        credit_balances_post_type: str,
    ):
        self.balances_addresses = balances_addresses
        self.balances_post_type = balances_post_type
        self.credit_balances_addresses = credit_balances_addresses
        self.credit_balances_post_type = credit_balances_post_type

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
            content.type == self.credit_balances_post_type
            and content.address in self.credit_balances_addresses
            and content.content
        ):
            LOGGER.info("Updating credit balances...")
            update_credit_balances(session=session, content=content.content)
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
