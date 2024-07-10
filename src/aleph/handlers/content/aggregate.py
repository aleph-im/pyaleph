import itertools
import logging
from typing import List, Sequence, Set, cast

from aleph_message.models import AggregateContent

from aleph.db.accessors.aggregates import (
    count_aggregate_elements,
    delete_aggregate,
    delete_aggregate_element,
    get_aggregate_by_key,
    get_aggregate_content_keys,
    insert_aggregate,
    insert_aggregate_element,
    mark_aggregate_as_dirty,
    merge_aggregate_elements,
    refresh_aggregate,
    update_aggregate,
)
from aleph.db.models import AggregateDb, AggregateElementDb, MessageDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidMessageFormat

LOGGER = logging.getLogger(__name__)


def _get_aggregate_content(message: MessageDb) -> AggregateContent:
    content = message.parsed_content
    if not isinstance(content, AggregateContent):
        raise InvalidMessageFormat(
            f"Unexpected content type for aggregate message: {message.item_hash}"
        )
    return content


class AggregateMessageHandler(ContentHandler):
    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        # Nothing to do, aggregates are independent of one another
        return

    @staticmethod
    async def _insert_aggregate_element(session: DbSession, message: MessageDb):
        content = cast(AggregateContent, message.parsed_content)
        aggregate_element = AggregateElementDb(
            item_hash=message.item_hash,
            key=content.key,
            owner=content.address,
            content=content.content,
            creation_datetime=timestamp_to_datetime(message.parsed_content.time),
        )

        insert_aggregate_element(
            session=session,
            item_hash=aggregate_element.item_hash,
            key=aggregate_element.key,
            owner=aggregate_element.owner,
            content=aggregate_element.content,
            creation_datetime=aggregate_element.creation_datetime,
        )

        return aggregate_element

    @staticmethod
    async def _append_to_aggregate(
        session: DbSession,
        aggregate: AggregateDb,
        elements: Sequence[AggregateElementDb],
    ):
        new_content = merge_aggregate_elements(elements)

        update_aggregate(
            session=session,
            key=aggregate.key,
            owner=aggregate.owner,
            content=new_content,
            last_revision_hash=elements[-1].item_hash,
            creation_datetime=aggregate.creation_datetime,
        )

    @staticmethod
    async def _prepend_to_aggregate(
        session: DbSession,
        aggregate: AggregateDb,
        elements: Sequence[AggregateElementDb],
    ):
        new_content = merge_aggregate_elements(elements)

        update_aggregate(
            session=session,
            key=aggregate.key,
            owner=aggregate.owner,
            content=new_content,
            last_revision_hash=aggregate.last_revision_hash,
            creation_datetime=elements[0].creation_datetime,
            prepend=True,
        )

    async def _update_aggregate(
        self,
        session: DbSession,
        key: str,
        owner: str,
        elements: Sequence[AggregateElementDb],
    ):
        """
        Creates/updates an aggregate with new elements.

        :param session: DB session.
        :param key: Aggregate key.
        :param owner: Aggregate owner.
        :param elements: New elements to insert, ordered by their creation_datetime field.
        :return:
        """

        # Let's forget about you for now
        if owner == "0x51A58800b26AA1451aaA803d1746687cB88E0501":
            return

        dirty_threshold = 1000

        aggregate_metadata = get_aggregate_by_key(
            session=session, owner=owner, key=key, with_content=False
        )

        if not aggregate_metadata:
            LOGGER.info("%s/%s does not exist, creating it", key, owner)

            content = merge_aggregate_elements(elements)
            insert_aggregate(
                session=session,
                key=key,
                owner=owner,
                content=content,
                creation_datetime=elements[0].creation_datetime,
                last_revision_hash=elements[-1].item_hash,
            )
            return

        if aggregate_metadata.dirty:
            LOGGER.info("%s/%s is dirty, skipping update", owner, key)
            return

        LOGGER.info("%s/%s already exists, updating it", owner, key)

        # Best case scenario: the elements we are adding are all posterior to the last
        # update, we can just merge the content of aggregate and the new elements.
        if (
            aggregate_metadata.last_revision.creation_datetime
            < elements[0].creation_datetime
        ):
            await self._append_to_aggregate(
                session=session, aggregate=aggregate_metadata, elements=elements
            )
            return

        # Similar case, all the new elements are anterior to the aggregate.
        if aggregate_metadata.creation_datetime > elements[-1].creation_datetime:
            await self._prepend_to_aggregate(
                session=session, aggregate=aggregate_metadata, elements=elements
            )
            return

        LOGGER.info("%s/%s: out of order refresh", owner, key)

        # Last chance before a full refresh, check the keys of the aggregate
        # and determine if there's a conflict.
        keys = set(get_aggregate_content_keys(session=session, key=key, owner=owner))
        new_keys = set(itertools.chain(element.content.keys for element in elements))
        conflicting_keys = keys & new_keys

        if not conflicting_keys:
            LOGGER.info("No conflicting keys for %s/%s, updating it", owner, key)
            await self._append_to_aggregate(
                session=session, aggregate=aggregate_metadata, elements=elements
            )
            return

        # One more case we can consider: if the last revision overwrote all the keys
        # from the last revision. After that, only a full refresh can solve the issue.
        last_revision_keys = set(aggregate_metadata.last_revision.content.keys())
        keys_requiring_refresh = new_keys - last_revision_keys
        if not keys_requiring_refresh:
            # A full refresh would yield the same aggregate, nothing to do.
            LOGGER.info("Outdated info, skipping refresh for %s/%s", owner, key)
            return

        if (
            count_aggregate_elements(session=session, owner=owner, key=key)
            > dirty_threshold
        ):
            LOGGER.info("%s/%s: too many elements, marking as dirty")
            mark_aggregate_as_dirty(session=session, owner=owner, key=key)
            return

        # Out of order insertions. Here, we need to get all the elements in the database
        # and recompute the aggregate entirely. This operation may be quite costly for
        # large aggregates, so we do it as a last resort.
        # Expect the new elements to already be added to the current session.
        # We flush it to make them accessible from the current transaction.
        session.flush()
        refresh_aggregate(session=session, owner=owner, key=key)

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        sorted_messages = sorted(
            messages,
            key=lambda m: (m.parsed_content.key, m.parsed_content.address, m.time),
        )

        for (key, owner), messages_by_aggregate in itertools.groupby(
            sorted_messages,
            key=lambda m: (m.parsed_content.key, m.parsed_content.address),
        ):
            aggregate_elements = [
                await self._insert_aggregate_element(session=session, message=message)
                for message in messages_by_aggregate
            ]

            await self._update_aggregate(
                session=session, key=key, owner=owner, elements=aggregate_elements
            )

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_aggregate_content(message)
        owner = content.address
        key = content.key

        LOGGER.debug("Deleting aggregate element %s...", message.item_hash)
        delete_aggregate(session=session, owner=owner, key=key)
        delete_aggregate_element(session=session, item_hash=message.item_hash)

        LOGGER.debug("Refreshing aggregate %s/%s...", owner, key)
        refresh_aggregate(session=session, owner=owner, key=key)

        return set()
