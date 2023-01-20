import datetime as dt
import json
from typing import Dict, List, Sequence, Iterable

import pytest
from aleph_message.models import ItemType, Chain, MessageType, AggregateContent
from configmanager import Config
from more_itertools import one
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from aleph.db.accessors.aggregates import get_aggregate_by_key, get_aggregate_elements
from aleph.db.models import PendingMessageDb, MessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.job_utils import ProcessedMessage
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory, DbSession
from message_test_helpers import process_pending_messages


@pytest.mark.asyncio
async def test_process_aggregate_first_element(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    fixture_aggregate_messages: List[Dict],
):
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(), ipfs_service=mocker.AsyncMock()
    )
    chain_service = mocker.AsyncMock()
    message_handler = MessageHandler(
        session_factory=session_factory,
        chain_service=chain_service,
        storage_service=storage_service,
        config=mock_config,
    )

    item_hash = "a87004aa03f8ae63d2c4bbe84b93b9ce70ca6482ce36c82ab0b0f689fc273f34"

    with session_factory() as session:
        pending_message = (
            session.execute(
                select(PendingMessageDb)
                .where(PendingMessageDb.item_hash == item_hash)
                .options(selectinload(PendingMessageDb.tx))
            )
        ).scalar_one()

    await message_handler.fetch_and_process_one_message_db(
        pending_message=pending_message
    )

    # Check the aggregate
    content = json.loads(pending_message.item_content)

    expected_key = content["key"]
    expected_creation_datetime = timestamp_to_datetime(content["time"])

    with session_factory() as session:
        elements = list(
            get_aggregate_elements(
                session=session, key=expected_key, owner=pending_message.sender
            )
        )
        assert len(elements) == 1
        element = elements[0]
        assert element.key == expected_key
        assert element.creation_datetime == expected_creation_datetime
        assert element.content == content["content"]

        aggregate = get_aggregate_by_key(
            session=session,
            owner=pending_message.sender,
            key=expected_key,
        )

        assert aggregate
        assert aggregate.key == expected_key
        assert aggregate.owner == pending_message.sender
        assert aggregate.content == content["content"]
        assert aggregate.creation_datetime == expected_creation_datetime
        assert aggregate.last_revision_hash == element.item_hash


@pytest.mark.asyncio
async def test_process_aggregates(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_aggregate_messages: List[Dict],
):
    pipeline = message_processor.make_pipeline()
    messages = [message async for message in pipeline]

    # TODO: improve this test


@pytest.fixture()
def aggregate_updates() -> Sequence[PendingMessageDb]:
    original = PendingMessageDb(
        item_hash="53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c",
        chain=Chain.ETH,
        sender="0x720F319A9c3226dCDd7D8C49163D79EDa1084E98",
        signature="0x7eee4cfc03b963ec51f04f60f6f7d58b0f24e0309d209feecb55af9e411ed1c01cfb547bb13539e91308b044c3661d93ddf272426542bc1a47722614cb0cd3621c",
        item_type=ItemType.inline,
        type=MessageType.aggregate,
        item_content='{"address":"0x720F319A9c3226dCDd7D8C49163D79EDa1084E98","time":1644857371.391834,"key":"test_reference","content":{"a":1,"c":2}}',
        channel=Channel("INTEGRATION_TESTS"),
        time=timestamp_to_datetime(1644859283.101),
        check_message=True,
        fetched=True,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
        reception_time=dt.datetime(2022, 1, 1),
    )
    update = PendingMessageDb(
        item_hash="0022ed09d16a1c3d6cbb3c7e2645657ebaa0382eba65be06264b106f528b85bf",
        chain=Chain.ETH,
        sender="0x720F319A9c3226dCDd7D8C49163D79EDa1084E98",
        signature="0xe6129196c36b066302692b53bcb78a9d8c996854b170238ebfe56924f0b6be604883c30a66d75250de489e1edb683c7da8ddb1ccb50a39d1bbbdad617e5c958f1b",
        item_type=ItemType.inline,
        type=MessageType.aggregate,
        item_content='{"address":"0x720F319A9c3226dCDd7D8C49163D79EDa1084E98","time":1644857704.6253593,"key":"test_reference","content":{"c":3,"d":4}}',
        channel=Channel("INTEGRATION_TESTS"),
        time=timestamp_to_datetime(1644859283.12),
        check_message=True,
        fetched=True,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1, 1, 0, 0),
        reception_time=dt.datetime(2022, 1, 1),
    )

    return original, update


async def process_aggregates_one_by_one(
    session: DbSession,
    message_processor: PendingMessageProcessor,
    aggregates: Iterable[PendingMessageDb],
) -> Sequence[MessageDb]:

    messages = []
    for pending_aggregate in aggregates:
        session.add(pending_aggregate)
        session.commit()

        result = one(
            await process_pending_messages(
                message_processor=message_processor,
                pending_messages=[pending_aggregate],
                session=session,
            )
        )
        assert isinstance(result, ProcessedMessage)
        messages.append(result.message)

    return messages


@pytest.mark.asyncio
async def test_process_aggregates_in_order(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    aggregate_updates: Sequence[PendingMessageDb],
):
    with session_factory() as session:
        original, update = await process_aggregates_one_by_one(
            session=session,
            message_processor=message_processor,
            aggregates=aggregate_updates,
        )

        # Sanity check
        assert original.item_hash == aggregate_updates[0].item_hash
        assert update.item_hash == aggregate_updates[1].item_hash

        content = original.parsed_content
        assert isinstance(content, AggregateContent)
        aggregate = get_aggregate_by_key(
            session=session, key=content.key, owner=content.address
        )
        assert aggregate

        assert aggregate.content == {"a": 1, "c": 3, "d": 4}


@pytest.mark.asyncio
async def test_process_aggregates_reverse_order(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    aggregate_updates: Sequence[PendingMessageDb],
):
    with session_factory() as session:
        update, original = await process_aggregates_one_by_one(
            session=session,
            message_processor=message_processor,
            aggregates=reversed(aggregate_updates),
        )

        content = original.parsed_content
        assert isinstance(content, AggregateContent)
        aggregate = get_aggregate_by_key(
            session=session, key=content.key, owner=content.address
        )
        assert aggregate

        assert aggregate.content == {"a": 1, "c": 3, "d": 4}
