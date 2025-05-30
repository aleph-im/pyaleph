import datetime as dt
import json
from typing import Dict, Iterable, List, Sequence

import pytest
import pytz
from aleph_message.models import AggregateContent, Chain, ItemType, MessageType
from configmanager import Config
from message_test_helpers import process_pending_messages
from more_itertools import one
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.aggregates import get_aggregate_by_key, get_aggregate_elements
from aleph.db.models import AggregateDb, AggregateElementDb, MessageDb, PendingMessageDb
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import AsyncDbSession, AsyncDbSessionFactory
from aleph.types.message_processing_result import ProcessedMessage


@pytest.mark.asyncio
async def test_process_aggregate_first_element(
    mocker,
    mock_config: Config,
    session_factory: AsyncDbSessionFactory,
    fixture_aggregate_messages: List[Dict],
):
    storage_service = StorageService(
        storage_engine=mocker.AsyncMock(),
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )
    signature_verifier = SignatureVerifier()
    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=storage_service,
        config=mock_config,
    )

    item_hash = "a87004aa03f8ae63d2c4bbe84b93b9ce70ca6482ce36c82ab0b0f689fc273f34"

    async with session_factory() as session:
        pending_message = (
            await session.execute(
                select(PendingMessageDb)
                .where(PendingMessageDb.item_hash == item_hash)
                .options(selectinload(PendingMessageDb.tx))
            )
        ).scalar_one()

        await message_handler.process(session=session, pending_message=pending_message)
        await session.commit()

    # Check the aggregate
    content = json.loads(pending_message.item_content)

    expected_key = content["key"]
    expected_creation_datetime = timestamp_to_datetime(content["time"])

    async with session_factory() as session:
        elements = list(
            await get_aggregate_elements(
                session=session, key=expected_key, owner=pending_message.sender
            )
        )
        assert len(elements) == 1
        element = elements[0]
        assert element.key == expected_key
        assert element.creation_datetime == expected_creation_datetime
        assert element.content == content["content"]

        aggregate = await get_aggregate_by_key(
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
    session_factory: AsyncDbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_aggregate_messages: List[Dict],
):
    pipeline = message_processor.make_pipeline()
    [message async for message in pipeline]

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
    session: AsyncDbSession,
    message_processor: PendingMessageProcessor,
    aggregates: Iterable[PendingMessageDb],
) -> Sequence[MessageDb]:
    messages = []
    for pending_aggregate in aggregates:
        session.add(pending_aggregate)
        await session.commit()

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
    session_factory: AsyncDbSessionFactory,
    message_processor: PendingMessageProcessor,
    aggregate_updates: Sequence[PendingMessageDb],
):
    async with session_factory() as session:
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
        aggregate = await get_aggregate_by_key(
            session=session, key=str(content.key), owner=content.address
        )
        assert aggregate

        assert aggregate.content == {"a": 1, "c": 3, "d": 4}


@pytest.mark.asyncio
async def test_process_aggregates_reverse_order(
    session_factory: AsyncDbSessionFactory,
    message_processor: PendingMessageProcessor,
    aggregate_updates: Sequence[PendingMessageDb],
):
    async with session_factory() as session:
        update, original = await process_aggregates_one_by_one(
            session=session,
            message_processor=message_processor,
            aggregates=reversed(aggregate_updates),
        )

        content = original.parsed_content
        assert isinstance(content, AggregateContent)
        aggregate = await get_aggregate_by_key(
            session=session, key=str(content.key), owner=content.address
        )
        assert aggregate

        assert aggregate.content == {"a": 1, "c": 3, "d": 4}


@pytest.mark.asyncio
async def test_delete_aggregate_one_element(
    mocker,
    session_factory: AsyncDbSessionFactory,
):
    async with session_factory() as session:
        element = AggregateElementDb(
            item_hash="d73d50b2d2c670d4c6c8e03ad0e4e2145642375f92784c68539a3400e0e4e242",
            key="my-aggregate",
            owner="0xme",
            content={"Hello": "world"},
            creation_datetime=dt.datetime(2023, 1, 1),
        )
        session.add(element)
        session.add(
            AggregateDb(
                key=element.key,
                owner=element.owner,
                content=element.content,
                creation_datetime=element.creation_datetime,
                last_revision_hash=element.item_hash,
                dirty=False,
            )
        )
        await session.commit()

        message = mocker.MagicMock()
        message.item_hash = element.item_hash
        message.parsed_content = AggregateContent(
            key=element.key,
            address=element.owner,
            time=element.creation_datetime.timestamp(),
            content=element.content,
        )

        aggregate_handler = AggregateMessageHandler()
        await aggregate_handler.forget_message(session=session, message=message)
        await session.commit()

        aggregate = get_aggregate_by_key(
            session=session, owner=element.owner, key=element.key
        )
        assert aggregate is None
        aggregate_elements = list(
            await get_aggregate_elements(
                session=session, owner=element.owner, key=element.key
            )
        )
        assert aggregate_elements == []


@pytest.mark.asyncio
@pytest.mark.parametrize("element_to_forget", ["first", "last"])
async def test_delete_aggregate_two_elements(
    mocker,
    session_factory: AsyncDbSessionFactory,
    element_to_forget: str,
):
    async with session_factory() as session:
        first_element = AggregateElementDb(
            item_hash="d73d50b2d2c670d4c6c8e03ad0e4e2145642375f92784c68539a3400e0e4e242",
            key="my-aggregate",
            owner="0xme",
            content={"Hello": "world"},
            creation_datetime=pytz.utc.localize(dt.datetime(2023, 1, 1)),
        )
        last_element = AggregateElementDb(
            item_hash="37a2ca64f9fdd35a2aac386003179c594b3f0963e064c0663ff84368bc3c1bb5",
            key=first_element.key,
            owner=first_element.owner,
            content={"Goodbye": "blue sky"},
            creation_datetime=pytz.utc.localize(dt.datetime(2023, 1, 2)),
        )
        session.add(first_element)
        session.add(last_element)
        session.add(
            AggregateDb(
                key=first_element.key,
                owner=first_element.owner,
                content={"Hello": "world", "Goodbye": "blue sky"},
                creation_datetime=first_element.creation_datetime,
                last_revision_hash=last_element.item_hash,
                dirty=False,
            )
        )
        await session.commit()

        if element_to_forget == "first":
            element_to_delete, element_to_keep = first_element, last_element
        else:
            element_to_delete, element_to_keep = last_element, first_element

        message = mocker.MagicMock()
        message.item_hash = element_to_delete.item_hash
        message.parsed_content = AggregateContent(
            key=element_to_delete.key,
            address=element_to_delete.owner,
            time=element_to_delete.creation_datetime.timestamp(),
            content=element_to_delete.content,
        )

        aggregate_handler = AggregateMessageHandler()
        await aggregate_handler.forget_message(session=session, message=message)
        await session.commit()

        aggregate = await get_aggregate_by_key(
            session=session, owner=first_element.owner, key=first_element.key
        )
        assert aggregate is not None
        assert not aggregate.dirty
        assert aggregate.owner == element_to_keep.owner
        assert aggregate.key == element_to_keep.key
        assert aggregate.content == element_to_keep.content
        assert aggregate.last_revision_hash == element_to_keep.item_hash
        assert aggregate.creation_datetime == element_to_keep.creation_datetime

        aggregate_elements = list(
            await get_aggregate_elements(
                session=session, owner=first_element.owner, key=first_element.key
            )
        )
        assert len(aggregate_elements) == 1
        element_db = aggregate_elements[0]

        assert element_db.owner == element_to_keep.owner
        assert element_db.key == element_to_keep.key
        assert element_db.content == element_to_keep.content
        assert element_db.creation_datetime == element_to_keep.creation_datetime
        assert element_db.content == element_to_keep.content
