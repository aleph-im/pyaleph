import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple, cast

import pytest
import pytest_asyncio
from aleph_message.models import AggregateContent, PostContent
from configmanager import Config
from in_memory_storage_engine import InMemoryStorageEngine
from sqlalchemy import insert

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.aggregates import refresh_aggregate
from aleph.db.models import (
    AggregateElementDb,
    ChainTxDb,
    MessageDb,
    message_confirmations,
)
from aleph.db.models.posts import PostDb
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import AsyncDbSessionFactory


# TODO: remove the raw parameter, it's just to avoid larger refactorings
async def _load_fixtures(
    session_factory: AsyncDbSessionFactory, filename: str, raw: bool = True
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    async with fixtures_file.open() as f:
        messages_json = json.load(f)

    messages = []
    tx_hashes = set()

    async with session_factory() as session:
        for message_dict in messages_json:
            message_db = MessageDb.from_message_dict(message_dict)
            messages.append(message_db)
            session.add(message_db)
            for confirmation in message_dict.get("confirmations", []):
                if (tx_hash := confirmation["hash"]) not in tx_hashes:
                    chain_tx_db = ChainTxDb.from_dict(confirmation)
                    tx_hashes.add(tx_hash)
                    session.add(chain_tx_db)

                await session.flush()
                await session.execute(
                    insert(message_confirmations).values(
                        item_hash=message_db.item_hash, tx_hash=tx_hash
                    )
                )
        await session.commit()

    return messages_json if raw else messages


@pytest_asyncio.fixture
async def fixture_messages(
    session_factory: AsyncDbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "fixture_messages.json")


def make_aggregate_element(message: MessageDb) -> AggregateElementDb:
    content = cast(AggregateContent, message.parsed_content)
    aggregate_element = AggregateElementDb(
        key=content.key,
        owner=content.address,
        content=content.content,
        item_hash=message.item_hash,
        creation_datetime=timestamp_to_datetime(content.time),
    )

    return aggregate_element


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: AsyncDbSessionFactory,
) -> Sequence[MessageDb]:
    messages = await _load_fixtures(
        session_factory, "fixture_aggregates.json", raw=False
    )
    aggregate_keys = set()
    async with session_factory() as session:
        for message in messages:
            aggregate_element = make_aggregate_element(message)  # type: ignore
            session.add(aggregate_element)
            aggregate_keys.add((aggregate_element.owner, aggregate_element.key))
        await session.commit()

        for owner, key in aggregate_keys:
            await refresh_aggregate(session=session, owner=owner, key=key)

        await session.commit()

    return messages  # type: ignore


def make_post_db(message: MessageDb) -> PostDb:
    content = cast(PostContent, message.parsed_content)
    return PostDb(
        item_hash=message.item_hash,
        owner=content.address,
        type=content.type,
        ref=content.ref,
        amends=content.ref if content.type == "amend" else None,
        channel=message.channel,
        content=content.content,
        creation_datetime=timestamp_to_datetime(content.time),
    )


@pytest_asyncio.fixture
async def fixture_posts(
    session_factory: AsyncDbSessionFactory,
) -> Sequence[PostDb]:
    messages = await _load_fixtures(session_factory, "fixture_posts.json", raw=False)
    posts = [make_post_db(message) for message in messages]  # type: ignore

    async with session_factory() as session:
        session.add_all(posts)
        await session.commit()

    return posts


@pytest.fixture
def post_with_refs_and_tags() -> Tuple[MessageDb, PostDb]:
    message = MessageDb(
        item_hash="1234",
        sender="0xdeadbeef",
        type="POST",
        chain="ETH",
        signature=None,
        item_type="storage",
        item_content=None,
        content={"content": {"tags": ["original", "mainnet"], "swap": "this"}},
        time=dt.datetime(2023, 5, 1, tzinfo=dt.timezone.utc),
        channel=None,
        size=254,
    )

    post = PostDb(
        item_hash=message.item_hash,
        owner=message.sender,
        type=None,
        ref="custom-ref",
        amends=None,
        channel=None,
        content=message.content["content"],
        creation_datetime=message.time,
        latest_amend=None,
    )

    return message, post


@pytest.fixture
def amended_post_with_refs_and_tags(post_with_refs_and_tags: Tuple[MessageDb, PostDb]):
    original_message, original_post = post_with_refs_and_tags

    amend_message = MessageDb(
        item_hash="5678",
        sender="0xdeadbeef",
        type="POST",
        chain="ETH",
        signature=None,
        item_type="storage",
        item_content=None,
        content={"content": {"tags": ["amend", "mainnet"], "don't": "swap"}},
        time=dt.datetime(2023, 5, 2, tzinfo=dt.timezone.utc),
        channel=None,
        size=277,
    )

    amend_post = PostDb(
        item_hash=amend_message.item_hash,
        owner=original_message.sender,
        type="amend",
        ref=original_message.item_hash,
        amends=original_message.item_hash,
        channel=None,
        content=amend_message.content["content"],
        creation_datetime=amend_message.time,
        latest_amend=None,
    )

    return amend_message, amend_post


@pytest.fixture
def message_processor(
    mocker, mock_config: Config, session_factory: AsyncDbSessionFactory
):
    storage_engine = InMemoryStorageEngine(files={})
    storage_service = StorageService(
        storage_engine=storage_engine,
        ipfs_service=mocker.AsyncMock(),
        node_cache=mocker.AsyncMock(),
    )
    signature_verifier = SignatureVerifier()
    message_handler = MessageHandler(
        signature_verifier=signature_verifier,
        storage_service=storage_service,
        config=mock_config,
    )
    message_processor = PendingMessageProcessor(
        session_factory=session_factory,
        message_handler=message_handler,
        max_retries=0,
        mq_message_exchange=mocker.AsyncMock(),
        mq_conn=mocker.AsyncMock(),
        pending_message_queue=mocker.AsyncMock(),
    )
    return message_processor
