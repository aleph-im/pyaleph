import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple, cast

import pytest
import pytest_asyncio
from aleph_message.models import (
    AggregateContent,
    Chain,
    ItemType,
    MessageType,
    PostContent,
)
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
from aleph.db.models.messages import MessageStatusDb
from aleph.db.models.posts import PostDb
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.channel import Channel
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus


# TODO: remove the raw parameter, it's just to avoid larger refactorings
async def _load_fixtures(
    session_factory: DbSessionFactory, filename: str, raw: bool = True
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    messages = []
    tx_hashes = set()

    with session_factory() as session:
        for message_dict in messages_json:
            message_db = MessageDb.from_message_dict(message_dict)
            messages.append(message_db)
            session.add(message_db)

            for confirmation in message_dict.get("confirmations", []):
                if (tx_hash := confirmation["hash"]) not in tx_hashes:
                    chain_tx_db = ChainTxDb.from_dict(confirmation)
                    tx_hashes.add(tx_hash)
                    session.add(chain_tx_db)

                session.flush()
                session.execute(
                    insert(message_confirmations).values(
                        item_hash=message_db.item_hash, tx_hash=tx_hash
                    )
                )

            message_status = MessageStatusDb(
                item_hash=message_dict["item_hash"],
                status=MessageStatus.PROCESSED,
                reception_time=utc_now(),
            )
            session.add(message_status)

        session.commit()

    return messages_json if raw else messages


@pytest_asyncio.fixture
async def fixture_messages(
    session_factory: DbSessionFactory,
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
    session_factory: DbSessionFactory,
) -> Sequence[MessageDb]:
    messages = await _load_fixtures(
        session_factory, "fixture_aggregates.json", raw=False
    )
    aggregate_keys = set()
    with session_factory() as session:
        for message in messages:
            aggregate_element = make_aggregate_element(message)  # type: ignore
            session.add(aggregate_element)
            aggregate_keys.add((aggregate_element.owner, aggregate_element.key))
        session.commit()

        for owner, key in aggregate_keys:
            refresh_aggregate(session=session, owner=owner, key=key)

        session.commit()

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
    session_factory: DbSessionFactory,
) -> Sequence[PostDb]:
    messages = await _load_fixtures(session_factory, "fixture_posts.json", raw=False)
    posts = [make_post_db(message) for message in messages]  # type: ignore

    with session_factory() as session:
        session.add_all(posts)
        session.commit()

    return posts


@pytest.fixture
def post_with_refs_and_tags() -> Tuple[MessageDb, PostDb, MessageStatusDb]:
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

    message_status = MessageStatusDb(
        item_hash=message.item_hash,
        status=MessageStatus.PROCESSED,
        reception_time=utc_now(),
    )

    return message, post, message_status


@pytest.fixture
def amended_post_with_refs_and_tags(
    post_with_refs_and_tags: Tuple[MessageDb, PostDb, MessageStatusDb],
):
    original_message, original_post, _ = post_with_refs_and_tags

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

    message_status = MessageStatusDb(
        item_hash=amend_message.item_hash,
        status=MessageStatus.PROCESSED,
        reception_time=utc_now(),
    )

    return amend_message, amend_post, message_status


@pytest.fixture
def test_addresses():
    """Return a list of test addresses used in the address stats message fixtures."""
    return [
        "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",  # Has POST, STORE, PROGRAM
        "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4",  # Has POST
        "0x5D00fAD0763A876202a29FE71D30B4554D28FB97",  # Has STORE
        "0xDifferentAddress1",  # Has AGGREGATE
        "0xDifferentAddress2",  # Has INSTANCE
    ]


@pytest.fixture
def fixture_address_stats_messages(
    session_factory: DbSessionFactory, test_addresses
) -> List[MessageDb]:
    """Create test messages with different types and addresses for address stats testing."""
    now = utc_now()
    messages = [
        # First address has multiple message types
        MessageDb(
            item_hash="hash1",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig1",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content1"},
            size=100,
            time=now,
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        MessageDb(
            item_hash="hash2",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig2",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content2"},
            size=100,
            time=now + dt.timedelta(seconds=1),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        MessageDb(
            item_hash="hash3",
            chain=Chain.ETH,
            sender=test_addresses[0],
            signature="0xsig3",
            item_type=ItemType.inline,
            type=MessageType.program,
            content={"test": "content3"},
            size=100,
            time=now + dt.timedelta(seconds=2),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        # Second address has only POST
        MessageDb(
            item_hash="hash4",
            chain=Chain.ETH,
            sender=test_addresses[1],
            signature="0xsig4",
            item_type=ItemType.inline,
            type=MessageType.post,
            content={"test": "content4"},
            size=100,
            time=now + dt.timedelta(seconds=3),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        # Third address has only STORE
        MessageDb(
            item_hash="hash5",
            chain=Chain.ETH,
            sender=test_addresses[2],
            signature="0xsig5",
            item_type=ItemType.inline,
            type=MessageType.store,
            content={"test": "content5"},
            size=100,
            time=now + dt.timedelta(seconds=4),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        # Fourth address has AGGREGATE
        MessageDb(
            item_hash="hash6",
            chain=Chain.ETH,
            sender=test_addresses[3],
            signature="0xsig6",
            item_type=ItemType.inline,
            type=MessageType.aggregate,
            content={"key": "test6", "content": {"data": "aggregate"}},
            size=100,
            time=now + dt.timedelta(seconds=5),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
        # Fifth address has INSTANCE
        MessageDb(
            item_hash="hash7",
            chain=Chain.ETH,
            sender=test_addresses[4],
            signature="0xsig7",
            item_type=ItemType.inline,
            type=MessageType.instance,
            content={"test": "content7"},
            size=100,
            time=now + dt.timedelta(seconds=6),
            channel=Channel("TEST"),
            status_value=MessageStatus.PROCESSED,
            reception_time=now,
        ),
    ]

    message_statuses = [
        MessageStatusDb(
            item_hash=msg.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=now,
        )
        for msg in messages
    ]

    with session_factory() as session:
        session.add_all(messages)
        session.add_all(message_statuses)
        session.commit()

    return messages


@pytest.fixture
def message_processor(mocker, mock_config: Config, session_factory: DbSessionFactory):
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
