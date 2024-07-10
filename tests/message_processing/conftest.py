import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Sequence

import pytest
import pytest_asyncio
from configmanager import Config
from in_memory_storage_engine import InMemoryStorageEngine

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.models import ChainTxDb, PendingMessageDb
from aleph.handlers.message_handler import MessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory

from .load_fixtures import load_fixture_messages


@pytest.fixture
def fixture_messages():
    return load_fixture_messages("test-data-pending-tx-messages.json")


# TODO: this code (and the fixture data) is duplicated with tests/api/conftest.py.
#       it could make sense to have some general fixtures available to all the test cases
#       to reduce duplication between DB tests, API tests, etc.
async def _load_fixtures(
    session_factory: DbSessionFactory, filename: str
) -> Sequence[Dict[str, Any]]:
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages_json = json.load(f)

    pending_messages = []
    chain_txs = []
    tx_hashes = set()
    for message_dict in messages_json:
        pending_messages.append(
            PendingMessageDb.from_message_dict(
                message_dict,
                reception_time=dt.datetime(2022, 1, 1),
                fetched=True,
            )
        )
        for confirmation in message_dict.get("confirmations", []):
            if (tx_hash := confirmation["hash"]) not in tx_hashes:
                chain_txs.append(ChainTxDb.from_dict(confirmation))
                tx_hashes.add(tx_hash)

    with session_factory() as session:
        session.add_all(pending_messages)
        session.add_all(chain_txs)
        session.commit()

    return messages_json


@pytest_asyncio.fixture
async def fixture_aggregate_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "test-data-aggregates.json")


@pytest_asyncio.fixture
async def fixture_post_messages(
    session_factory: DbSessionFactory,
) -> Sequence[Dict[str, Any]]:
    return await _load_fixtures(session_factory, "test-data-posts.json")


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
