import json
from pathlib import Path
import pytest_asyncio
from aleph.model.messages import Message


@pytest_asyncio.fixture
async def fixture_messages(test_db):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / "fixture_messages.json"

    with fixtures_file.open() as f:
        messages = json.load(f)

    await Message.collection.insert_many(messages)
    return messages
