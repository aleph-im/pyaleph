import json
from pathlib import Path
from typing import Dict, List

import pytest_asyncio
from aleph.model.messages import Message


async def _load_fixtures(filename: str):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / filename

    with fixtures_file.open() as f:
        messages = json.load(f)

    await Message.collection.insert_many(messages)
    return messages


@pytest_asyncio.fixture
async def fixture_messages(test_db) -> List[Dict]:
    return await _load_fixtures("fixture_messages.json")


@pytest_asyncio.fixture
async def fixture_aggregate_messages(test_db) -> List[Dict]:
    return await _load_fixtures("fixture_aggregates.json")


@pytest_asyncio.fixture
async def fixture_post_messages(test_db) -> List[Dict]:
    return await _load_fixtures("fixture_posts.json")
