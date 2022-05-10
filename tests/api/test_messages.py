import itertools
import json
from pathlib import Path
from typing import Dict, Iterable, List

import pytest
import pytest_asyncio

from aleph.model.messages import Message
from aleph.web import create_app

MESSAGES_URI = "/api/v0/messages.json"


def get_messages_by_channel(messages: Iterable[Dict], channel: str) -> List[Dict]:
    return [msg for msg in messages if msg["channel"] == channel]


def assert_messages_equal(messages: Iterable[Dict], expected_messages: Iterable[Dict]):
    messages_by_hash = {msg["item_hash"]: msg for msg in messages}

    for expected_message in expected_messages:
        message = messages_by_hash[expected_message["item_hash"]]

        assert message["channel"] == expected_message["channel"]
        assert message["content"] == expected_message["content"]
        assert message["sender"] == expected_message["sender"]
        assert message["signature"] == expected_message["signature"]


@pytest_asyncio.fixture
async def fixture_messages(test_db):
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_file = fixtures_dir / "fixture_messages.json"

    with fixtures_file.open() as f:
        messages = json.load(f)

    await Message.collection.insert_many(messages)
    return messages


@pytest.mark.asyncio
async def test_get_messages(fixture_messages, aiohttp_client):
    app = create_app()
    client = await aiohttp_client(app)

    response = await client.get(MESSAGES_URI)
    assert response.status == 200, await response.text()

    data = await response.json()

    messages = data["messages"]
    assert len(messages) == len(fixture_messages)
    assert_messages_equal(messages, fixture_messages)

    assert data["pagination_total"] == len(messages)
    assert data["pagination_page"] == 1


@pytest.mark.asyncio
async def test_get_messages_filter_by_channel(fixture_messages, aiohttp_client):
    app = create_app()
    client = await aiohttp_client(app)

    async def fetch_messages_by_channel(channel: str) -> Dict:
        response = await client.get(MESSAGES_URI, params={"channels": channel})
        assert response.status == 200, await response.text()
        return await response.json()

    data = await fetch_messages_by_channel("unit-tests")
    messages = data["messages"]

    unit_test_messages = get_messages_by_channel(fixture_messages, "unit-tests")

    assert len(messages) == len(unit_test_messages)
    assert_messages_equal(messages, unit_test_messages)

    data = await fetch_messages_by_channel("aggregates-tests")
    messages = data["messages"]

    aggregates_test_messages = get_messages_by_channel(fixture_messages, "aggregates-tests")
    assert_messages_equal(messages, aggregates_test_messages)

    # Multiple channels
    data = await fetch_messages_by_channel("aggregates-tests,unit-tests")
    messages = data["messages"]

    assert_messages_equal(messages, itertools.chain(unit_test_messages, aggregates_test_messages))

    # Nonexistent channel
    data = await fetch_messages_by_channel("none-pizza-with-left-beef")
    assert data["messages"] == []
