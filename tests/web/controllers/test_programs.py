import json
from hashlib import sha256
from pathlib import Path

import pytest
import pytest_asyncio

from aleph.model.messages import Message


@pytest_asyncio.fixture
async def fixture_program_message(test_db):
    fixtures_file = Path(__file__).parent / "fixtures/messages/program.json"

    with fixtures_file.open() as f:
        messages = json.load(f)

    # Add item_content and item_hash to messages, modify in place:
    for message in messages:
        if 'item_content' not in message:
            message['item_content'] = json.dumps(message['content'])
        if 'item_hash' not in message:
            message['item_hash'] = sha256(message['item_content'].encode()).hexdigest()

    await Message.collection.insert_many(messages)
    return messages


@pytest.mark.asyncio
async def test_get_programs_on_message(fixture_program_message, ccn_api_client):
    response = await ccn_api_client.get("/api/v0/programs/on/message")
    assert response.status == 200, await response.text()

    data = await response.json()
    expected = {
        'item_hash': fixture_program_message[0]['item_hash'],
        'content': {'on': {'message': fixture_program_message[0]['content']['on']['message']}},
    }

    assert data == [expected]
