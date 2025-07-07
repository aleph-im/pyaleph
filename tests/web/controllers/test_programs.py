import json
from hashlib import sha256
from pathlib import Path
from typing import List, Sequence

import pytest
import pytest_asyncio
from message_test_helpers import make_validated_message_from_dict

from aleph.db.models import MessageDb
from aleph.types.db_session import AsyncDbSessionFactory


@pytest_asyncio.fixture
async def fixture_program_messages(
    session_factory: AsyncDbSessionFactory,
) -> List[MessageDb]:
    fixtures_file = Path(__file__).parent / "fixtures/messages/program.json"

    with fixtures_file.open() as f:
        json_messages = json.load(f)

    # Add item_content and item_hash to messages, modify in place:
    messages = []
    for message_dict in json_messages:
        if "item_content" not in message_dict:
            message_dict["item_content"] = json.dumps(message_dict["content"])
        if "item_hash" not in message_dict:
            message_dict["item_hash"] = sha256(
                message_dict["item_content"].encode()
            ).hexdigest()

        messages.append(
            make_validated_message_from_dict(
                message_dict=message_dict,
                raw_content=json.dumps(message_dict["content"]),
            )
        )

    async with session_factory() as session:
        session.add_all(messages)
        await session.commit()

    return messages


@pytest.mark.asyncio
async def test_get_programs_on_message(
    fixture_program_messages: Sequence[MessageDb], ccn_api_client
):
    response = await ccn_api_client.get("/api/v0/programs/on/message")
    assert response.status == 200, await response.text()

    data = await response.json()
    expected = {
        "item_hash": fixture_program_messages[0].item_hash,
        "content": {
            "on": {"message": fixture_program_messages[0].content["on"]["message"]}
        },
    }

    assert data == [expected]
