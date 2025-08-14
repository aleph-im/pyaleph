import json
from typing import Dict, List

import pytest
from aleph_message.models import ItemHash
from configmanager import Config
from sqlalchemy import select

from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.accessors.posts import get_post
from aleph.db.models import PostDb
from aleph.handlers.content.post import PostMessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory


@pytest.mark.asyncio
async def test_process_post_and_amend(
    session_factory: DbSessionFactory,
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    fixture_post_messages: List[Dict],
):
    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    original_hash = "9f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc1025"
    amend_hash = "93776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c"

    with session_factory() as session:
        # We should now have one post
        post = get_post(session=session, item_hash=original_hash)

    fixtures_by_item_hash = {m["item_hash"]: m for m in fixture_post_messages}
    original = fixtures_by_item_hash[original_hash]
    amend = fixtures_by_item_hash[amend_hash]
    original_content = json.loads(original["item_content"])
    amend_content = json.loads(amend["item_content"])

    assert post
    assert post.item_hash == amend_hash
    assert post.original_item_hash == original_hash
    assert post.content == amend_content["content"]
    assert post.original_type == original_content["type"]
    assert post.last_updated == timestamp_to_datetime(amend_content["time"])
    assert post.created == timestamp_to_datetime(original_content["time"])
    assert post.channel == original["channel"]


@pytest.mark.asyncio
async def test_forget_original_post(
    session_factory: DbSessionFactory,
    mock_config: Config,
    message_processor: PendingMessageProcessor,
    fixture_post_messages: List[Dict],
):
    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    original_hash = "9f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc1025"
    amend_hash = "93776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c"

    content_handler = PostMessageHandler(
        balances_addresses=[],
        balances_post_type="no-balances-today",
        credit_balances_addresses=[],
        credit_balances_post_types=["no-credit-balances-today"],
    )
    with session_factory() as session:
        original_message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(original_hash)
        )
        assert original_message is not None
        additional_hashes_to_forget = await content_handler.forget_message(
            session=session,
            message=original_message,
        )
        session.commit()

        assert additional_hashes_to_forget == {amend_hash}

        posts = list(session.execute(select(PostDb)).scalars())
        assert posts == []
