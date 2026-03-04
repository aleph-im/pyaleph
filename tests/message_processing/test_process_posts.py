import datetime as dt
import json
from typing import Dict, List

import pytest
from aleph_message.models import ItemHash
from configmanager import Config
from message_test_helpers import make_validated_message_from_dict
from sqlalchemy import select

from aleph.db.accessors.balances import (
    get_credit_balance,
    update_credit_balances_distribution,
)
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.db.accessors.posts import get_post
from aleph.db.models import AlephCreditHistoryDb, MessageDb, PostDb
from aleph.handlers.content.post import PostMessageHandler
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import InvalidMessageFormat


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
        credit_balances_channels=["nope"],
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


WHITELISTED_ADDRESS = "0xWhitelisted12345678901234567890123456789012"
REGULAR_SENDER = "0xRegularSender123456789012345678901234567890"
RECIPIENT_ADDRESS = "0xRecipient12345678901234567890123456789012345"


def _make_credit_transfer_message(
    sender: str, recipient: str, amount: int, msg_hash: str
) -> MessageDb:
    item_content = json.dumps(
        {
            "address": sender,
            "time": 1651050219.0,
            "content": {
                "transfer": {"credits": [{"address": recipient, "amount": amount}]}
            },
            "type": "aleph_credit_transfer",
        }
    )
    return make_validated_message_from_dict(
        {
            "chain": "ETH",
            "item_hash": msg_hash,
            "sender": sender,
            "type": "POST",
            "channel": "ALEPH_CREDIT",
            "item_content": item_content,
            "item_type": "inline",
            "signature": "0xsig",
            "time": 1651050219.0,
        }
    )


def _make_credit_distribution_message(
    sender: str, recipient: str, amount: int, msg_hash: str
) -> MessageDb:
    item_content = json.dumps(
        {
            "address": sender,
            "time": 1651050219.0,
            "content": {
                "distribution": {
                    "credits": [
                        {
                            "address": recipient,
                            "amount": amount,
                            "price": "1.0",
                            "tx_hash": "0xtx",
                            "provider": "test",
                            "origin": "test",
                            "ref": "ref",
                            "payment_method": "crypto",
                        }
                    ],
                    "token": "ALEPH",
                    "chain": "ETH",
                }
            },
            "type": "aleph_credit_distribution",
        }
    )
    return make_validated_message_from_dict(
        {
            "chain": "ETH",
            "item_hash": msg_hash,
            "sender": sender,
            "type": "POST",
            "channel": "ALEPH_CREDIT",
            "item_content": item_content,
            "item_type": "inline",
            "signature": "0xsig",
            "time": 1651050219.0,
        }
    )


@pytest.mark.asyncio
async def test_credit_transfer_non_whitelisted_sender(
    session_factory: DbSessionFactory,
):
    """
    Non-whitelisted users can transfer credits they own.
    This was previously blocked because all credit balance operations required
    the sender to be a whitelisted address.
    """
    handler = PostMessageHandler(
        balances_addresses=[],
        balances_post_type="balance",
        credit_balances_addresses=[WHITELISTED_ADDRESS],
        credit_balances_post_types=[
            "aleph_credit_distribution",
            "aleph_credit_transfer",
            "aleph_credit_expense",
        ],
        credit_balances_channels=["ALEPH_CREDIT"],
    )

    with session_factory() as session:
        # Give the regular sender some credits via a whitelisted distribution
        update_credit_balances_distribution(
            session=session,
            credits_list=[
                {
                    "address": REGULAR_SENDER,
                    "amount": 1000,
                    "price": "1.0",
                    "tx_hash": "0xinit",
                    "provider": "test",
                    "origin": "test",
                    "ref": "ref",
                    "payment_method": "crypto",
                }
            ],
            token="ALEPH",
            chain="ETH",
            message_hash="init_dist_hash_abc",
            message_timestamp=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        )
        session.commit()

        initial_balance = get_credit_balance(session, REGULAR_SENDER)
        assert initial_balance > 0

        # Process a transfer from the regular (non-whitelisted) sender
        transfer_msg = _make_credit_transfer_message(
            sender=REGULAR_SENDER,
            recipient=RECIPIENT_ADDRESS,
            amount=100,
            msg_hash="a" * 64,
        )
        await handler.process_post(session=session, message=transfer_msg)
        session.commit()

        # Verify the transfer was processed: sender debited, recipient credited
        records = (
            session.query(AlephCreditHistoryDb).filter_by(credit_ref="a" * 64).all()
        )
        assert len(records) == 2

        recipient_record = next(r for r in records if r.amount > 0)
        sender_record = next(r for r in records if r.amount < 0)

        assert recipient_record.address == RECIPIENT_ADDRESS
        assert recipient_record.amount == 1_000_000  # 100 * 10000 multiplier
        assert sender_record.address == REGULAR_SENDER
        assert sender_record.amount == -1_000_000

        new_balance = get_credit_balance(session, REGULAR_SENDER)
        assert new_balance == initial_balance - 1_000_000


@pytest.mark.asyncio
async def test_credit_transfer_insufficient_balance_rejected(
    session_factory: DbSessionFactory,
):
    """Non-whitelisted senders with insufficient balance cannot transfer."""
    handler = PostMessageHandler(
        balances_addresses=[],
        balances_post_type="balance",
        credit_balances_addresses=[WHITELISTED_ADDRESS],
        credit_balances_post_types=[
            "aleph_credit_distribution",
            "aleph_credit_transfer",
            "aleph_credit_expense",
        ],
        credit_balances_channels=["ALEPH_CREDIT"],
    )

    with session_factory() as session:
        # Sender has no credits
        transfer_msg = _make_credit_transfer_message(
            sender=REGULAR_SENDER,
            recipient=RECIPIENT_ADDRESS,
            amount=100,
            msg_hash="b" * 64,
        )
        with pytest.raises(InvalidMessageFormat) as exc_info:
            await handler.process_post(session=session, message=transfer_msg)
        assert "Insufficient credit balance" in exc_info.value.args[0][0]


@pytest.mark.asyncio
async def test_credit_distribution_non_whitelisted_sender_ignored(
    session_factory: DbSessionFactory,
):
    """
    Non-whitelisted senders cannot trigger credit distributions.
    The distribution should be silently ignored (not raise an error,
    just not processed).
    """
    handler = PostMessageHandler(
        balances_addresses=[],
        balances_post_type="balance",
        credit_balances_addresses=[WHITELISTED_ADDRESS],
        credit_balances_post_types=[
            "aleph_credit_distribution",
            "aleph_credit_transfer",
            "aleph_credit_expense",
        ],
        credit_balances_channels=["ALEPH_CREDIT"],
    )

    with session_factory() as session:
        dist_msg = _make_credit_distribution_message(
            sender=REGULAR_SENDER,
            recipient=RECIPIENT_ADDRESS,
            amount=1000,
            msg_hash="c" * 64,
        )
        # Should process without error, but distribution should be ignored
        await handler.process_post(session=session, message=dist_msg)
        session.commit()

        # No credit history records should exist for this message
        records = (
            session.query(AlephCreditHistoryDb).filter_by(credit_ref="c" * 64).all()
        )
        assert len(records) == 0
        assert get_credit_balance(session, RECIPIENT_ADDRESS) == 0
