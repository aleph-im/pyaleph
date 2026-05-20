import datetime as dt
import json
from typing import Any

import pytest
from aleph_message.models import MessageType
from message_test_helpers import make_validated_message_from_dict

from aleph.db.models import (
    AggregateDb,
    AggregateElementDb,
    MessageDb,
    MessageStatusDb,
)
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.handlers.content.post import PostMessageHandler
from aleph.handlers.content.store import StoreMessageHandler
from aleph.handlers.content.vm import VmMessageHandler
from aleph.toolkit.constants import DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus, PermissionDenied


OWNER = "0xA000000000000000000000000000000000000001"
OWNER_2 = "0xA000000000000000000000000000000000000002"
DELEGATE_D1 = "0xD100000000000000000000000000000000000001"
DELEGATE_D2 = "0xD200000000000000000000000000000000000002"
STRANGER = "0xE000000000000000000000000000000000000099"

FAKE_SIG = "0x" + "00" * 65
FAKE_FILE_HASH = "f" * 64


def _store_message_dict(
    *,
    sender: str,
    content_address: str,
    item_hash: str,
    channel: str = "TEST",
    time: float = 1700000000.0,
) -> dict:
    content: dict[str, Any] = {
        "address": content_address,
        "time": time,
        "item_type": "storage",
        "item_hash": FAKE_FILE_HASH,
        "mime_type": "text/plain",
    }
    return {
        "chain": "ETH",
        "channel": channel,
        "sender": sender,
        "type": "STORE",
        "time": time,
        "item_type": "inline",
        "item_content": json.dumps(content, separators=(",", ":")),
        "item_hash": item_hash,
        "signature": FAKE_SIG,
        "content": content,
    }


def _forget_message_dict(
    *,
    sender: str,
    content_address: str,
    item_hash: str,
    target_hashes: list[str],
    channel: str = "TEST",
    time: float = 1700000100.0,
) -> dict:
    content: dict[str, Any] = {
        "address": content_address,
        "time": time,
        "hashes": target_hashes,
    }
    return {
        "chain": "ETH",
        "channel": channel,
        "sender": sender,
        "type": "FORGET",
        "time": time,
        "item_type": "inline",
        "item_content": json.dumps(content, separators=(",", ":")),
        "item_hash": item_hash,
        "signature": FAKE_SIG,
        "content": content,
    }


def _insert_processed_message(session, message_dict: dict) -> MessageDb:
    message = make_validated_message_from_dict(
        message_dict, raw_content=message_dict["item_content"]
    )
    session.add(message)
    session.add(
        MessageStatusDb(
            item_hash=message.item_hash,
            status=MessageStatus.PROCESSED,
            reception_time=dt.datetime(2022, 1, 1),
        )
    )
    return message


def _insert_security_aggregate(
    session, owner: str, authorizations: list[dict]
) -> None:
    aggregate_dt = timestamp_to_datetime(1700000050.0)
    aggregate_content = {"authorizations": authorizations}
    # Derive a deterministic, unique-per-owner 64-char hex revision hash.
    revision_hash = owner.lower().removeprefix("0x").ljust(64, "0")
    session.add(
        AggregateDb(
            key="security",
            owner=owner,
            content=aggregate_content,
            creation_datetime=aggregate_dt,
            last_revision=AggregateElementDb(
                item_hash=revision_hash,
                key="security",
                owner=owner,
                content=aggregate_content,
                creation_datetime=aggregate_dt,
            ),
            dirty=False,
        )
    )


@pytest.fixture
def forget_handler(mocker) -> ForgetMessageHandler:
    vm_handler = VmMessageHandler()
    content_handlers = {
        MessageType.aggregate: AggregateMessageHandler(),
        MessageType.instance: vm_handler,
        MessageType.post: PostMessageHandler(
            balances_addresses=["nope"],
            balances_post_type="no-balances-in-tests",
            credit_balances_addresses=["nope"],
            credit_balances_post_types=["no-balances-in-tests"],
            credit_balances_channels=["nope"],
        ),
        MessageType.program: vm_handler,
        MessageType.store: StoreMessageHandler(
            storage_service=mocker.AsyncMock(),
            grace_period=24,
            max_unauthenticated_upload_file_size=DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
        ),
    }
    return ForgetMessageHandler(content_handlers=content_handlers)


@pytest.mark.asyncio
async def test_owner_forgets_delegates_content(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """O delegated STORE to D1, D1 stored with content.address=O, then O
    sends a FORGET. Under the old strict-equality rule this is denied;
    under the new rule it succeeds because O is the content owner."""

    store_hash = "1" * 64
    forget_hash = "2" * 64

    with session_factory() as session:
        _insert_security_aggregate(
            session,
            owner=OWNER,
            authorizations=[{"address": DELEGATE_D1, "types": ["STORE"]}],
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=DELEGATE_D1,
                content_address=OWNER,
                item_hash=store_hash,
            ),
        )
        session.commit()

        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=OWNER,
                content_address=OWNER,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        # Asserts the positive case: check_permissions must return without
        # raising. Under the old strict-equality rule this raises
        # PermissionDenied (the TDD "red" signal); after Task 3 it succeeds.
        await forget_handler.check_permissions(session=session, message=forget_msg)
