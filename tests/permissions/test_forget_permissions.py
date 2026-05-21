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


@pytest.mark.asyncio
async def test_owner_forgets_own_content(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """Sanity regression: a sender who owns their own content can still
    forget it without any delegation in play."""

    store_hash = "3" * 64
    forget_hash = "4" * 64

    with session_factory() as session:
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash
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

        await forget_handler.check_permissions(session=session, message=forget_msg)


@pytest.mark.asyncio
async def test_second_delegate_forgets_owner_content(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """O grants types=[FORGET] to D2; D2 forgets a STORE that O signed.
    D2 was never authorized to STORE, only to FORGET."""

    store_hash = "5" * 64
    forget_hash = "6" * 64

    with session_factory() as session:
        _insert_security_aggregate(
            session,
            owner=OWNER,
            authorizations=[{"address": DELEGATE_D2, "types": ["FORGET"]}],
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash
            ),
        )
        session.commit()

        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=DELEGATE_D2,
                content_address=OWNER,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        await forget_handler.check_permissions(session=session, message=forget_msg)


@pytest.mark.asyncio
async def test_second_delegate_forgets_first_delegates_content(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """Original-question scenario: O granted STORE to D1 and FORGET to D2.
    D1 created a STORE with content.address=O. D2 cleans it up."""

    store_hash = "7" * 64
    forget_hash = "8" * 64

    with session_factory() as session:
        _insert_security_aggregate(
            session,
            owner=OWNER,
            authorizations=[
                {"address": DELEGATE_D1, "types": ["STORE"]},
                {"address": DELEGATE_D2, "types": ["FORGET"]},
            ],
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
                sender=DELEGATE_D2,
                content_address=OWNER,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        await forget_handler.check_permissions(session=session, message=forget_msg)


@pytest.mark.asyncio
async def test_owner_forgets_after_revoking_delegate(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """Regression: an owner can still forget content owned by them
    (content.address=OWNER) even when their security aggregate has been
    cleared of all delegations. The new authorization rule does not
    require any aggregate entry for the owner's self-forget path; this
    test confirms that revoking past delegations does not accidentally
    lock the owner out of cleaning up content the delegate created on
    their behalf."""

    store_hash = "9" * 64
    forget_hash = "a" * 64

    with session_factory() as session:
        # Aggregate exists but does not (any longer) list D1.
        _insert_security_aggregate(
            session, owner=OWNER, authorizations=[]
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

        await forget_handler.check_permissions(session=session, message=forget_msg)


@pytest.mark.asyncio
async def test_cross_owner_multi_target_forget(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """A single sender (D2) holds FORGET delegation from two different
    owners. A single FORGET message lists targets owned by both. Per-target
    authorization succeeds for each."""

    store_hash_o1 = "b" * 64
    store_hash_o2 = "c" * 64
    forget_hash = "d" * 64

    with session_factory() as session:
        _insert_security_aggregate(
            session,
            owner=OWNER,
            authorizations=[{"address": DELEGATE_D2, "types": ["FORGET"]}],
        )
        _insert_security_aggregate(
            session,
            owner=OWNER_2,
            authorizations=[{"address": DELEGATE_D2, "types": ["FORGET"]}],
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash_o1
            ),
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER_2, content_address=OWNER_2, item_hash=store_hash_o2
            ),
        )
        session.commit()

        # D2 signs the FORGET as themselves; base check passes trivially
        # (sender == content.address), per-target checks verify each owner.
        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=DELEGATE_D2,
                content_address=DELEGATE_D2,
                item_hash=forget_hash,
                target_hashes=[store_hash_o1, store_hash_o2],
            )
        )

        await forget_handler.check_permissions(session=session, message=forget_msg)


@pytest.mark.asyncio
async def test_forget_by_aggregate_key(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """O sends a FORGET that uses the `aggregates: [key]` field rather
    than explicit `hashes`. The handler queries AggregateElementDb where
    `key == <aggregate_key>` AND `owner == content.address` and uses each
    returned `item_hash` as a forget target. The per-target authorization
    check then runs against each underlying AGGREGATE message.

    `ForgetContent.aggregates` is typed as `List[ItemHash]`, so the key
    value itself must be a valid 64-char hex string. Note that the key
    (`aggregate_key`) and the underlying message's item_hash
    (`aggregate_msg_hash`) are distinct values: the key is the lookup
    column, the item_hash is the target."""

    # aggregate_key must be a valid ItemHash (64-char hex) because
    # ForgetContent.aggregates is typed as List[ItemHash].
    aggregate_key = "0e" + "0" * 62
    aggregate_msg_hash = "0b" + "0" * 62
    forget_hash = "0c" + "0" * 62
    aggregate_dt = timestamp_to_datetime(1700000000.0)

    aggregate_content = {
        "address": OWNER,
        "time": 1700000000.0,
        "key": aggregate_key,
        "content": {"hello": "world"},
    }
    aggregate_message_dict = {
        "chain": "ETH",
        "channel": "TEST",
        "sender": OWNER,
        "type": "AGGREGATE",
        "time": 1700000000.0,
        "item_type": "inline",
        "item_content": json.dumps(aggregate_content, separators=(",", ":")),
        "item_hash": aggregate_msg_hash,
        "signature": FAKE_SIG,
        "content": aggregate_content,
    }

    with session_factory() as session:
        _insert_processed_message(session, aggregate_message_dict)
        session.add(
            AggregateElementDb(
                item_hash=aggregate_msg_hash,
                key=aggregate_key,
                owner=OWNER,
                content=aggregate_content["content"],
                creation_datetime=aggregate_dt,
            )
        )
        session.commit()

        forget_content = {
            "address": OWNER,
            "time": 1700000200.0,
            "hashes": [],
            "aggregates": [aggregate_key],
        }
        forget_dict = {
            "chain": "ETH",
            "channel": "TEST",
            "sender": OWNER,
            "type": "FORGET",
            "time": 1700000200.0,
            "item_type": "inline",
            "item_content": json.dumps(forget_content, separators=(",", ":")),
            "item_hash": forget_hash,
            "signature": FAKE_SIG,
            "content": forget_content,
        }
        forget_msg = make_validated_message_from_dict(forget_dict)

        await forget_handler.check_permissions(
            session=session, message=forget_msg
        )


@pytest.mark.asyncio
async def test_stranger_cannot_forget(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """An address with no delegation from O cannot forget O's content."""

    store_hash = "e" * 64
    forget_hash = "01" + "0" * 62

    with session_factory() as session:
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash
            ),
        )
        session.commit()

        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=STRANGER,
                content_address=STRANGER,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        with pytest.raises(PermissionDenied):
            await forget_handler.check_permissions(
                session=session, message=forget_msg
            )


@pytest.mark.asyncio
async def test_revoked_delegate_cannot_forget(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """D1 was previously delegated by O and created a STORE with
    content.address=O. O has since revoked D1. D1 tries to forget
    that STORE. This is the intended tightening relative to the old
    sender-equality rule."""

    store_hash = "02" + "0" * 62
    forget_hash = "03" + "0" * 62

    with session_factory() as session:
        # Aggregate exists but D1 is no longer listed.
        _insert_security_aggregate(
            session, owner=OWNER, authorizations=[]
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
                sender=DELEGATE_D1,
                content_address=DELEGATE_D1,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        with pytest.raises(PermissionDenied):
            await forget_handler.check_permissions(
                session=session, message=forget_msg
            )


@pytest.mark.asyncio
async def test_delegate_without_forget_scope_cannot_forget(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """O grants D1 types=[STORE] only. D1 tries to forget. The per-target
    check honors the types filter and denies because FORGET is not in
    D1's scope."""

    store_hash = "04" + "0" * 62
    forget_hash = "05" + "0" * 62

    with session_factory() as session:
        _insert_security_aggregate(
            session,
            owner=OWNER,
            authorizations=[{"address": DELEGATE_D1, "types": ["STORE"]}],
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash
            ),
        )
        session.commit()

        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=DELEGATE_D1,
                content_address=DELEGATE_D1,
                item_hash=forget_hash,
                target_hashes=[store_hash],
            )
        )

        with pytest.raises(PermissionDenied):
            await forget_handler.check_permissions(
                session=session, message=forget_msg
            )


@pytest.mark.asyncio
async def test_owner_cannot_forget_delegate_self_signed_content(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """D1 is delegated by O but signs a STORE with content.address=D1
    (acting as themselves, not on behalf of O). O has no authorization
    claim over content owned by D1."""

    store_hash = "06" + "0" * 62
    forget_hash = "07" + "0" * 62

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
                content_address=DELEGATE_D1,
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

        with pytest.raises(PermissionDenied):
            await forget_handler.check_permissions(
                session=session, message=forget_msg
            )


@pytest.mark.asyncio
async def test_forget_all_or_nothing_on_mixed_targets(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """A FORGET lists two targets: one authorized (owned by O, signed by O)
    and one not (owned by D1, signed by D1). The whole FORGET must be
    denied and neither target may be forgotten. Because check_permissions
    is invoked before process() in the message pipeline, raising here is
    sufficient to guarantee no target is touched; we additionally verify
    both targets are still in PROCESSED state."""

    from aleph.db.accessors.messages import get_message_status
    from aleph_message.models import ItemHash

    store_hash_owned = "08" + "0" * 62
    store_hash_other = "09" + "0" * 62
    forget_hash = "0a" + "0" * 62

    with session_factory() as session:
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=OWNER, content_address=OWNER, item_hash=store_hash_owned
            ),
        )
        _insert_processed_message(
            session,
            _store_message_dict(
                sender=DELEGATE_D1,
                content_address=DELEGATE_D1,
                item_hash=store_hash_other,
            ),
        )
        session.commit()

        forget_msg = make_validated_message_from_dict(
            _forget_message_dict(
                sender=OWNER,
                content_address=OWNER,
                item_hash=forget_hash,
                target_hashes=[store_hash_owned, store_hash_other],
            )
        )

        with pytest.raises(PermissionDenied):
            await forget_handler.check_permissions(
                session=session, message=forget_msg
            )

        # Neither target should have been touched.
        for target_hash in (store_hash_owned, store_hash_other):
            status = get_message_status(
                session=session, item_hash=ItemHash(target_hash)
            )
            assert status is not None
            assert status.status == MessageStatus.PROCESSED
