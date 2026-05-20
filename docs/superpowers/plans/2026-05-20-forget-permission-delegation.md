# FORGET Permission Delegation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the strict sender-equality rule in `ForgetMessageHandler.check_permissions` with a per-target authorization check that uses the existing `security`-aggregate delegation system, so that an owner (or their authorized delegate) can forget content created by another delegate.

**Architecture:** Promote the existing private `_check_delegated_authorization` helper in `permissions.py` to a public function `is_sender_authorized_for_owner`. Use it inside the FORGET handler to validate, for each target, that the FORGET sender is authorized to act for `target.parsed_content.address`. The two-phase `check_permissions` / `process` structure already gives the all-or-nothing guarantee (no forget runs unless every target is validated).

**Tech Stack:** Python, SQLAlchemy, pytest, aleph-message. No DB schema changes, no migration, no API surface change.

**Spec:** `docs/superpowers/specs/2026-05-20-forget-permission-delegation-design.md`

---

## File Structure

- Modify: `src/aleph/permissions.py` (rename helper, expose publicly)
- Modify: `src/aleph/handlers/content/forget.py` (replace sender-equality with per-target authorization)
- Create: `tests/permissions/test_forget_permissions.py` (new unit-test file for the new behavior)
- Verify: `tests/message_processing/test_process_forgets.py` (existing end-to-end forget tests stay green)
- Verify: `tests/permissions/test_check_sender_authorization.py` (existing delegation tests stay green)

---

## Task 1: Refactor `permissions.py` to expose `is_sender_authorized_for_owner`

This task is a pure rename. No behavior change. It splits the symbol-rename work from the FORGET handler change so each commit is small and reviewable.

**Files:**
- Modify: `src/aleph/permissions.py`

### Steps

- [ ] **Step 1: Rename `_check_delegated_authorization` to `is_sender_authorized_for_owner` and drop the leading underscore.**

In `src/aleph/permissions.py`, change the function definition and its single existing caller. The function body is unchanged.

```python
def is_sender_authorized_for_owner(
    session: DbSession, sender: str, owner_address: str, message: MessageDb
) -> bool:
    """Check whether `sender` is authorized to act for `owner_address` per
    the security aggregate, scoped by the type / channel / chain / etc. of
    `message`.
    """

    if sender.lower() == owner_address.lower():
        return True

    aggregate = get_aggregate_by_key(
        session=session, key="security", owner=owner_address
    )

    if not aggregate:
        return False

    authorizations = aggregate.content.get("authorizations", [])

    for auth in authorizations:
        if auth.get("address", "").lower() != sender.lower():
            continue

        if auth.get("chain") and message.chain != auth.get("chain"):
            continue

        channels = auth.get("channels", [])
        mtypes = auth.get("types", [])
        ptypes = auth.get("post_types", [])
        akeys = auth.get("aggregate_keys", [])

        if len(channels) and message.channel not in channels:
            continue

        if len(mtypes) and message.type not in mtypes:
            continue

        if message.type == MessageType.post:
            if len(ptypes) and message.parsed_content.type not in ptypes:
                continue

        if message.type == MessageType.aggregate:
            if len(akeys) and message.parsed_content.key not in akeys:
                continue

        return True

    return False
```

Then in the same file, update the call sites in `check_sender_authorization`:

```python
# In the post-amend branch:
return is_sender_authorized_for_owner(
    session=session,
    sender=sender,
    owner_address=original_address,
    message=original_message,
)

# At the end of the function:
return is_sender_authorized_for_owner(
    session=session, sender=sender, owner_address=address, message=message
)
```

- [ ] **Step 2: Verify nothing else referenced the private name.**

Run: `grep -rn "_check_delegated_authorization" src/ tests/`

Expected: no remaining occurrences.

- [ ] **Step 3: Run the existing permission test suite.**

Run: `venv/bin/pytest tests/permissions/ -v`

Expected: PASS. All existing tests in `tests/permissions/test_check_sender_authorization.py` should be unaffected (the function body is the same; only the name changed).

- [ ] **Step 4: Commit.**

```bash
git add src/aleph/permissions.py
git commit -m "refactor(permissions): expose is_sender_authorized_for_owner as public helper"
```

---

## Task 2: Add the canonical failing test (owner forgets delegate's content)

This test captures the original scenario from the spec: O delegated STORE to D1, D1 created content with `content.address = O`, and O now wants to forget it. Under the current code this should fail with `PermissionDenied`; under the planned change it should succeed.

**Files:**
- Create: `tests/permissions/test_forget_permissions.py`

### Steps

- [ ] **Step 1: Create the test file with shared helpers and the canonical positive test.**

Create `tests/permissions/test_forget_permissions.py` with the following content. This file owns the shared helpers used by Tasks 2, 4, and 5.

```python
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
    session, owner: str, authorizations: list[dict], unique_suffix: str = ""
) -> None:
    aggregate_dt = timestamp_to_datetime(1700000050.0)
    aggregate_content = {"authorizations": authorizations}
    revision_hash = ("b" + unique_suffix).ljust(64, "0")
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

        await forget_handler.check_permissions(session=session, message=forget_msg)
```

- [ ] **Step 2: Run the new test to verify it FAILS under current behavior.**

Run: `venv/bin/pytest tests/permissions/test_forget_permissions.py::test_owner_forgets_delegates_content -v`

Expected: FAIL with `aleph.types.message_status.PermissionDenied: Cannot forget message ... because it belongs to another user`. This confirms the test correctly captures the gap before the fix.

- [ ] **Step 3: Commit the failing test.**

```bash
git add tests/permissions/test_forget_permissions.py
git commit -m "test(forget): add failing test for owner forgetting delegate content"
```

---

## Task 3: Implement the per-target authorization check in `forget.py`

This is the core behavior change. Replace the strict sender-equality block in `ForgetMessageHandler.check_permissions` with a call to `is_sender_authorized_for_owner` against the target message's `content.address`.

**Files:**
- Modify: `src/aleph/handlers/content/forget.py`

### Steps

- [ ] **Step 1: Add the import and replace the sender-equality block.**

In `src/aleph/handlers/content/forget.py`, add the import near the top of the file (alphabetically, after the existing `from aleph.handlers.content.content_handler import ContentHandler`):

```python
from aleph.permissions import is_sender_authorized_for_owner
```

Then in `ForgetMessageHandler.check_permissions`, replace this block:

```python
            if target_message.sender != message.sender:
                raise PermissionDenied(
                    f"Cannot forget message {target_hash} because it belongs to another user"
                )
```

with this:

```python
            target_owner = target_message.parsed_content.address
            if not is_sender_authorized_for_owner(
                session=session,
                sender=message.sender,
                owner_address=target_owner,
                message=message,
            ):
                raise PermissionDenied(
                    f"Sender {message.sender} is not authorized to forget message "
                    f"{target_hash} owned by {target_owner}"
                )
```

The surrounding code (status checks, `target_message` lookup, FORGET-of-FORGET check) is unchanged.

- [ ] **Step 2: Run the canonical test to verify it now passes.**

Run: `venv/bin/pytest tests/permissions/test_forget_permissions.py::test_owner_forgets_delegates_content -v`

Expected: PASS.

- [ ] **Step 3: Run the existing forget end-to-end suite to confirm no regression.**

Run: `venv/bin/pytest tests/message_processing/test_process_forgets.py -v`

Expected: PASS. All five existing tests should still pass because each one already uses `sender == content.address` for both the target and the FORGET message, which is the self-owner case that works identically under the new rule.

- [ ] **Step 4: Commit the implementation.**

```bash
git add src/aleph/handlers/content/forget.py
git commit -m "fix(forget): authorize by target.content.address instead of sender equality

Replaces the strict sender-equality check in ForgetMessageHandler with
a per-target call to is_sender_authorized_for_owner. An owner can now
forget messages whose content.address they own, regardless of who
signed them, via the existing security-aggregate delegation. A former
delegate whose authorization has been revoked can no longer forget
messages they signed under that delegation."
```

---

## Task 4: Add the remaining positive tests

These tests cover the rest of the positive cases in the spec test plan. Each should pass against the implementation from Task 3.

**Files:**
- Modify: `tests/permissions/test_forget_permissions.py`

### Steps

- [ ] **Step 1: Add the self-ownership regression test.**

Append to `tests/permissions/test_forget_permissions.py`:

```python
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
```

- [ ] **Step 2: Add the second-delegate cleanup test.**

Append:

```python
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
```

- [ ] **Step 3: Add the combined "D2 cleans up D1's content" test.**

Append:

```python
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
```

- [ ] **Step 4: Add the test for forget surviving revocation of an unrelated permission.**

Append:

```python
@pytest.mark.asyncio
async def test_owner_forgets_after_revoking_delegate(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """D1's STORE delegation was revoked (the security aggregate no longer
    lists D1) but the STORE message D1 created with content.address=O
    persists. O can still forget it."""

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
```

- [ ] **Step 5: Add the cross-owner multi-target test.**

Append:

```python
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
            unique_suffix="1",
        )
        _insert_security_aggregate(
            session,
            owner=OWNER_2,
            authorizations=[{"address": DELEGATE_D2, "types": ["FORGET"]}],
            unique_suffix="2",
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
```

- [ ] **Step 6: Add the aggregate-key expansion test.**

Append:

```python
@pytest.mark.asyncio
async def test_forget_by_aggregate_key(
    session_factory: DbSessionFactory,
    forget_handler: ForgetMessageHandler,
):
    """O sends a FORGET that uses the `aggregates: [key]` field rather
    than explicit `hashes`. The handler expands the key to its element
    hashes via AggregateElementDb and runs the per-target check against
    each underlying AGGREGATE message. Verifies expansion still works
    under the new per-target authorization rule."""

    aggregate_key = "my_settings"
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
```

- [ ] **Step 7: Run all positive tests.**

Run: `venv/bin/pytest tests/permissions/test_forget_permissions.py -v -k "owner_forgets or second_delegate or cross_owner or aggregate_key"`

Expected: PASS for all six positive tests added in Tasks 2 and 4.

- [ ] **Step 8: Commit.**

```bash
git add tests/permissions/test_forget_permissions.py
git commit -m "test(forget): cover positive delegation cases (regression, second-delegate, cross-owner, aggregate expansion)"
```

---

## Task 5: Add the negative tests

These tests assert that the new authorization rule denies forgets that should not be allowed, including the intended tightening for revoked delegates and the all-or-nothing guarantee for multi-target forgets.

**Files:**
- Modify: `tests/permissions/test_forget_permissions.py`

### Steps

- [ ] **Step 1: Add the stranger test.**

Append:

```python
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
```

- [ ] **Step 2: Add the revoked-delegate test.**

Append:

```python
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
```

- [ ] **Step 3: Add the scoped-away-from-FORGET delegate test.**

Append:

```python
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
```

- [ ] **Step 4: Add the "no claim over self-signed delegate content" test.**

Append:

```python
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
```

- [ ] **Step 5: Add the all-or-nothing test.**

Append:

```python
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
```

- [ ] **Step 6: Run all negative tests.**

Run: `venv/bin/pytest tests/permissions/test_forget_permissions.py -v -k "cannot_forget or all_or_nothing"`

Expected: PASS for all five negative tests.

- [ ] **Step 7: Run the full new test file.**

Run: `venv/bin/pytest tests/permissions/test_forget_permissions.py -v`

Expected: PASS for all twelve tests added in Tasks 2, 4, 5 (1 canonical + 6 positive + 5 negative).

- [ ] **Step 8: Commit.**

```bash
git add tests/permissions/test_forget_permissions.py
git commit -m "test(forget): cover negative cases and all-or-nothing target validation"
```

---

## Task 6: Run the full test suite and lint

A final sweep to ensure no regressions in related areas (messages, permissions, process-forgets) and that the changes are properly formatted.

### Steps

- [ ] **Step 1: Run all tests that touch FORGET or permissions.**

Run:
```bash
venv/bin/pytest tests/permissions/ tests/message_processing/test_process_forgets.py tests/message_processing/test_process_forgotten_messages.py -v
```

Expected: PASS for every test.

- [ ] **Step 2: Format the modified files.**

Run:
```bash
venv/bin/black src/aleph/permissions.py src/aleph/handlers/content/forget.py tests/permissions/test_forget_permissions.py
venv/bin/isort src/aleph/permissions.py src/aleph/handlers/content/forget.py tests/permissions/test_forget_permissions.py
```

Expected: either no changes (already formatted) or only whitespace/import-order adjustments.

- [ ] **Step 3: If formatting produced changes, commit them.**

```bash
git status
# if there are modifications:
git add -u
git commit -m "style: format files modified by FORGET permission change"
```

- [ ] **Step 4: Run the full test suite.**

Run: `venv/bin/pytest -x -q`

Expected: PASS. If anything unrelated fails, investigate before considering the work complete.

---

## Notes for the implementer

- **No DB schema change, no migration.** The security aggregate schema already supports the `types: [...]` filter we rely on.
- **No existing test should need editing.** Each existing forget test uses `sender == content.address` for both the target and the FORGET, which is the self-owner case that passes identically under the new rule. If any existing test does fail after Task 3, stop and diagnose before patching it; it likely indicates an unintended behavior change.
- **Test hashes** in this plan are arbitrary 64-character hex strings. They do not need to match any real signature; `make_validated_message_from_dict` bypasses signature verification.
- **`OWNER_2`** is introduced as a constant in Task 4 (used for the cross-owner test). If your linter complains about it being unused after Task 2, add it together with the cross-owner test in Task 4.
