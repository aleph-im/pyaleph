# Authenticated IPFS Uploads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `POST /api/v0/ipfs/add_file` to accept a signed STORE message in the multipart form (mirroring `/storage/add_file`) and add an explicit size limit to the existing unauthenticated mode.

**Architecture:** Reuse the `MultipartUploadedFile` + `StorageMetadata` + `_verify_message_signature` + `_verify_user_balance` helpers from `storage.py`. Rewrite `ipfs.py`'s controller so metadata is optional; when present, signature + message content are validated before pinning, and a CID/balance check runs after pinning. On any post-pin failure, the pinned file gets a 24 h grace period (same mechanism the unauthenticated mode already uses).

**Tech Stack:** Python 3.12, aiohttp (web framework + multipart), pydantic (metadata schema), pytest + pytest-asyncio, `aleph-message` models, `hatch` test runner.

---

## Spec reference

Design doc: `docs/superpowers/specs/2026-04-23-authenticated-ipfs-uploads-design.md`. Read it before starting.

## File structure

**Modified files:**
- `src/aleph/config.py` (lines 212-243, the `ipfs` config block) — add two new config keys with defaults pulled from `aleph.toolkit.constants`.
- `src/aleph/web/controllers/ipfs.py` — rewrite `ipfs_add_file` to parse multipart with an optional `metadata` field, verify the message before pinning, and apply grace-on-post-pin-error.

**New file:**
- `tests/api/test_ipfs.py` — endpoint tests covering unauth regression + authenticated happy path + all documented error paths.

**Not modified:**
- `src/aleph/web/controllers/storage.py` — `_verify_message_signature`, `_verify_user_balance`, `StorageMetadata`, `MultipartUploadedFile` stay put; `ipfs.py` imports them. This matches the spec's "helper stays in storage.py" decision and avoids a cross-module refactor.
- `src/aleph/handlers/content/store.py` — the 1 MiB "fetch from network instead of pin" branch is explicitly out of scope (see spec § Non-goals).

## Testing strategy

All tests live in `tests/api/test_ipfs.py`. The existing `test_ipfs_add_file` test in `tests/api/test_storage.py` (line 372) stays put as a secondary regression guard — do not remove it.

The `api_client` fixture mocks `ipfs_service.add_bytes` to return a fixed CID regardless of input. This means **the file content bytes don't have to hash to the expected CID** — the mock returns whatever we configure. So test messages can hardcode their `item_hash` to the mocked CID.

`_verify_message_signature` is mocked in tests that don't specifically care about signature verification, to avoid having to maintain signed-message fixtures for IPFS. The one "bad signature" test mocks it with `side_effect=web.HTTPForbidden()` to simulate rejection.

Run tests with: `hatch run testing:test tests/api/test_ipfs.py -v`

---

## Task 1: Add IPFS-specific size config keys

**Files:**
- Modify: `src/aleph/config.py:212-243`

- [ ] **Step 1: Read the existing `ipfs` config block**

Confirm lines 212-243 still match what's in this plan. Open `src/aleph/config.py` and locate the block that starts with `"ipfs": {`.

- [ ] **Step 2: Add two size keys**

In the `ipfs` block, just after `"peers": [...]` and before `"pinning": {...}`, add:

```python
            # Maximum file size for authenticated uploads via /ipfs/add_file, in bytes.
            "max_upload_file_size": DEFAULT_MAX_FILE_SIZE,
            # Maximum file size for unauthenticated uploads via /ipfs/add_file, in bytes.
            "max_unauthenticated_upload_file_size": DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE,
```

- [ ] **Step 3: Verify the imports at the top of `config.py` already have both constants**

Run:

```bash
grep -n "DEFAULT_MAX_FILE_SIZE\|DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE" src/aleph/config.py | head -5
```

Expected: both constants appear in an import near the top of the file. If either is missing, add it to the existing `from aleph.toolkit.constants import ...` line.

- [ ] **Step 4: Sanity check — load the config in a REPL**

```bash
hatch run testing:python -c "from aleph.config import get_defaults; d = get_defaults(); print(d['ipfs']['max_upload_file_size'], d['ipfs']['max_unauthenticated_upload_file_size'])"
```

Expected output: `104857600 26214400` (100 MiB and 25 MiB in bytes).

- [ ] **Step 5: Commit**

```bash
git add src/aleph/config.py
git commit -m "$(cat <<'EOF'
feat: add IPFS-specific upload size config keys

Introduces ipfs.max_upload_file_size and
ipfs.max_unauthenticated_upload_file_size so the IPFS upload endpoint's
limits can be tuned independently from storage. Defaults match the
storage equivalents so existing deployments are unaffected.

Part of the authenticated /ipfs/add_file work
(docs/superpowers/specs/2026-04-23-authenticated-ipfs-uploads-design.md).
EOF
)"
```

---

## Task 2: Bootstrap `tests/api/test_ipfs.py` with the unauth regression test

**Files:**
- Create: `tests/api/test_ipfs.py`

- [ ] **Step 1: Create the test file with fixtures and the baseline regression test**

Create `tests/api/test_ipfs.py`:

```python
import json
from decimal import Decimal
from io import BytesIO
from typing import Any

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aleph_message.models import Chain
from in_memory_storage_engine import InMemoryStorageEngine

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.files import get_file
from aleph.db.models import AlephBalanceDb, GracePeriodFilePinDb
from aleph.storage import StorageService
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_status import MessageStatus
from aleph.web.controllers.app_state_getters import (
    APP_STATE_SIGNATURE_VERIFIER,
    APP_STATE_STORAGE_SERVICE,
)
from aleph.web.controllers.utils import BroadcastStatus, PublicationStatus

IPFS_ADD_FILE_URI = "/api/v0/ipfs/add_file"

FILE_CONTENT = b"Hello earthlings, I come in pieces"
EXPECTED_FILE_CID = "QmPoBEaYRf2HDHHFsD7tYkCcSdpLbx5CYDgCgDtW4ywhSK"
MOCK_FILE_SIZE = 34  # length of FILE_CONTENT

# An IPFS STORE message shaped like MESSAGE_DICT in test_storage.py, but with
# item_type=ipfs and item_hash=EXPECTED_FILE_CID. Signature verification is
# mocked away in tests that don't care about it, so this signature is not
# cryptographically valid.
IPFS_MESSAGE_DICT: dict[str, Any] = {
    "chain": "ETH",
    "sender": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
    "type": "STORE",
    "channel": "null",
    "signature": "0x" + "00" * 65,  # placeholder; signature verify is mocked
    "time": 1692193373.7144432,
    "item_type": "inline",
    "item_content": json.dumps(
        {
            "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
            "time": 1692193373.714271,
            "item_type": "ipfs",
            "item_hash": EXPECTED_FILE_CID,
            "mime_type": "application/octet-stream",
        }
    ),
    "item_hash": "8227acbc2f7c43899efd9f63ea9d8119a4cb142f3ba2db5fe499ccfab86dfaed",
}


@pytest_asyncio.fixture
async def api_client(ccn_test_aiohttp_app, mocker, aiohttp_client):
    ipfs_service = mocker.AsyncMock()
    ipfs_service.add_bytes = mocker.AsyncMock(return_value=EXPECTED_FILE_CID)
    ipfs_service.pinning_client.files.stat = mocker.AsyncMock(
        return_value={
            "Hash": EXPECTED_FILE_CID,
            "Size": MOCK_FILE_SIZE,
            "CumulativeSize": 42,
            "Blocks": 0,
            "Type": "file",
        }
    )

    ccn_test_aiohttp_app[APP_STATE_STORAGE_SERVICE] = StorageService(
        storage_engine=InMemoryStorageEngine(files={}),
        ipfs_service=ipfs_service,
        node_cache=mocker.AsyncMock(),
    )
    ccn_test_aiohttp_app[APP_STATE_SIGNATURE_VERIFIER] = SignatureVerifier()

    client = await aiohttp_client(ccn_test_aiohttp_app)
    return client


def _get_ipfs_service_mock(api_client):
    """Extract the mocked ipfs_service from the storage service in app state."""
    return api_client.app[APP_STATE_STORAGE_SERVICE].ipfs_service


def _has_grace_period(session, file_hash: str) -> bool:
    """Helper: is there an active grace-period pin for this file?"""
    return (
        session.query(GracePeriodFilePinDb)
        .filter_by(file_hash=file_hash)
        .first()
        is not None
    )


@pytest.mark.asyncio
async def test_unauth_upload_happy_path(
    api_client, session_factory: DbSessionFactory
):
    """Regression: existing unauthenticated upload still works."""
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    body = await response.text()
    assert response.status == 200, body
    payload = await response.json()
    assert payload["status"] == "success"
    assert payload["hash"] == EXPECTED_FILE_CID
    assert payload["size"] == MOCK_FILE_SIZE

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert file.type == FileType.FILE
        assert _has_grace_period(session, EXPECTED_FILE_CID)
```

Note: `get_ipfs_service_from_request` returns `storage_service.ipfs_service`, so installing the mocked `ipfs_service` on the `StorageService` is sufficient — no separate app-state slot exists.

- [ ] **Step 2: Run the test**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_unauth_upload_happy_path -v
```

Expected: PASS (this exercises today's behavior).

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "$(cat <<'EOF'
test: bootstrap test_ipfs.py with unauth regression

Adds the test file with the api_client fixture and a baseline test
asserting the existing unauthenticated /ipfs/add_file flow returns
the CID, persists the file row, and applies the 24 h grace period.

Scaffolds for the authenticated-upload tests that follow.
EOF
)"
```

---

## Task 3: Enforce explicit size limit for the unauth path

**Files:**
- Modify: `src/aleph/web/controllers/ipfs.py` — switch from `request.post()` to `request.multipart()` + `MultipartUploadedFile`, enforcing the new config value.
- Modify: `tests/api/test_ipfs.py` — add a size-overflow test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_unauth_upload_exceeding_size_limit(
    api_client, session_factory: DbSessionFactory
):
    """Unauth mode rejects files over max_unauthenticated_upload_file_size."""
    # Default unauth limit is 25 MiB. Send 26 MiB.
    oversized = b"x" * (26 * 1024 * 1024)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(oversized))

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 413, await response.text()
```

- [ ] **Step 2: Run — expect FAIL**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_unauth_upload_exceeding_size_limit -v
```

Expected: FAIL (current code has no explicit size check; test gets 200 or the aiohttp-default 413 at a different threshold).

- [ ] **Step 3: Rewrite `ipfs_add_file` to use multipart streaming + size cap**

Replace the entire body of `src/aleph/web/controllers/ipfs.py` with:

```python
import asyncio
import logging

from aiohttp import BodyPartReader, web

from aleph.db.accessors.files import upsert_file
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.storage import MultipartUploadedFile
from aleph.web.controllers.utils import add_grace_period_for_file

logger = logging.getLogger(__name__)


async def ipfs_add_file(request: web.Request):
    """
    Upload a file to IPFS.

    ---
    summary: Add file to IPFS
    tags:
      - IPFS
    requestBody:
      required: true
      content:
        multipart/form-data:
          schema:
            type: object
            required:
              - file
            properties:
              file:
                type: string
                format: binary
              metadata:
                type: string
                description: Optional JSON with a signed STORE message.
    responses:
      '200':
        description: Upload result with IPFS CID
      '403':
        description: IPFS is disabled on this node, or signature invalid
      '413':
        description: File too large
      '422':
        description: Invalid multipart or metadata
    """
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    max_unauthenticated_upload_file_size = (
        config.ipfs.max_unauthenticated_upload_file_size.value
    )

    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)

    uploaded_file = None
    filename = "file"
    try:
        if request.content_type != "multipart/form-data":
            raise web.HTTPBadRequest(
                reason="Expected Content-Type: multipart/form-data"
            )

        reader = await request.multipart()
        async for part in reader:
            if part is None:
                raise web.HTTPBadRequest(reason="Invalid multipart structure")
            if not isinstance(part, BodyPartReader):
                raise web.HTTPBadRequest(reason="Invalid multipart structure")

            if part.name == "file":
                filename = part.filename or "file"
                uploaded_file = MultipartUploadedFile(
                    part, max_unauthenticated_upload_file_size
                )
                await uploaded_file.read_and_validate()

        if uploaded_file is None:
            raise web.HTTPUnprocessableEntity(
                reason="Missing 'file' in multipart form."
            )

        temp_file = await uploaded_file.open_temp_file()
        file_content = await temp_file.read()
        if isinstance(file_content, str):
            file_content = file_content.encode("utf-8")

        cid = await ipfs_service.add_bytes(file_content)

        try:
            stats = await asyncio.wait_for(
                ipfs_service.pinning_client.files.stat(f"/ipfs/{cid}"),
                config.ipfs.stat_timeout.value,
            )
            size = stats["Size"]
        except TimeoutError:
            raise web.HTTPNotFound(reason="File not found on IPFS")

        with session_factory() as session:
            upsert_file(
                session=session,
                file_hash=cid,
                size=size,
                file_type=FileType.FILE,
            )
            add_grace_period_for_file(
                session=session, file_hash=cid, hours=grace_period
            )
            session.commit()

        return web.json_response(
            {
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            }
        )

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()
```

- [ ] **Step 4: Run both tests**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_unauth_upload_happy_path tests/api/test_ipfs.py::test_unauth_upload_exceeding_size_limit -v
```

Expected: both PASS.

- [ ] **Step 5: Run the legacy regression test in `test_storage.py` to make sure nothing broke**

```bash
hatch run testing:test tests/api/test_storage.py::test_ipfs_add_file -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/aleph/web/controllers/ipfs.py tests/api/test_ipfs.py
git commit -m "$(cat <<'EOF'
refactor: stream multipart in /ipfs/add_file, enforce size cap

Switches the unauthenticated /ipfs/add_file flow from request.post()
to request.multipart() + MultipartUploadedFile, matching /storage/add_file
and enforcing ipfs.max_unauthenticated_upload_file_size (default 25 MiB)
explicitly instead of relying on aiohttp's default.

Behavior is otherwise unchanged: file is pinned, upserted, and a 24 h
grace period is applied.
EOF
)"
```

---

## Task 4: Accept optional `metadata` field with signed STORE message

**Files:**
- Modify: `src/aleph/web/controllers/ipfs.py` — parse metadata, validate early, use authenticated size cap when metadata is present.
- Modify: `tests/api/test_ipfs.py` — add authenticated happy-path test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_happy_path(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Authenticated upload: small file, valid message, sufficient balance."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "aleph.web.controllers.ipfs.broadcast_and_process_message",
        new_callable=mocker.AsyncMock,
        return_value=BroadcastStatus(
            publication_status=PublicationStatus.from_failures([]),
            message_status=MessageStatus.PROCESSED,
        ),
    )

    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                chain=Chain.ETH,
                balance=Decimal(1000),
                eth_height=0,
            )
        )
        session.commit()

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": True}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    body = await response.text()
    assert response.status == 200, body
    payload = await response.json()
    assert payload["hash"] == EXPECTED_FILE_CID

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        # Authenticated uploads do NOT get a grace period (message anchors).
        assert not _has_grace_period(session, EXPECTED_FILE_CID)
```

- [ ] **Step 2: Run — expect FAIL**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_happy_path -v
```

Expected: FAIL (controller doesn't parse metadata yet; likely 200 but with a grace period applied, or the mocker.patch targets a symbol that doesn't exist yet).

- [ ] **Step 3: Rewrite the controller to handle metadata**

Replace `src/aleph/web/controllers/ipfs.py` with:

```python
import asyncio
import logging
import math

from aiohttp import BodyPartReader, web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType
from pydantic import ValidationError

from aleph.db.accessors.files import upsert_file
from aleph.schemas.cost_estimation_messages import CostEstimationStoreContent
from aleph.toolkit.constants import MiB
from aleph.types.files import FileType
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_session_factory_from_request,
    get_signature_verifier_from_request,
)
from aleph.web.controllers.storage import (
    MultipartUploadedFile,
    StorageMetadata,
    _verify_message_signature,
    _verify_user_balance,
)
from aleph.web.controllers.utils import (
    add_grace_period_for_file,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
)

logger = logging.getLogger(__name__)


async def ipfs_add_file(request: web.Request):
    """
    Upload a file to IPFS. Optionally include a signed STORE message so
    the upload is anchored to the aleph.im network in one call.

    ---
    summary: Add file to IPFS
    tags:
      - IPFS
    requestBody:
      required: true
      content:
        multipart/form-data:
          schema:
            type: object
            required:
              - file
            properties:
              file:
                type: string
                format: binary
              metadata:
                type: string
                description: >
                  Optional JSON with a signed STORE message
                  (item_type=ipfs). When present, the CID computed after
                  pinning must match message.content.item_hash.
    responses:
      '200':
        description: Upload result with IPFS CID
      '402':
        description: Insufficient balance for the STORE message
      '403':
        description: IPFS disabled on this node, or signature invalid
      '413':
        description: File too large
      '422':
        description: Invalid multipart, metadata, or CID mismatch
    """
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    max_upload_file_size = config.ipfs.max_upload_file_size.value
    max_unauthenticated_upload_file_size = (
        config.ipfs.max_unauthenticated_upload_file_size.value
    )

    ipfs_service = get_ipfs_service_from_request(request)
    if ipfs_service is None:
        raise web.HTTPForbidden(reason="IPFS is disabled on this node")

    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)

    uploaded_file = None
    metadata = None
    filename = "file"
    cid = None
    size = None

    try:
        if request.content_type != "multipart/form-data":
            raise web.HTTPBadRequest(
                reason="Expected Content-Type: multipart/form-data"
            )

        # Read the largest allowed limit here; we narrow it later once we
        # know whether metadata is present. This means unauthenticated
        # requests get a two-step check: the initial streaming cap is
        # max_upload_file_size, and a secondary check afterwards enforces
        # max_unauthenticated_upload_file_size.
        reader = await request.multipart()
        async for part in reader:
            if part is None:
                raise web.HTTPBadRequest(reason="Invalid multipart structure")
            if not isinstance(part, BodyPartReader):
                raise web.HTTPBadRequest(reason="Invalid multipart structure")

            if part.name == "file":
                filename = part.filename or "file"
                uploaded_file = MultipartUploadedFile(
                    part, max_upload_file_size
                )
                await uploaded_file.read_and_validate()
            elif part.name == "metadata":
                metadata = await part.read(decode=True)

        if uploaded_file is None:
            raise web.HTTPUnprocessableEntity(
                reason="Missing 'file' in multipart form."
            )

        # Narrow the effective cap for unauthenticated requests.
        if metadata is None and uploaded_file.size > max_unauthenticated_upload_file_size:
            raise web.HTTPRequestEntityTooLarge(
                actual_size=uploaded_file.size,
                max_size=max_unauthenticated_upload_file_size,
            )

        # Parse + validate the message BEFORE pinning (fail-fast).
        message = None
        message_content = None
        sync = False
        if metadata:
            metadata_bytes = (
                metadata.file.read()
                if isinstance(metadata, FileField)
                else metadata
            )
            try:
                storage_metadata = StorageMetadata.model_validate_json(
                    metadata_bytes
                )
            except ValidationError as e:
                raise web.HTTPUnprocessableEntity(
                    reason=f"Could not decode metadata: {e.json()}"
                )
            message = storage_metadata.message
            sync = storage_metadata.sync

            await _verify_message_signature(
                pending_message=message, signature_verifier=signature_verifier
            )
            if not message.item_content:
                raise web.HTTPUnprocessableEntity(
                    reason="Store message content needed"
                )
            try:
                message_content = CostEstimationStoreContent.model_validate_json(
                    message.item_content
                )
            except ValidationError as e:
                raise web.HTTPUnprocessableEntity(
                    reason=f"Invalid store message content: {e.json()}"
                )
            if message_content.item_type != ItemType.ipfs:
                raise web.HTTPUnprocessableEntity(
                    reason=(
                        "Expected item_type=ipfs in STORE message, "
                        f"got {message_content.item_type}"
                    )
                )

        # Pin to IPFS — side effect: file is now on the local IPFS node.
        temp_file = await uploaded_file.open_temp_file()
        file_content = await temp_file.read()
        if isinstance(file_content, str):
            file_content = file_content.encode("utf-8")

        cid = await ipfs_service.add_bytes(file_content)

        try:
            stats = await asyncio.wait_for(
                ipfs_service.pinning_client.files.stat(f"/ipfs/{cid}"),
                config.ipfs.stat_timeout.value,
            )
            size = stats["Size"]
        except TimeoutError:
            raise web.HTTPNotFound(reason="File not found on IPFS")

        # Post-pin: CID match, balance check, persist.
        # Failures from this point on must leave the pin covered by the
        # 24 h grace period so the GC doesn't strand it.
        try:
            if message_content is not None:
                message_content.estimated_size_mib = math.ceil(
                    uploaded_file.size / MiB
                )
                if message_content.item_hash != cid:
                    raise web.HTTPUnprocessableEntity(
                        reason=(
                            f"File hash does not match "
                            f"({cid} != {message_content.item_hash})"
                        )
                    )
                with session_factory() as session:
                    _verify_user_balance(
                        session=session,
                        content=message_content,
                        max_unauthenticated_upload_file_size=(
                            max_unauthenticated_upload_file_size
                        ),
                    )

            with session_factory() as session:
                upsert_file(
                    session=session,
                    file_hash=cid,
                    size=size,
                    file_type=FileType.FILE,
                )
                if message_content is None:
                    add_grace_period_for_file(
                        session=session, file_hash=cid, hours=grace_period
                    )
                session.commit()
        except web.HTTPException:
            with session_factory() as session:
                upsert_file(
                    session=session,
                    file_hash=cid,
                    size=size,
                    file_type=FileType.FILE,
                )
                add_grace_period_for_file(
                    session=session, file_hash=cid, hours=grace_period
                )
                session.commit()
            logger.warning(
                "Post-pin failure for %s; applied %dh grace period",
                cid,
                grace_period,
            )
            raise

        status_code = 200
        if message:
            broadcast_status = await broadcast_and_process_message(
                pending_message=message,
                sync=sync,
                request=request,
                logger=logger,
            )
            status_code = broadcast_status_to_http_status(broadcast_status)

        return web.json_response(
            data={
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            },
            status=status_code,
        )

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()
```

- [ ] **Step 4: Run the authenticated happy-path test plus both unauth tests**

```bash
hatch run testing:test tests/api/test_ipfs.py -v
```

Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/ipfs.py tests/api/test_ipfs.py
git commit -m "$(cat <<'EOF'
feat: accept signed STORE metadata in /ipfs/add_file

Adds an optional 'metadata' multipart field carrying a signed STORE
message with item_type=ipfs. Signature and content are validated before
pinning; CID match and balance are checked after pinning. On post-pin
error, the pinned file is covered by the 24 h grace period so no orphan
is left behind.

When metadata is provided, the authenticated size cap (100 MiB by
default) applies instead of the unauthenticated 25 MiB cap.
EOF
)"
```

---

## Task 5: Reject bad signatures before pinning

**Files:**
- Modify: `tests/api/test_ipfs.py` — add bad-signature test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_bad_signature(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Invalid signature is rejected BEFORE the file is pinned."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
        side_effect=web.HTTPForbidden(),
    )
    # Spy on add_bytes to confirm we never called it.
    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 403, await response.text()
    ipfs_service.add_bytes.assert_not_called()

    with session_factory() as session:
        assert get_file(session=session, file_hash=EXPECTED_FILE_CID) is None
```

(The `from aiohttp import web` import was added in Task 2's scaffold.)

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_bad_signature -v
```

Expected: PASS. (Controller already verifies before pinning per Task 4.)

If it FAILS because `add_bytes.assert_not_called()` misfires, re-read the controller to make sure signature verification happens before `ipfs_service.add_bytes(...)` — if not, move the verification up.

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file rejects bad signatures before pinning"
```

---

## Task 6: Reject wrong `item_type` in message

**Files:**
- Modify: `tests/api/test_ipfs.py` — add wrong-item-type test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_rejects_storage_item_type(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Message with item_type=storage is rejected, no pin happens."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    bad_message = {
        **IPFS_MESSAGE_DICT,
        "item_content": json.dumps(
            {
                "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                "time": 1692193373.714271,
                "item_type": "storage",
                "item_hash": "0214e5578f5acb5d36ea62255cbf1157a4bdde7b9612b5db4899b2175e310b6f",
                "mime_type": "application/octet-stream",
            }
        ),
    }

    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": bad_message, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()
    ipfs_service.add_bytes.assert_not_called()
```

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_rejects_storage_item_type -v
```

Expected: PASS. (The item_type check added in Task 4 handles this.)

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file rejects STORE messages with item_type=storage"
```

---

## Task 7: Enforce authenticated size cap (100 MiB)

**Files:**
- Modify: `tests/api/test_ipfs.py` — add oversized-auth test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_exceeding_authenticated_cap(
    api_client, session_factory: DbSessionFactory, mocker
):
    """Authenticated upload above max_upload_file_size (100 MiB) returns 413."""
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Build a 101 MiB payload. The test fixture doesn't care about content
    # since ipfs_service.add_bytes is mocked.
    oversized = b"x" * (101 * 1024 * 1024)
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(oversized))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 413, await response.text()
```

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_exceeding_authenticated_cap -v
```

Expected: PASS. (The `MultipartUploadedFile(part, max_upload_file_size)` constructor in the controller enforces the 100 MiB cap at read time.)

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file enforces 100 MiB cap for authenticated uploads"
```

---

## Task 8: CID mismatch returns 422 + leaves 24 h grace on the pin

**Files:**
- Modify: `tests/api/test_ipfs.py` — CID-mismatch test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_cid_mismatch(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    If the message.item_hash does not match the CID the daemon produced,
    return 422 and leave the pinned file under a 24 h grace period.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # Message claims a different CID than the fixture's mocked add_bytes.
    mismatched = {
        **IPFS_MESSAGE_DICT,
        "item_content": json.dumps(
            {
                "address": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
                "time": 1692193373.714271,
                "item_type": "ipfs",
                "item_hash": "QmDifferentCidThatWillNotMatchTheMockedOne",
                "mime_type": "application/octet-stream",
            }
        ),
    }

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": mismatched, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()

    # Pin happened (CID mismatch is a post-pin check), so the file is in DB
    # with a grace period.
    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert _has_grace_period(session, EXPECTED_FILE_CID)
```

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_cid_mismatch -v
```

Expected: PASS. (Controller raises `HTTPUnprocessableEntity` and the post-pin except block applies the grace period.)

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file CID mismatch returns 422 with grace period on pin"
```

---

## Task 9: Balance shortfall returns 402 + leaves 24 h grace on the pin

**Files:**
- Modify: `tests/api/test_ipfs.py` — balance-shortfall test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_insufficient_balance(
    api_client,
    session_factory: DbSessionFactory,
    mocker,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """
    File above the unauth threshold with insufficient balance returns 402
    and leaves the pinned file under a 24 h grace period.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )

    # 26 MiB payload > 25 MiB unauth cap, so balance check fires.
    payload = b"x" * (26 * 1024 * 1024)

    # No balance row inserted → balance is zero.

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(payload))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": False}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 402, await response.text()

    with session_factory() as session:
        file = get_file(session=session, file_hash=EXPECTED_FILE_CID)
        assert file is not None
        assert _has_grace_period(session, EXPECTED_FILE_CID)
```

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_insufficient_balance -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file balance shortfall returns 402 with grace on pin"
```

---

## Task 10: Balance check is skipped below the unauth threshold

**Files:**
- Modify: `tests/api/test_ipfs.py` — small-file-no-balance-check test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_small_file_skips_balance_check(
    api_client, session_factory: DbSessionFactory, mocker
):
    """
    For files below max_unauthenticated_upload_file_size, the balance check
    short-circuits even when balance is zero. Matches /storage/add_file's
    rule: anything you could have uploaded unauth for free doesn't need a
    balance check.

    We deliberately do NOT mock _verify_user_balance: its internal threshold
    short-circuit is exactly what we want to exercise. Zero balance + tiny
    file → request must succeed.
    """
    mocker.patch(
        "aleph.web.controllers.ipfs._verify_message_signature",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "aleph.web.controllers.ipfs.broadcast_and_process_message",
        new_callable=mocker.AsyncMock,
        return_value=BroadcastStatus(
            publication_status=PublicationStatus.from_failures([]),
            message_status=MessageStatus.PROCESSED,
        ),
    )

    # No balance inserted — balance is zero. File is 34 bytes << 25 MiB.
    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata",
        json.dumps({"message": IPFS_MESSAGE_DICT, "sync": True}),
        content_type="application/json",
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 200, await response.text()
```

Note: `_verify_user_balance` in `storage.py` (lines 153-170) short-circuits internally when `estimated_size_mib <= max_unauthenticated_upload_file_size / MiB`, so with a tiny file and zero balance it returns without raising. Getting a 200 here is evidence the threshold logic is being honored.

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_small_file_skips_balance_check -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file small authenticated files skip balance check"
```

---

## Task 11: Malformed metadata JSON returns 422

**Files:**
- Modify: `tests/api/test_ipfs.py` — malformed-metadata test.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_ipfs.py`:

```python
@pytest.mark.asyncio
async def test_auth_upload_malformed_metadata(
    api_client, session_factory: DbSessionFactory
):
    """Garbage in `metadata` returns 422 and does not pin."""
    ipfs_service = _get_ipfs_service_mock(api_client)

    form_data = aiohttp.FormData()
    form_data.add_field("file", BytesIO(FILE_CONTENT))
    form_data.add_field(
        "metadata", "not-json-at-all", content_type="application/json"
    )

    response = await api_client.post(IPFS_ADD_FILE_URI, data=form_data)
    assert response.status == 422, await response.text()
    ipfs_service.add_bytes.assert_not_called()
```

- [ ] **Step 2: Run**

```bash
hatch run testing:test tests/api/test_ipfs.py::test_auth_upload_malformed_metadata -v
```

Expected: PASS. (`StorageMetadata.model_validate_json` raises `ValidationError`, caught by the controller, returns 422 before the pin.)

- [ ] **Step 3: Commit**

```bash
git add tests/api/test_ipfs.py
git commit -m "test: /ipfs/add_file malformed metadata returns 422 before pin"
```

---

## Task 12: Full suite + lint pass

**Files:**
- None (validation only).

- [ ] **Step 1: Run formatter**

```bash
hatch run linting:fmt
```

Expected: no diff or auto-applied formatting in the files you touched. If a diff was applied, stage it.

- [ ] **Step 2: Run full lint**

```bash
hatch run linting:all
```

Expected: clean. If mypy or ruff complain about the new code, fix inline. The most likely issues:
- Unused imports in `ipfs.py` from the earlier version (e.g. `FileField` — keep only what's used).
- `cid` and `size` being `Optional` when first declared; narrow with `assert` inside the post-pin block if mypy complains.

- [ ] **Step 3: Run the full test file**

```bash
hatch run testing:test tests/api/test_ipfs.py -v
```

Expected: all tests PASS.

- [ ] **Step 4: Run the storage test file to confirm no regression**

```bash
hatch run testing:test tests/api/test_storage.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit any lint fixes**

```bash
git add -A
git status
# Only commit if there are staged changes from lint fixes.
git commit -m "style: apply linting fixes for authenticated IPFS uploads"
```

(If no changes, skip this commit.)

---

## Out-of-scope follow-ups

Tracked in the spec's "Open questions / deferred work" section. These are **not** part of this plan:

1. The unauthenticated `/ipfs/add_file` open write surface — still open to the internet.
2. The < 1 MiB "fetch from network instead of pin" quirk in `/messages` STORE processing.
3. Streaming uploads (only relevant if the 100 MiB cap is raised significantly).

---

## Done when

- All 12 tasks are complete and committed.
- `hatch run testing:test tests/api/test_ipfs.py tests/api/test_storage.py -v` is all green.
- `hatch run linting:all` is clean.
- The endpoint accepts the new `metadata` field per the contract in the spec.
- The unauthenticated path still behaves as it did before, plus the new explicit 25 MiB cap.
