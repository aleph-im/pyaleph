# contentFormat Query Parameter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `contentFormat=full|headers|none` query parameter to `GET /api/v0/messages.json` that returns a reduced, per-type metadata subset of `content` (`headers`) built entirely from denormalized columns, deferring the heavy `content` JSONB.

**Architecture:** A new `ContentFormat` enum drives three behaviours. `none` reproduces today's `excludeContent` (defer + drop content). `headers` also defers the JSONB but rebuilds a small `content` dict from the already-loaded denormalized columns (`owner`, `content_type`, `content_ref`, `content_key`, `content_item_hash`). `excludeContent` is kept as a deprecated alias resolved to `none` by a model validator, so all downstream code reads a single resolved `content_format`. The websocket path supports two states only (`full` / stripped); `headers` degrades to `none` there.

**Tech Stack:** Python 3.12, pydantic v2, SQLAlchemy 2 (ORM `defer`), aiohttp, pytest / pytest-asyncio.

---

## Environment notes (read before running anything)

- **All work happens in the worktree** `/home/olivier/git/aleph/pyaleph/.claude/worktrees/od+messages-content-format`. Run every command from there.
- The venv (`/home/olivier/git/aleph/pyaleph/venv`) has `aleph` installed **editable against the MAIN repo src**, not the worktree. You MUST prefix test/python runs with `PYTHONPATH=src` so the worktree code is imported:

  ```bash
  PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -v
  ```

  Verify once: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -c "import aleph,os;print(os.path.dirname(aleph.__file__))"` must print a path under `.claude/worktrees/`.
- DB-backed tests (anything using `session_factory` / `ccn_api_client` / `fixture_messages`) require a local Postgres reachable at `127.0.0.1` (the `session_factory` fixture drops/recreates the `public` schema and runs migrations). The pure-logic tests in Task 2 and Task 3 do **not** need a DB.
- Lint/format commands (use the venv directly, not hatch — hatch is not installed here):
  ```bash
  /home/olivier/git/aleph/pyaleph/venv/bin/black src tests
  /home/olivier/git/aleph/pyaleph/venv/bin/isort src tests
  /home/olivier/git/aleph/pyaleph/venv/bin/ruff check src tests
  ```

## File structure

- Create: `src/aleph/types/content_format.py` — the `ContentFormat` enum (neutral home next to `sort_order.py`, `message_status.py`; avoids circular imports).
- Modify: `src/aleph/schemas/messages_query_params.py` — add `content_format` field + resolution validator on `BaseMessageQueryParams`; mark `exclude_content` deprecated.
- Modify: `src/aleph/web/controllers/messages.py` — `build_headers_content` helper; refactor `message_to_dict`, `format_response`, `view_messages_list`, `_send_history_to_ws`, and the `_WsClient` construction to use `content_format`; update the OpenAPI docstring.
- Create: `tests/api/test_content_format.py` — unit tests for `build_headers_content` (DB-free) and query-param resolution (DB-free).
- Modify: `tests/api/test_list_messages.py` — integration tests for `contentFormat` on the list endpoint and WS history (DB-backed), alongside the existing `excludeContent` tests.

---

## Task 1: `ContentFormat` enum

**Files:**
- Create: `src/aleph/types/content_format.py`
- Test: `tests/api/test_content_format.py`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_content_format.py`:

```python
from aleph.types.content_format import ContentFormat


def test_content_format_values():
    assert ContentFormat.FULL.value == "full"
    assert ContentFormat.HEADERS.value == "headers"
    assert ContentFormat.NONE.value == "none"
    # str-enum: compares equal to its raw value
    assert ContentFormat.HEADERS == "headers"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'aleph.types.content_format'`

- [ ] **Step 3: Write minimal implementation**

Create `src/aleph/types/content_format.py`:

```python
from enum import Enum


class ContentFormat(str, Enum):
    """Level of message ``content`` detail returned by the messages API.

    * ``full``    - the complete content (default).
    * ``headers`` - a reduced, per-type metadata subset built from
                    denormalized columns; the content JSONB is not read.
    * ``none``    - content omitted entirely (the behaviour of the
                    deprecated ``excludeContent=true`` flag).
    """

    FULL = "full"
    HEADERS = "headers"
    NONE = "none"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/aleph/types/content_format.py tests/api/test_content_format.py
git commit -m "feat(messages): add ContentFormat enum"
```

---

## Task 2: `build_headers_content` helper

Builds the reduced `content` dict from denormalized columns, keyed on message type. DB-free: it only reads attributes already set on the `MessageDb` instance (`MessageDb.__init__` derives `owner`/`content_type`/`content_ref`/`content_key`/`content_item_hash` from the `content` dict).

**Files:**
- Modify: `src/aleph/web/controllers/messages.py`
- Test: `tests/api/test_content_format.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_content_format.py`:

```python
from aleph_message.models import Chain, ItemType, MessageType

from aleph.db.models import MessageDb
from aleph.web.controllers.messages import build_headers_content


def _make_message(message_type: MessageType, content: dict) -> MessageDb:
    # MessageDb.__init__ derives the denormalized columns from `content`.
    return MessageDb(
        item_hash="q" * 64,
        type=message_type,
        chain=Chain.ETH,
        sender="0xSENDER",
        signature=None,
        item_type=ItemType.inline,
        item_content=None,
        content=content,
        channel=None,
        size=0,
    )


def test_headers_post_keeps_type_and_ref():
    msg = _make_message(
        MessageType.post,
        {"address": "0xABC", "time": 1.0, "type": "my-type", "ref": "ref123",
         "content": {"big": "x" * 1000}},
    )
    assert build_headers_content(msg) == {
        "address": "0xABC",
        "type": "my-type",
        "ref": "ref123",
    }


def test_headers_post_omits_missing_ref():
    msg = _make_message(
        MessageType.post,
        {"address": "0xABC", "time": 1.0, "type": "my-type"},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "type": "my-type"}


def test_headers_aggregate_keeps_key():
    msg = _make_message(
        MessageType.aggregate,
        {"address": "0xABC", "time": 1.0, "key": "my-key", "content": {"a": 1}},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "key": "my-key"}


def test_headers_store_keeps_item_hash_and_ref():
    msg = _make_message(
        MessageType.store,
        {"address": "0xABC", "time": 1.0, "item_type": "ipfs",
         "item_hash": "Qm123", "ref": "ref456"},
    )
    assert build_headers_content(msg) == {
        "address": "0xABC",
        "item_hash": "Qm123",
        "ref": "ref456",
    }


def test_headers_store_omits_missing_ref():
    msg = _make_message(
        MessageType.store,
        {"address": "0xABC", "time": 1.0, "item_type": "ipfs", "item_hash": "Qm123"},
    )
    assert build_headers_content(msg) == {"address": "0xABC", "item_hash": "Qm123"}


def test_headers_forget_address_only():
    msg = _make_message(
        MessageType.forget,
        {"address": "0xABC", "time": 1.0, "hashes": ["Qm1"], "reason": "spam"},
    )
    assert build_headers_content(msg) == {"address": "0xABC"}


def test_headers_program_address_only():
    msg = _make_message(
        MessageType.program,
        {"address": "0xABC", "time": 1.0, "type": "vm-function"},
    )
    # PROGRAM content has a `type` field, but headers mode does not expose it.
    assert build_headers_content(msg) == {"address": "0xABC"}


def test_headers_instance_address_only():
    msg = _make_message(
        MessageType.instance,
        {"address": "0xABC", "time": 1.0},
    )
    assert build_headers_content(msg) == {"address": "0xABC"}
```

Note: PROGRAM content has a `type` field, so `content_type` will be populated; the test asserts headers mode still returns address-only for PROGRAM, which pins the per-type field map (PROGRAM is not in the map, so no extra fields are emitted).

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_headers_content'`

- [ ] **Step 3: Write minimal implementation**

In `src/aleph/web/controllers/messages.py`, add `MessageType` to the existing `aleph_message.models` import:

```python
from aleph_message.models import ItemHash, MessageType
```

Add the `ContentFormat` import alongside the other `aleph.types` imports:

```python
from aleph.types.content_format import ContentFormat
```

Then add the helper directly above the existing `message_to_dict` function (around line 293):

```python
# Per-type reduced "headers" content: (output key in content, MessageDb attribute).
# `address` is emitted for every type from the `owner` column and is handled
# separately. All source attributes are denormalized columns, so building the
# reduced content never touches the (deferred) content JSONB.
_HEADERS_FIELDS: Dict[MessageType, List[tuple]] = {
    MessageType.post: [("type", "content_type"), ("ref", "content_ref")],
    MessageType.aggregate: [("key", "content_key")],
    MessageType.store: [("item_hash", "content_item_hash"), ("ref", "content_ref")],
    MessageType.program: [],
    MessageType.instance: [],
    MessageType.forget: [],
}


def build_headers_content(message: MessageDb) -> Dict[str, Any]:
    """Reduced ``content`` for ``contentFormat=headers``, built from columns.

    `address` is always included (from ``owner``); the per-type fields in
    ``_HEADERS_FIELDS`` are included when their column value is not ``None``.
    """
    content: Dict[str, Any] = {"address": message.owner}
    for output_key, attr in _HEADERS_FIELDS.get(message.type, []):
        value = getattr(message, attr)
        if value is not None:
            content[output_key] = value
    return content
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -v`
Expected: PASS (all build_headers_content tests + the Task 1 test)

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/messages.py tests/api/test_content_format.py
git commit -m "feat(messages): add build_headers_content helper"
```

---

## Task 3: `content_format` query parameter + resolution

Add the field to `BaseMessageQueryParams` and a `model_validator(mode="after")` that collapses `contentFormat` + `excludeContent` into a single concrete `content_format`.

**Files:**
- Modify: `src/aleph/schemas/messages_query_params.py`
- Test: `tests/api/test_content_format.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_content_format.py`:

```python
from aleph.schemas.messages_query_params import MessageQueryParams


def test_content_format_default_is_full():
    params = MessageQueryParams.model_validate({})
    assert params.content_format == ContentFormat.FULL


def test_exclude_content_true_resolves_to_none():
    params = MessageQueryParams.model_validate({"excludeContent": "true"})
    assert params.content_format == ContentFormat.NONE


def test_explicit_content_format_overrides_exclude_content():
    params = MessageQueryParams.model_validate(
        {"excludeContent": "true", "contentFormat": "full"}
    )
    assert params.content_format == ContentFormat.FULL


def test_content_format_headers_parsed():
    params = MessageQueryParams.model_validate({"contentFormat": "headers"})
    assert params.content_format == ContentFormat.HEADERS


def test_content_format_invalid_rejected():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        MessageQueryParams.model_validate({"contentFormat": "bogus"})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -k content_format -v`
Expected: FAIL (`content_format` attribute does not exist / not resolved)

- [ ] **Step 3: Write minimal implementation**

In `src/aleph/schemas/messages_query_params.py`, add the import near the top:

```python
from aleph.types.content_format import ContentFormat
```

Update the existing `exclude_content` field description to mark it deprecated, and add the `content_format` field immediately after it (currently around line 84):

```python
    exclude_content: bool = Field(
        default=False,
        alias="excludeContent",
        description="Deprecated: use contentFormat=none instead. If true (and "
        "contentFormat is not set), omit the 'content' field from each message.",
    )

    content_format: Optional[ContentFormat] = Field(
        default=None,
        alias="contentFormat",
        description="Level of content detail: 'full' (default) returns the "
        "complete content; 'headers' returns a reduced per-type metadata subset "
        "(address, plus type/ref for POST, key for AGGREGATE, item_hash/ref for "
        "STORE); 'none' omits content entirely. Takes precedence over "
        "excludeContent when set.",
    )
```

Add a resolution validator to `BaseMessageQueryParams`. Place it right after the existing `validate_field_dependencies` method (around line 131):

```python
    @model_validator(mode="after")
    def resolve_content_format(self):
        # Collapse the deprecated excludeContent flag into content_format so all
        # downstream code reads a single concrete value. Explicit contentFormat
        # always wins.
        if self.content_format is None:
            self.content_format = (
                ContentFormat.NONE if self.exclude_content else ContentFormat.FULL
            )
        return self
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py -v`
Expected: PASS (all tests in the file)

- [ ] **Step 5: Commit**

```bash
git add src/aleph/schemas/messages_query_params.py tests/api/test_content_format.py
git commit -m "feat(messages): add contentFormat query param with excludeContent alias"
```

---

## Task 4: Wire `content_format` into the list endpoint

Refactor `message_to_dict`, `format_response`, and `view_messages_list` (both cursor and legacy page modes) to use the resolved `content_format` instead of the `exclude_content` bool.

**Files:**
- Modify: `src/aleph/web/controllers/messages.py:293-346` (`message_to_dict`, `format_response`) and `:482-562` (`view_messages_list`)
- Test: `tests/api/test_list_messages.py`

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/api/test_list_messages.py` (after the existing `test_exclude_content*` tests, around line 924):

```python
@pytest.mark.asyncio
async def test_content_format_headers_per_type(
    fixture_messages: Sequence[Dict[str, Any]], ccn_api_client
):
    """contentFormat=headers returns the reduced per-type content subset."""
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"contentFormat": "headers"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()

    by_hash = {m["item_hash"]: m for m in data["messages"]}
    assert len(by_hash) == len(fixture_messages)

    for original in fixture_messages:
        msg = by_hash[original["item_hash"]]
        content = msg["content"]
        msg_type = original["type"]

        # address always present (sourced from the owner column)
        assert content["address"] == original["content"]["address"]
        # heavy nested user payload is never present in headers mode
        assert "content" not in content
        # content.time is intentionally omitted; top-level time still present
        assert "time" not in content
        assert "time" in msg

        if msg_type == "POST":
            assert content.get("type") == original["content"].get("type")
            assert set(content.keys()) <= {"address", "type", "ref"}
        elif msg_type == "AGGREGATE":
            assert content["key"] == original["content"]["key"]
            assert set(content.keys()) == {"address", "key"}
        elif msg_type == "STORE":
            assert content["item_hash"] == original["content"]["item_hash"]
            assert set(content.keys()) <= {"address", "item_hash", "ref"}
        elif msg_type == "FORGET":
            assert set(content.keys()) == {"address"}


@pytest.mark.asyncio
async def test_content_format_none_matches_exclude_content(
    fixture_messages: Sequence[Dict[str, Any]], ccn_api_client
):
    """contentFormat=none drops content just like excludeContent=true."""
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"contentFormat": "none"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    assert len(data["messages"]) == len(fixture_messages)
    for msg in data["messages"]:
        assert "content" not in msg
        assert "item_hash" in msg


@pytest.mark.asyncio
async def test_content_format_full_is_default(
    fixture_messages: Sequence[Dict[str, Any]], ccn_api_client
):
    """contentFormat=full returns the complete content (same as no param)."""
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"contentFormat": "full"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    for msg in data["messages"]:
        assert "content" in msg
        # a full content carries the nested time field
        assert "time" in msg["content"]


@pytest.mark.asyncio
async def test_content_format_headers_cursor_pagination(
    fixture_messages: Sequence[Dict[str, Any]], ccn_api_client
):
    """headers mode works with cursor pagination."""
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"contentFormat": "headers", "pagination": "2"}
    )
    assert response.status == 200, await response.text()
    data = await response.json()
    for msg in data["messages"]:
        assert "address" in msg["content"]
        assert "content" not in msg["content"]


@pytest.mark.asyncio
async def test_content_format_invalid_returns_422(
    fixture_messages: Sequence[Dict[str, Any]], ccn_api_client
):
    response = await ccn_api_client.get(
        MESSAGES_URI, params={"contentFormat": "bogus"}
    )
    assert response.status == 422, await response.text()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -k content_format -v`
Expected: FAIL — `contentFormat` is parsed but ignored, so `headers`/`none`/`full` all return full content (assertions on reduced content fail; the 422 test already passes).

- [ ] **Step 3a: Refactor `message_to_dict`**

Replace the current `message_to_dict` body (lines ~293-312) with:

```python
def message_to_dict(
    message: MessageDb, content_format: ContentFormat = ContentFormat.FULL
) -> Dict[str, Any]:
    if content_format == ContentFormat.FULL:
        message_dict = message.to_dict()
    else:
        message_dict = message.to_dict(exclude={"content"})
        if content_format == ContentFormat.HEADERS:
            message_dict["content"] = build_headers_content(message)
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)

    # Remove denormalized columns from API response to avoid breaking SDKs
    for key in MessageDb.DENORMALIZED_COLUMNS:
        message_dict.pop(key, None)

    return message_dict
```

- [ ] **Step 3b: Refactor `format_response`**

Change the `format_response` signature and the `message_to_dict` call (lines ~327-337):

```python
def format_response(
    messages: Iterable[MessageDb],
    pagination: int,
    page: int,
    total_messages: int,
    content_format: ContentFormat = ContentFormat.FULL,
) -> web.Response:
    formatted_messages = [
        message_to_dict(message, content_format=content_format)
        for message in messages
    ]
```

(Leave the rest of `format_response` unchanged.)

- [ ] **Step 3c: Refactor `view_messages_list`**

Replace the `exclude_content` extraction (lines ~482-484):

```python
    find_filters = query_params.model_dump(exclude_none=True)

    content_format: ContentFormat = query_params.content_format
    # Both keys are consumed here; neither is a query filter.
    find_filters.pop("content_format", None)
    find_filters.pop("exclude_content", None)
```

In the cursor-mode branch, replace the defer block (lines ~509-510) and the formatting call (line ~520):

```python
        if content_format != ContentFormat.FULL:
            messages_query = messages_query.options(defer(MessageDb.content))
```

```python
        formatted = [
            message_to_dict(m, content_format=content_format) for m in messages
        ]
```

In the legacy page-mode branch, replace the defer block (lines ~544-545):

```python
            if content_format != ContentFormat.FULL:
                messages_query = messages_query.options(defer(MessageDb.content))
```

and the `format_response` call (lines ~556-562):

```python
        return format_response(
            messages,
            pagination=pagination_per_page,
            page=pagination_page,
            total_messages=total_msgs,
            content_format=content_format,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -k "content_format or exclude_content" -v`
Expected: PASS (new contentFormat tests AND the pre-existing excludeContent tests, which now flow through the resolver).

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/messages.py tests/api/test_list_messages.py
git commit -m "feat(messages): honor contentFormat on the messages list endpoint"
```

---

## Task 5: Websocket path (headers degrades to none)

The WS path supports two states only. Map `headers` to `none` there so behaviour stays backward compatible and predictable.

**Files:**
- Modify: `src/aleph/web/controllers/messages.py` — `_send_history_to_ws` (lines ~571-587) and the `_WsClient` construction (line ~689)
- Test: `tests/api/test_list_messages.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_list_messages.py`:

```python
@pytest.mark.asyncio
async def test_content_format_ws_history_none(
    fixture_messages: Sequence[Dict[str, Any]],
    session_factory: DbSessionFactory,
):
    """_send_history_to_ws with contentFormat=none strips content."""
    from unittest.mock import AsyncMock

    from aleph.web.controllers.messages import _send_history_to_ws

    ws = AsyncMock()
    history = len(fixture_messages)
    query_params = WsMessageQueryParams(history=history, contentFormat="none")
    await _send_history_to_ws(
        ws=ws, session_factory=session_factory, history=history,
        query_params=query_params,
    )
    assert ws.send_str.call_count == len(fixture_messages)
    for call in ws.send_str.call_args_list:
        payload = json.loads(call.args[0])
        assert "content" not in payload


@pytest.mark.asyncio
async def test_content_format_ws_history_headers_degrades_to_none(
    fixture_messages: Sequence[Dict[str, Any]],
    session_factory: DbSessionFactory,
):
    """WS does not support headers; it degrades to none (content stripped)."""
    from unittest.mock import AsyncMock

    from aleph.web.controllers.messages import _send_history_to_ws

    ws = AsyncMock()
    history = len(fixture_messages)
    query_params = WsMessageQueryParams(history=history, contentFormat="headers")
    await _send_history_to_ws(
        ws=ws, session_factory=session_factory, history=history,
        query_params=query_params,
    )
    assert ws.send_str.call_count == len(fixture_messages)
    for call in ws.send_str.call_args_list:
        payload = json.loads(call.args[0])
        assert "content" not in payload
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -k "ws_history_none or ws_history_headers" -v`
Expected: FAIL — `_send_history_to_ws` still reads `exclude_content` only, so `contentFormat=none/headers` (with `excludeContent` unset) leaves content in the payload.

- [ ] **Step 3: Refactor `_send_history_to_ws` and `_WsClient` construction**

Replace the head of `_send_history_to_ws` (lines ~571-580):

```python
    find_filters = query_params.model_dump(exclude_none=True)
    content_format: ContentFormat = query_params.content_format
    find_filters.pop("content_format", None)
    find_filters.pop("exclude_content", None)

    # The websocket payload supports two states only: content present or absent.
    # `headers` is not implemented here, so it degrades to `none`.
    if content_format == ContentFormat.HEADERS:
        content_format = ContentFormat.NONE

    messages_query = make_matching_messages_query(
        pagination=history,
        include_confirmations=True,
        **find_filters,
    )
    if content_format != ContentFormat.FULL:
        messages_query = messages_query.options(defer(MessageDb.content))
```

Replace the `message_to_dict` call in the same function (line ~586):

```python
        msg_dict = message_to_dict(message, content_format=content_format)
```

Replace the `_WsClient` construction (line ~689) so live broadcast strips content for both `none` and `headers`:

```python
        client = _WsClient(
            ws, query_params, query_params.content_format != ContentFormat.FULL
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -k "ws_history" -v`
Expected: PASS (the two new WS tests AND the pre-existing `test_exclude_content_ws_history*` tests).

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/messages.py tests/api/test_list_messages.py
git commit -m "feat(messages): support contentFormat in websocket history (headers->none)"
```

---

## Task 6: OpenAPI docstring

Document `contentFormat` and mark `excludeContent` deprecated in the `view_messages_list` docstring.

**Files:**
- Modify: `src/aleph/web/controllers/messages.py:443-448`

- [ ] **Step 1: Update the docstring**

Replace the `excludeContent` parameter block (lines ~443-448) with:

```python
      - name: excludeContent
        in: query
        deprecated: true
        schema:
          type: boolean
          default: false
        description: >-
          Deprecated: use contentFormat=none. If true (and contentFormat is not
          set), omit the 'content' field from each message.
      - name: contentFormat
        in: query
        schema:
          type: string
          enum: [full, headers, none]
          default: full
        description: >-
          Level of content detail. 'full' (default) returns the complete
          content. 'headers' returns a reduced per-type metadata subset
          (address; plus type/ref for POST, key for AGGREGATE, item_hash/ref for
          STORE) without reading the content JSONB. 'none' omits content
          entirely. Takes precedence over excludeContent.
```

- [ ] **Step 2: Verify the endpoint still imports and serves**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_list_messages.py -k content_format -v`
Expected: PASS (docstring is a YAML comment; this just confirms nothing broke).

- [ ] **Step 3: Commit**

```bash
git add src/aleph/web/controllers/messages.py
git commit -m "docs(messages): document contentFormat and deprecate excludeContent in OpenAPI"
```

---

## Task 7: Format, lint, full test pass

- [ ] **Step 1: Format**

```bash
/home/olivier/git/aleph/pyaleph/venv/bin/black src tests
/home/olivier/git/aleph/pyaleph/venv/bin/isort src tests
```

- [ ] **Step 2: Lint**

```bash
/home/olivier/git/aleph/pyaleph/venv/bin/ruff check src/aleph/web/controllers/messages.py src/aleph/schemas/messages_query_params.py src/aleph/types/content_format.py tests/api/test_content_format.py
```
Expected: no errors. (mypy is known to have a sqlalchemy-stubs conflict locally; skip unless it runs cleanly.)

- [ ] **Step 3: Full targeted test run**

```bash
PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/pytest tests/api/test_content_format.py tests/api/test_list_messages.py -v
```
Expected: all PASS, including the pre-existing `excludeContent` tests.

- [ ] **Step 4: Commit any formatting changes**

```bash
git add -A
git commit -m "style: format contentFormat changes" || echo "nothing to format"
```

---

## Finishing

When all tasks pass, hand off to the `superpowers:finishing-a-development-branch` skill. Per project convention, the final PR tree must **not** include `docs/superpowers/specs/*` or `docs/superpowers/plans/*`; remove them with a dedicated `chore: unstage docs` commit before opening the PR.

## Self-review notes (spec coverage)

- contentFormat enum (full/headers/none) → Task 1.
- Reduced per-type field map (address everywhere; POST type+ref; AGGREGATE key; STORE item_hash+ref; PROGRAM/INSTANCE/FORGET address-only; NULL omitted; no content.time) → Task 2 (unit) + Task 4 (integration).
- New param + excludeContent deprecated alias + precedence resolution → Task 3.
- Defer content JSONB for headers and none; full/none/headers wiring on list endpoint (cursor + page modes) → Task 4.
- find_filters strips content_format and exclude_content → Tasks 4 and 5.
- WS supports full/none only, headers degrades to none (history + live broadcast) → Task 5.
- OpenAPI docstring updated → Task 6.
- Single-message endpoint and a third WS variant are explicitly out of scope (see spec).
