# Unauthenticated Uploads Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Immediate mitigation of the unauthenticated upload surface: cap `add_json` at 25 MiB, shorten grace period and GC interval, and emit deprecation signals on all anonymous uploads.

**Architecture:** Node-local API hardening only, per the spec `docs/superpowers/specs/2026-06-09-unauthenticated-uploads-design.md` (phase 1 section). No protocol, schema or SDK changes. Phase 2 (mandatory signed-message submission, multipart `POST /api/v0/messages`, deletion of `add_json` and all anonymous paths) is deliberately NOT in this plan; it is blocked on SDK releases and gets its own plan when the deprecation window ends.

**Tech Stack:** Python 3, aiohttp, configmanager, pytest + pytest-asyncio.

**Environment notes (read first):**

- All work happens in a git worktree (use the superpowers:using-git-worktrees skill), branch `fix/unauthenticated-uploads-phase1` off `main`. Do NOT switch branches in the main checkout.
- The venv lives in the MAIN repo. From the worktree, run tests as:
  `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest <path> -v`
- Formatters: `/home/olivier/git/aleph/pyaleph/venv/bin/black` and `.../venv/bin/isort`.
- Commit messages: imperative, conventional-commit style, NO em dashes, NO Co-Authored-By trailers.
- `docs/superpowers/` files get committed on the branch while working, then removed from the tree in a final `unstage docs` commit before the PR (project convention).

---

### Task 1: Worktree and docs setup

**Files:**
- Create: worktree at `/home/olivier/git/aleph/pyaleph-uploads-phase1`
- Copy in: `docs/superpowers/specs/2026-06-09-unauthenticated-uploads-design.md` and this plan (both are untracked in the main checkout)

- [ ] **Step 1: Create the worktree and branch**

```bash
git -C /home/olivier/git/aleph/pyaleph worktree add /home/olivier/git/aleph/pyaleph-uploads-phase1 -b fix/unauthenticated-uploads-phase1 main
cd /home/olivier/git/aleph/pyaleph-uploads-phase1
```

- [ ] **Step 2: Copy the spec and plan into the worktree and commit**

```bash
mkdir -p docs/superpowers/specs docs/superpowers/plans
cp /home/olivier/git/aleph/pyaleph/docs/superpowers/specs/2026-06-09-unauthenticated-uploads-design.md docs/superpowers/specs/
cp /home/olivier/git/aleph/pyaleph/docs/superpowers/plans/2026-06-09-unauthenticated-uploads-phase1.md docs/superpowers/plans/
git add docs/superpowers
git commit -m "docs: add unauthenticated uploads spec and phase 1 plan"
```

- [ ] **Step 3: Verify the test environment works**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py::test_storage_add_json -v`
Expected: PASS (requires local postgres/redis as for any test run).

---

### Task 2: Size cap on both add_json endpoints

**Files:**
- Modify: `src/aleph/web/controllers/storage.py` (imports, new helper, `add_ipfs_json_controller` at ~line 62, `add_storage_json_controller` at ~line 105)
- Test: `tests/api/test_storage.py`

Background: both controllers currently do `data = await request.json()`, which buffers up to aiohttp's `client_max_size` (100 MiB, set in `src/aleph/web/__init__.py:40`). The documented anonymous cap is `storage.max_unauthenticated_upload_file_size` (25 MiB). We stream-read with an explicit cap and abort early.

- [ ] **Step 1: Write the failing tests**

Add to `tests/api/test_storage.py` (after `test_ipfs_add_json`, ~line 615; `STORAGE_ADD_JSON_URI` and `IPFS_ADD_JSON_URI` constants already exist at the top of the file):

```python
@pytest.mark.asyncio
async def test_storage_add_json_over_size_limit(api_client, mock_config):
    mock_config.storage.max_unauthenticated_upload_file_size.value = 1024
    response = await api_client.post(STORAGE_ADD_JSON_URI, json={"data": "a" * 2048})
    assert response.status == 413, await response.text()


@pytest.mark.asyncio
async def test_ipfs_add_json_over_size_limit(api_client, mock_config):
    mock_config.storage.max_unauthenticated_upload_file_size.value = 1024
    response = await api_client.post(IPFS_ADD_JSON_URI, json={"data": "a" * 2048})
    assert response.status == 413, await response.text()


@pytest.mark.asyncio
async def test_storage_add_json_invalid_json(api_client):
    response = await api_client.post(
        STORAGE_ADD_JSON_URI,
        data=b"{not valid json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status == 422, await response.text()
```

Note: `mock_config` is the session-level config proxy fixture from `tests/conftest.py:158`; mutating `.value` on it is the established pattern. If the mutation leaks into other tests, restore the original value at the end of the test (read it into a variable first).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py -k "over_size_limit or invalid_json" -v`
Expected: 3 FAILED (the size tests get 200 instead of 413, the invalid JSON test gets 500 instead of 422).

- [ ] **Step 3: Implement the capped reader and wire it into both controllers**

In `src/aleph/web/controllers/storage.py`, add to the imports block:

```python
import json
from typing import Any, Optional
```

(`Optional` is already imported; merge rather than duplicate. Stdlib `json` is used deliberately to keep parse semantics identical to `request.json()`.)

Add the helper above `add_ipfs_json_controller`:

```python
async def _read_json_body_with_limit(request: web.Request, max_size: int) -> Any:
    """Read a JSON request body, aborting as soon as it exceeds max_size.

    Unlike request.json(), this does not buffer up to client_max_size
    (100 MiB) before checking; anonymous endpoints must reject at the
    unauthenticated upload limit (25 MiB by default).
    """
    content_length = request.content_length
    if content_length is not None and content_length > max_size:
        raise web.HTTPRequestEntityTooLarge(
            actual_size=content_length, max_size=max_size
        )

    buffer = bytearray()
    async for chunk in request.content.iter_chunked(8192):
        buffer.extend(chunk)
        if len(buffer) > max_size:
            raise web.HTTPRequestEntityTooLarge(
                actual_size=len(buffer), max_size=max_size
            )

    try:
        return json.loads(bytes(buffer))
    except json.JSONDecodeError:
        raise web.HTTPUnprocessableEntity(reason="Invalid JSON body")
```

In `add_ipfs_json_controller`, replace:

```python
    data = await request.json()
```

with:

```python
    max_size = config.storage.max_unauthenticated_upload_file_size.value
    data = await _read_json_body_with_limit(request, max_size)
```

Apply the identical replacement in `add_storage_json_controller` (both controllers already have `config` in scope).

- [ ] **Step 4: Run the new tests and the whole file**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py -v`
Expected: all PASS, including the 3 new tests and the existing `test_storage_add_json` / `test_ipfs_add_json` (proves parse semantics unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/storage.py tests/api/test_storage.py
git commit -m "fix(storage): enforce unauthenticated size limit on add_json endpoints"
```

---

### Task 3: Deprecation signal on all anonymous uploads

**Files:**
- Modify: `src/aleph/web/controllers/utils.py` (new helper)
- Modify: `src/aleph/web/controllers/storage.py` (`add_ipfs_json_controller`, `add_storage_json_controller`, `storage_add_file` response at ~line 499)
- Modify: `src/aleph/web/controllers/ipfs.py` (`ipfs_add_file` response at ~line 263)
- Test: `tests/api/test_storage.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/api/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_storage_add_json_deprecation_header(api_client, session_factory):
    response = await api_client.post(STORAGE_ADD_JSON_URI, json=JSON_CONTENT)
    assert response.status == 200, await response.text()
    assert response.headers.get("Deprecation") == "true"


@pytest.mark.asyncio
async def test_storage_add_file_anonymous_deprecation_header(api_client):
    form_data = aiohttp.FormData()
    form_data.add_field("file", FILE_CONTENT)
    response = await api_client.post(STORAGE_ADD_FILE_URI, data=form_data)
    assert response.status == 200, await response.text()
    assert response.headers.get("Deprecation") == "true"
```

(`JSON_CONTENT`, `FILE_CONTENT`, `STORAGE_ADD_FILE_URI` and the `aiohttp` import already exist in this file; mirror the existing `test_storage_add_file` posting pattern at ~line 260 if the form construction differs.)

Then extend the existing authenticated test `test_storage_add_file_with_message` (~line 303) with one assertion right after its status assertion:

```python
    assert "Deprecation" not in post_response.headers
```

(Use the actual response variable name in that test if it differs from `post_response`.)

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py -k "deprecation or with_message" -v`
Expected: the two new tests FAIL (no Deprecation header yet); `test_storage_add_file_with_message` PASSES (header correctly absent).

- [ ] **Step 3: Implement the helper and apply it to the four anonymous paths**

In `src/aleph/web/controllers/utils.py`, add near the top (module already imports `logging`, `web` and `Dict`):

```python
UPLOAD_DEPRECATION_HEADERS: Dict[str, str] = {"Deprecation": "true"}


def warn_deprecated_unauthenticated_upload(request: web.Request) -> Dict[str, str]:
    """Log and return deprecation headers for anonymous upload requests.

    Unauthenticated uploads are deprecated and will be removed; the log
    line lets node operators spot remaining anonymous traffic.
    """
    logging.getLogger(__name__).warning(
        "Deprecated unauthenticated upload on %s from %s. This path will be "
        "removed in a future release; uploads will require a signed message.",
        request.path,
        request.remote,
    )
    return dict(UPLOAD_DEPRECATION_HEADERS)
```

In `src/aleph/web/controllers/storage.py`, import it (extend the existing `from aleph.web.controllers.utils import (...)` block with `warn_deprecated_unauthenticated_upload`).

In `add_ipfs_json_controller` and `add_storage_json_controller`, change the final line of each from:

```python
    return web.json_response(output)
```

to:

```python
    return web.json_response(
        output, headers=warn_deprecated_unauthenticated_upload(request)
    )
```

In `storage_add_file` (~line 499), change:

```python
        output = {"status": "success", "hash": file_hash}
        return web.json_response(data=output, status=status_code)
```

to:

```python
        output = {"status": "success", "hash": file_hash}
        headers = (
            warn_deprecated_unauthenticated_upload(request) if message is None else None
        )
        return web.json_response(data=output, status=status_code, headers=headers)
```

In `src/aleph/web/controllers/ipfs.py`, import `warn_deprecated_unauthenticated_upload` from `aleph.web.controllers.utils` (an import block from that module already exists) and change the final response of `ipfs_add_file` (~line 263) from:

```python
        return web.json_response(
            data={
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            },
            status=status_code,
        )
```

to:

```python
        headers = (
            warn_deprecated_unauthenticated_upload(request) if metadata is None else None
        )
        return web.json_response(
            data={
                "status": "success",
                "hash": cid,
                "name": filename,
                "size": size,
            },
            status=status_code,
            headers=headers,
        )
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py tests/api/test_ipfs.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph/web/controllers/utils.py src/aleph/web/controllers/storage.py src/aleph/web/controllers/ipfs.py tests/api/test_storage.py
git commit -m "feat(storage): emit deprecation signal on unauthenticated uploads"
```

---

### Task 4: Shorten grace period and GC interval defaults

**Files:**
- Modify: `src/aleph/config.py:144-146`
- Modify: `src/aleph/web/controllers/ipfs.py` (stale "24 h grace period" comment at ~line 192)
- Create: `tests/test_config_defaults.py`

Note: `storage.grace_period` also drives post-FORGET file retention (`src/aleph/handlers/content/store.py:513`). The spec accepts this: a forgotten file is now reclaimable after 6h instead of 24h, which is consistent with the user having asked for deletion.

- [ ] **Step 1: Write the failing test**

Create `tests/test_config_defaults.py`:

```python
from aleph.config import get_defaults
from aleph.toolkit.constants import MiB


def test_upload_mitigation_defaults():
    defaults = get_defaults()
    assert defaults["storage"]["grace_period"] == 6
    assert defaults["storage"]["garbage_collector_period"] == 4
    assert defaults["storage"]["max_unauthenticated_upload_file_size"] == 25 * MiB
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/test_config_defaults.py -v`
Expected: FAIL (grace_period is 24, garbage_collector_period is 24).

- [ ] **Step 3: Change the defaults**

In `src/aleph/config.py`, change:

```python
            # Interval between garbage collector runs, expressed in hours.
            "garbage_collector_period": 24,
            # Grace period for files, expressed in hours.
            "grace_period": 24,
```

to:

```python
            # Interval between garbage collector runs, expressed in hours.
            "garbage_collector_period": 4,
            # Grace period for files, expressed in hours.
            "grace_period": 6,
```

In `src/aleph/web/controllers/ipfs.py`, update the comment that hardcodes the old duration (~line 192): replace `24 h grace period` with `grace period` in the comment text. Do not change any code on that path.

- [ ] **Step 4: Run the test and the GC tests**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/test_config_defaults.py tests/api/test_storage.py -v`
Also run any GC-specific tests if present: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests -k "garbage" -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/aleph/config.py src/aleph/web/controllers/ipfs.py tests/test_config_defaults.py
git commit -m "fix(storage): shorten grace period to 6h and GC interval to 4h"
```

---

### Task 5: Lint, full test pass, finish branch

**Files:**
- Possibly modified by formatters: the files touched above

- [ ] **Step 1: Format and lint**

```bash
/home/olivier/git/aleph/pyaleph/venv/bin/black src/aleph/web/controllers src/aleph/config.py tests/api/test_storage.py tests/test_config_defaults.py
/home/olivier/git/aleph/pyaleph/venv/bin/isort src/aleph/web/controllers src/aleph/config.py tests/api/test_storage.py tests/test_config_defaults.py
```

(mypy is known to not run cleanly locally due to a sqlalchemy stubs conflict; do not block on it.)

- [ ] **Step 2: Run the affected test files in full**

Run: `PYTHONPATH=src /home/olivier/git/aleph/pyaleph/venv/bin/python -m pytest tests/api/test_storage.py tests/api/test_ipfs.py tests/test_config_defaults.py -v`
Expected: all PASS.

- [ ] **Step 3: Commit any formatting changes**

```bash
git add -u
git commit -m "style: apply black and isort" || echo "nothing to format"
```

- [ ] **Step 4: Unstage docs (project convention: spec/plan files must not be in the PR tree)**

```bash
git rm -r docs/superpowers
git commit -m "docs: unstage superpowers docs before PR"
```

- [ ] **Step 5: Verify and hand off**

Run the full affected suite once more, then use the superpowers:finishing-a-development-branch skill (options: PR against main, etc.). PR description must mention: 25 MiB cap on add_json (was accidentally ~100 MiB), grace period 6h, GC 4h, deprecation headers, and link the phase 2 intent (signed-message-mandatory uploads) as the announced follow-up. No Co-Authored-By trailer, no em dashes.

---

## Explicitly out of scope (do not implement)

- Multipart `POST /api/v0/messages` (phase 2, separate plan when SDKs are ready)
- Deleting `add_json` or any anonymous path (phase 2)
- Making upload `metadata` mandatory (phase 2)
- Any billing/cost logic for POST/AGGREGATE (workstream 2)
- Rate limiting
