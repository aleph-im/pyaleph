# Authenticated IPFS Uploads — Design

**Status:** Draft
**Date:** 2026-04-23
**Target endpoint:** `POST /api/v0/ipfs/add_file`

## Summary

Add authenticated upload support to `/api/v0/ipfs/add_file` so users can upload a file and submit its companion STORE message in a single multipart request, mirroring `/api/v0/storage/add_file`. Also tighten the existing unauthenticated mode with an explicit size limit, and rationalize the pinning rules for the new path.

## Background — current state

Four paths can put content on a node today. Their rules differ in ways that are not driven by a single principle:

| Path | Auth | Size limit | Pinned to IPFS? | Grace period |
|---|---|---|---|---|
| `POST /ipfs/add_file` | none | aiohttp default (~100 MiB), not explicit | always, immediately | 24 h |
| `POST /storage/add_file` (raw) | none | 25 MiB | no (local engine) | 24 h |
| `POST /storage/add_file` + message | signed STORE (`item_type=storage`) | 100 MiB | no (local engine) | none (message anchors) |
| `POST /messages` STORE `item_type=ipfs` | signed STORE | N/A (CID referenced) | sync during processing, **only if ≥ 1 MiB** | balance-derived |
| `POST /messages` STORE `item_type=storage` | signed STORE | N/A | no | balance-derived |

Notable quirks:

1. `/ipfs/add_file` is open to the internet: anyone can pin arbitrary content for 24 h, no auth, no balance check.
2. The < 1 MiB branch in `/messages` STORE processing silently substitutes "fetch from aleph network, store in local engine" for IPFS pinning. It exists to avoid hammering the IPFS daemon with many small pins. It breaks the mental model of "item_type=ipfs ⇒ file is pinned on the node."
3. `/ipfs/add_file` has no explicit server-side size limit — only whatever aiohttp enforces by default.

This spec addresses the upload-authentication gap and the `/ipfs/add_file`-specific quirks. It explicitly does not change the `/messages` STORE processing behavior (see Out of Scope).

## Goals

- Allow a signed STORE message + file to be uploaded in a single request to `/api/v0/ipfs/add_file`.
- Define a rational pinning rule for the new path: content uploaded via multipart is pinned at upload time, always.
- Add an explicit size limit to the unauthenticated mode (close the "no explicit cap" gap).
- Keep the unauthenticated mode working — same shape, same semantics, one new config-driven limit.

## Non-goals (out of scope)

- **Not changing** the < 1 MiB / "fetch from network instead of pin" branch in `/messages` STORE-message processing. The operational concern (daemon overload) is real; a proper fix (async pin queue, rate-limited batching, or similar) deserves its own design.
- **Not changing** the unauthenticated `/ipfs/add_file` write permission model (still open to the internet). To be revisited.
- **Not introducing** streaming uploads. The file is still read into memory before pinning. Acceptable at 100 MiB; revisit if the cap is raised.
- **Not changing** `/storage/add_file` behavior.

## Design

### Endpoint contract

`POST /api/v0/ipfs/add_file` accepts a multipart/form-data body:

```
file:     <binary>                                      # required
metadata: {"message": <PendingInlineStoreMessage>,      # optional
           "sync": <bool>}
```

**Unauthenticated mode** (no `metadata` field): behavior matches today — pin the file, upsert the `files` row, add a 24 h grace period, return `{status, hash, name, size}`. The only change is an explicit size check (see Size limits).

**Authenticated mode** (`metadata` provided):

- `message.content.item_type` MUST be `"ipfs"` (reject 422 otherwise).
- `message.content.item_hash` MUST equal the CID computed by the local IPFS daemon after pinning (reject 422 on mismatch).
- Signature and content parsing are validated **before** the file is streamed to IPFS (cheap, fail-fast).
- Balance preflight fires only when `uploaded_size > ipfs.max_unauthenticated_upload_file_size`, mirroring the storage endpoint's rule.
- On full success: the STORE message is inserted into the pending queue via the same `broadcast_and_process_message` path `/storage/add_file` uses, honoring the `sync` flag.
- Response adds the `broadcast_status` field on top of the existing shape.

### Size limits

Two new config values:

| Config key | Default |
|---|---|
| `ipfs.max_upload_file_size` | 100 MiB |
| `ipfs.max_unauthenticated_upload_file_size` | 25 MiB |

Defaults match the equivalent `storage.*` values so nothing changes for existing deployments. They exist as separate keys so ops can tune IPFS limits independently — IPFS is plausibly the first path bumped for large-content use cases.

The unauthenticated path enforces `max_unauthenticated_upload_file_size` explicitly via `UploadedFile.read_and_validate()` (the same mechanism storage uses). Today it only has the aiohttp default.

### Pinning rules

This spec changes the pinning rule for **one** path:

| Path | Pins? | Change |
|---|---|---|
| `/ipfs/add_file` (authenticated) | yes, at upload | **new path, new rule** |
| `/ipfs/add_file` (unauthenticated) | yes, at upload | unchanged |
| `/storage/add_file` | no | unchanged |
| `/messages` STORE `item_type=ipfs`, ≥ 1 MiB | yes, during processing | unchanged |
| `/messages` STORE `item_type=ipfs`, < 1 MiB | no (fetch from network) | unchanged — known quirk, deferred |
| `/messages` STORE `item_type=storage` | no | unchanged |

Justification for eager pinning on the authenticated path: we already have the bytes, `ipfs add` pins as a side effect, and a paying STORE message bounds the abuse surface. Message processing becomes a no-op for the pin (`pin_add` is idempotent).

### Request lifecycle (authenticated path)

```
1. Parse multipart
   - Read metadata part first (small, fast).
   - Parse as StorageMetadata. Reject 422 if malformed.

2. Pre-flight message validation (no file read yet)
   - Verify signature via SignatureVerifier. Reject 403 on InvalidSignature.
   - Parse message.item_content as CostEstimationStoreContent.
   - Require item_type == "ipfs". Reject 422 otherwise.

3. Read file part
   - MultipartUploadedFile with max_size = ipfs.max_upload_file_size.
   - read_and_validate() enforces size; raises 413 on overflow.

4. Pin to IPFS
   - ipfs_service.add_bytes(file_content) pins as a side effect,
     returns the CID.
   - Wrapped with asyncio.wait_for(ipfs.stat_timeout).

5. Post-pin validation
   - content.estimated_size_mib = ceil(uploaded_size / MiB).
   - Require message.item_hash == computed_cid. Reject 422 on mismatch.
   - If uploaded_size > max_unauthenticated_upload_file_size:
       _verify_user_balance(). Reject 402 on shortfall.

6. Persist
   - Single DB transaction:
       upsert_file(file_hash=cid, size, file_type=FILE)
       — no grace period added; message anchors the pin.
   - broadcast_and_process_message(message, sync).

7. Error cleanup (any failure after step 4)
   - Leave the pin in place.
   - add_grace_period_for_file(cid, hours=24).
   - Return the appropriate error.
```

**Invariant:** once step 4 succeeds, the pin always ends up with a well-defined lifetime — either anchored by the STORE message (success path) or covered by the 24 h grace (any later failure). No orphaned pins.

Unauthenticated path stays as today (read → `add_bytes` → `upsert_file` → grace → return), plus the explicit size-limit check at step 3.

### Error model (authenticated path)

Checked in order:

| Code | Trigger | Side effect |
|---|---|---|
| 400 | Multipart malformed, missing `file`, wrong content-type on file part | None |
| 422 | `metadata` JSON unparseable, or `item_content` unparseable | None |
| 403 | Message signature invalid | None |
| 422 | `item_type != "ipfs"` | None |
| 413 | File exceeds `ipfs.max_upload_file_size` | None (aborted mid-read) |
| 408 / 504 | `ipfs_service.add_bytes` exceeds `ipfs.stat_timeout` | None (no pin) |
| 502 | IPFS daemon unreachable or errors during add | None (no pin) |
| 422 | CID mismatch: `computed_cid != message.item_hash` | Pin stays, 24 h grace added |
| 402 | Balance shortfall | Pin stays, 24 h grace added |
| 500 | DB insert failure, broadcast failure | Pin stays, 24 h grace added |

Unauthenticated path is a strict subset (no 403, no 402, no message-related 422s).

Errors that leave a grace-period pin behind log a `WARNING` with the CID for traceability. IPFS daemon errors (502/504) log `ERROR`.

### Code organization

Logic shared with `/storage/add_file` lives in `storage.py` today (`_check_and_add_file`, `_verify_message_signature`, `_verify_user_balance`). The two controllers diverge on how the file is ingested and hashed (SHA256 via local engine write for storage, CID via `ipfs_service.add_bytes` for IPFS), but converge afterwards.

The shared part — signature verification, `item_type` check, hash-match check, balance check, `upsert_file`, grace-period-on-error bookkeeping, `broadcast_and_process_message` — is extracted into a helper that the caller invokes **after** it has produced `(file_hash, uploaded_size)` from whichever backend. The helper takes `expected_item_type` as a parameter so it can reject mismatches. The file-ingest step and the exact form of "apply grace period on error" stay in each controller (since those are the backend-specific parts).

No new module; the helper stays in `storage.py` and `ipfs.py` imports it.

## Testing

Target file: `tests/api/test_ipfs.py` (extend existing).

**Unauthenticated (regression):**
- Happy path: upload succeeds, returns CID + size, file row has 24 h grace.
- Size overflow: upload exceeding `max_unauthenticated_upload_file_size` returns 413 (new — today there's no explicit cap).

**Authenticated happy path:**
- Valid signed STORE message, `item_type=ipfs`, hash matches: returns 200, file row exists with no grace, message enters pending queue.
- `sync=true`: response reflects processed status.
- File between 25 MiB and 100 MiB with sufficient balance: succeeds, balance check fired.
- File under 25 MiB: succeeds, balance check skipped (asserted via spy).

**Authenticated error paths** — each asserts status code + that pin has 24 h grace where applicable:
- Bad signature → 403, no pin.
- `item_type=storage` in message → 422, no pin.
- CID mismatch → 422, pin has 24 h grace.
- Insufficient balance on > 25 MiB upload → 402, pin has 24 h grace.
- Malformed metadata JSON → 422, no pin.
- File exceeds 100 MiB cap → 413, no pin.

**Shared helper** (if extracted): unit test calls it with `item_type=ipfs` and `item_type=storage` and asserts the right branch runs.

**Mocking:** real IPFS daemon unavailable in CI — use the existing `MockIpfsService` pattern already in `test_ipfs.py`. Balance and signature verifier have existing fixtures.

**Not covered here:** the unchanged `/messages` STORE `item_type=ipfs` flow (already covered by existing tests); daemon-outage scenarios beyond what's mockable.

## Open questions / deferred work

These are explicitly out of scope for this spec but worth tracking as follow-ups:

- The unauthenticated `/ipfs/add_file` write permission model — anyone on the internet can still pin content for 24 h. No change here; revisit with a proper abuse-mitigation story (rate limits, per-IP grace quotas, etc.).
- The < 1 MiB / "fetch from network" quirk in `/messages` STORE processing. Proper fix likely involves an async pin queue so small pins don't hit the daemon synchronously.
- Streaming uploads, needed only if the 100 MiB cap is raised significantly.
