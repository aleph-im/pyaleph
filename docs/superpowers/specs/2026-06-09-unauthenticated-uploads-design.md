# Closing the unauthenticated upload surface

Date: 2026-06-09
Status: design validated in brainstorming session, pending final review
Workstream: 1 of 3 (next: POST/AGGREGATE message billing; later: holder tier decommissioning)

## Problem

Anyone can push bytes into a CCN with no identity and no payment. Verified ingress surface on main today:

| Endpoint | Auth | Size limit | Outcome |
|---|---|---|---|
| `POST /api/v0/storage/add_file` (no metadata) | none | 25 MiB | 24h grace pin |
| `POST /api/v0/storage/add_json` | none | ~100 MiB (only bounded by aiohttp `client_max_size`) | 24h grace pin |
| `POST /api/v0/ipfs/add_json` | none | ~100 MiB (same accident) | 24h grace pin + IPFS pin |
| `POST /api/v0/ipfs/add_file` (no metadata) | none | 25 MiB | 24h grace pin + IPFS pin |
| `add_file` with metadata | signed STORE + balance | 100 MiB / 1 GiB | permanent pin on message |
| `POST /api/v0/ipfs/add_car` | signed STORE + balance (mandatory) | 4 GiB | permanent pin on message |

Key code references:

- `src/aleph/web/controllers/storage.py:379` (`storage_add_file`), `:62` and `:105` (the two `add_json` controllers, no auth, no explicit size check)
- `src/aleph/web/__init__.py:40` (`client_max_size=max_file_size`, the accidental 100 MiB bound)
- `src/aleph/web/controllers/ipfs.py:37` (`ipfs_add_file`)
- `src/aleph/web/controllers/utils.py:409` (`add_grace_period_for_file`)
- `src/aleph/services/storage/garbage_collector.py` (GC, runs every `storage.garbage_collector_period` hours, wired in `commands.py:239`)
- `src/aleph/config.py:143-150` (grace period 24h, GC period 24h, size limits)

The grace period plus GC cadence means anonymous bytes can live up to ~48h, renewable forever. An attacker looping uploads holds a sustained (upload rate x lifetime) of disk, bandwidth and IPFS daemon load hostage, at zero cost and with zero attribution.

## Why the anonymous path exists

The SDK flow for non-inline messages is two-step: upload the content anonymously (`add_json` or `add_file`), then submit the signed message whose `item_hash` references it. The grace pin bridges the gap until message processing creates the permanent `ContentFilePinDb` pin. Closing the anonymous path therefore requires a replacement submission flow, not just deletion.

## Semantics: two distinct flows, never to be conflated

This distinction drives the whole design and must be preserved in code and docs:

1. **Companion file (STORE only).** The uploaded bytes are a file that the message *references* via `content.item_hash` (a field inside the message content). The message itself is complete without the file. The upload endpoints (`add_file`, `add_car`) exist solely to ship file + STORE message in one shot and avoid the chicken-and-egg problem.
2. **Message-content completion (any type with `item_type=storage|ipfs`).** The missing bytes are the message's own content; `item_hash` at the *envelope* level is the hash of those bytes. The message is incomplete without them. Semantically this belongs to message submission, not to a file-upload API.

The two hashes live at different levels of the message and the validation logic is never shared between flows.

## Decisions taken (with rationale)

- **Keep inline messages at 200 KB and keep non-inline messages in the protocol.** An inline-only protocol was evaluated and rejected: the aleph p2p-service hardcodes gossipsub `max_transmit_size` to 256 KiB (`p2p-service` repo, `src/p2p/network.rs:81`, rust-libp2p 0.51.3), and the IPFS pubsub redundancy channel is hard-capped at 1 MiB by go-libp2p-pubsub (`DefaultMaxMessageSize`, not exposed in Kubo config at all). Announce-on-pubsub plus fetch-out-of-band is wire-level best practice; the flaw to fix is the unauthenticated ingress, not the indirection.
- **Two phases.** Mitigate immediately with config-level tightening (no client breakage), then make authenticated submission mandatory after a deprecation window coordinated with SDK releases.
- **Anonymous upload path removed entirely in phase 2** (not config-gated, not size-reduced).
- **Authenticated limits stay as they are** (100 MiB storage, 1 GiB IPFS, 4 GiB CAR). The 25 MiB unauthenticated tier is a legacy artifact that disappears with the anonymous path.

## Phase 1: immediate mitigation (no client breakage)

1. **Explicit size cap on both `add_json` endpoints.** Stream-read the body and reject with 413 past `storage.max_unauthenticated_upload_file_size` (25 MiB), instead of buffering up to the accidental 100 MiB `client_max_size`. Aligns all four anonymous paths at the documented 25 MiB.
2. **`storage.grace_period` default: 24h to 6h.** The legitimate upload-to-message gap is seconds; 6h is generous.
3. **`storage.garbage_collector_period` default: 24h to 4h.** Without this a 6h grace pin still survives up to ~30h because GC sweeps daily. Worst-case anonymous byte lifetime drops from ~48h to ~10h.
4. **Deprecation signal.** Anonymous uploads keep working but receive a `Deprecation` response header and a server log warning announcing phase 2.

Deliverable: one small PR (two config defaults, one streaming size check, one header). Release notes announce the phase 2 timeline.

## Phase 2: authenticated submission everywhere

### Flow 1: non-inline message submission via `POST /api/v0/messages`

The endpoint gains a `multipart/form-data` variant for message-content completion:

- Part `message`: the signed message JSON (same schema as the current JSON body).
- Part `content`: the exact serialized content bytes whose hash is the message's `item_hash`.

Order of operations, strictly:

1. Read the `message` part and verify the signature (covers `item_hash`, so a forged request dies before the node reads the body).
2. Run the balance/cost gate. This is the enforcement point where workstream 2 (message billing) plugs in; until billing lands it provides attribution only.
3. Stream the `content` part (cap: `storage.max_file_size`, 100 MiB) and verify integrity: `sha256(bytes) == item_hash` for `item_type=storage`; add to IPFS and verify the resulting CID equals `item_hash` for `item_type=ipfs`.
4. Store the content in the corresponding engine. Create a short grace pin (6h) to bridge the gap until message processing creates the permanent content pin (protects against a GC sweep racing the pending pipeline).
5. Broadcast and process the message as today. Only the message JSON propagates on both pubsubs; content never rides pubsub and is fetched on demand by peers, exactly as today.

Behavior of the existing `application/json` body:

- Inline messages: unchanged, forever.
- Non-inline messages without content: accepted with a `Deprecation` header during the window (node fetches content as today). After the window: rejected with 422 immediately. Rationale: once anonymous uploads are gone there is nothing for the node to fetch for new content, so failing fast replaces failing slowly via fetch timeout.
- Messages arriving via P2P sync are untouched: the origin node has the content (its API stored it at submission) and peers fetch it as today.

Edge cases:

- `item_type=ipfs` submitted to a node with IPFS disabled: 422 with a message suggesting the storage engine.
- Content part present on an inline message: 422.

### Flow 2: companion file uploads, narrowed to their real purpose

`storage/add_file`, `ipfs/add_file`, `ipfs/add_car` keep their existing design (signed STORE message in `metadata`, balance check before ingestion, the `add_car` pattern of reading metadata before the file part) with one change: `metadata` becomes mandatory. Anonymous branches are deleted.

### Deletions

- Both `add_json` endpoints (their job moves to Flow 1).
- All anonymous upload code paths and the `max_unauthenticated_upload_file_size` config keys (storage and ipfs).
- Upload-time grace pins for anonymous uploads. `GracePeriodFilePinDb` and the GC remain: they still serve post-FORGET retention (`store.py:_check_remaining_pins`) and the Flow 1 submission bridge pin.

## Error handling

| Condition | Status |
|---|---|
| Missing/invalid `metadata` (Flow 2) or missing `content` part for non-inline (Flow 1, post-window) | 422 |
| Hash/CID mismatch between content and `item_hash` | 422 |
| Content part on an inline message | 422 |
| `item_type=ipfs` with IPFS disabled | 422 |
| Invalid signature | 403 |
| Insufficient balance (existing behavior, extended by workstream 2) | 402 |
| Oversize body or part | 413 |

All error reasons name the migration path explicitly.

## Rollout

1. Phase 1 ships in the next CCN release. Operators inherit new defaults unless overridden.
2. SDK releases implement Flow 1 (one-step multipart submission) and stop calling `add_json`.
3. Deprecation window (suggested 2 to 3 months, final duration is an ecosystem comms decision).
4. Phase 2 CCN release removes the anonymous paths and the JSON-without-content variant for non-inline messages.

No network cutoff timestamp is needed: everything here is node-local API behavior, not message validity, so consensus is unaffected and history replays identically.

## Hand-off requirements to workstream 2 (message billing)

- The Flow 1 step 2 gate is the designated cost enforcement point for non-inline POST/AGGREGATE content; billing by message size covers the content footprint (content pin included).
- A signed message from a broke address still lands in `pending_messages` before any balance logic when it arrives via pubsub (inline, up to 200 KB). Workstream 2 should add a cheap pre-insert balance check to bound this.
- Message-list API endpoints returning large `content` values is a known pagination concern, out of scope for both workstreams, recorded here so it is not lost.

## Testing

Phase 1:

- `add_json` (both variants) rejects >25 MiB with 413 and accepts under the cap.
- New config defaults (grace 6h, GC 4h) take effect; overrides respected.
- Deprecation header present on all anonymous upload responses.

Phase 2:

- Flow 1 multipart: happy path for POST, AGGREGATE and STORE with non-inline content document, for both engines (storage hash check, IPFS CID check); content lands in the right engine; bridge grace pin created; permanent content pin appears after processing; end-to-end POST reaches the `posts` table.
- Flow 1 rejections: bad signature (403), hash mismatch (422), oversize content part (413), ipfs-disabled (422), content part on inline message (422), non-inline JSON without content post-window (422).
- Flow 2: uploads without metadata rejected (422); with valid metadata unchanged behavior.
- Deleted endpoints return 404.
- Signature verified before body consumption (metadata/message part read first).

## Open items

- Verify whether `aleph-message` (library) needs changes for SDKs to construct Flow 1 requests (likely SDK-only, the message schema itself is unchanged).
- SDK (Python and TypeScript) implementation plan and release coordination.
- Ecosystem comms plan and final deprecation window duration.
