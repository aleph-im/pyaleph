# Design: `contentFormat` query parameter for message read endpoints

Date: 2026-05-26
Status: Approved (pre-implementation)

## Problem

`GET /api/v0/messages.json` returns full message `content` by default. For some
message types that payload is large (POST/AGGREGATE nested `content`,
PROGRAM/INSTANCE config), and list responses routinely reach ~10 MB. The existing
`excludeContent=true` flag drops `content` entirely, which removes the lightweight
metadata an explorer needs to render a list (message subtype, target ref,
aggregate key, stored item hash). Clients are then forced to fetch full content
just to obtain a few header fields.

`excludeContent` is intentionally narrow: it exists so integrity-checking clients
can skip a `content` we cannot faithfully re-serialize. Overloading it to carry
selected fields is the wrong semantics. We want a distinct "headers" mode that
keeps a minimal, type-specific field set and strips the heavy remainder.

## Goal

Add a `contentFormat` query parameter exposing three mutually exclusive content
levels, with a reduced `headers` level that returns only curated metadata per
message type. The reduced level must be cheap on the server.

## Key insight: metadata is already denormalized

The metadata fields of interest already exist as real, indexed columns on the
`messages` table (the `od/denormalize-messages-table` work, merged to `main`):

| Reduced field    | Backing column        |
| ---------------- | --------------------- |
| `content.address`| `owner`               |
| `content.type`   | `content_type`        |
| `content.ref`    | `content_ref`         |
| `content.key`    | `content_key`         |
| `content.item_hash` | `content_item_hash`|

These columns are loaded by default and require no JSONB access. Therefore
`headers` mode can `defer(MessageDb.content)` entirely (skipping even the
Postgres TOAST read of the `content` JSONB) and rebuild a small `content` dict
purely from columns. Server cost for `headers` is identical to `none`
(`excludeContent`), while still returning useful metadata.

`message_to_dict` already strips `DENORMALIZED_COLUMNS` from API responses (to
avoid breaking SDKs); that behaviour is unchanged. `headers` mode reads those
column values internally to assemble the reduced `content`, but the columns
themselves are still not emitted as top-level keys.

## API

### Enum

```python
class ContentFormat(str, Enum):
    FULL = "full"
    HEADERS = "headers"
    NONE = "none"
```

Location: `aleph/types/content_format.py`, imported by the query-params schema.

### Query parameter

On `BaseMessageQueryParams`:

- `content_format: Optional[ContentFormat] = Field(default=None, alias="contentFormat")`
- `exclude_content: bool` is retained, its description marked **deprecated**.

### Precedence resolution

A `model_validator(mode="after")` collapses the two inputs into one concrete
`content_format`, so all downstream code reads a single field:

1. If `contentFormat` was supplied explicitly, it wins.
2. Else if `excludeContent=true`, resolve to `NONE`.
3. Else resolve to `FULL`.

After validation, `content_format` is never `None`.

### Endpoints

- `GET /api/v0/messages.json` (both cursor mode and legacy page mode).

Out of scope:

- The single-message endpoint `GET /api/v0/messages/{hash}` (`view_message` →
  `_get_message_with_status` → `format_message`). It serializes through strict
  pydantic models (`ProcessedMessageStatus` wrapping a validated `AlephMessage`
  whose content models use `extra="forbid"`), shares no code with the list
  endpoint's `message_to_dict` path, and does not support `excludeContent` today.
  A reduced `content` dict would not validate against those models. A single
  fetch is also never the large-payload problem. Supporting it would require a
  separate serialization path / response model; deferred until there is demand.
- The websocket broadcast path keeps `full` / `none` only. It uses an in-memory
  pydantic message with no DB projection, so `headers` would require a separate
  model-stripping implementation; deferred until there is demand.

## Reduced content field map (`headers`)

All values sourced from columns; the `content` JSONB is deferred (never read).

| Message type            | `content` keys returned        | Source columns                       |
| ----------------------- | ------------------------------ | ------------------------------------ |
| all                     | `address`                      | `owner`                              |
| POST                    | + `type`, `ref`                | `content_type`, `content_ref`        |
| AGGREGATE               | + `key`                        | `content_key`                        |
| STORE                   | + `item_hash`, `ref`           | `content_item_hash`, `content_ref`   |
| PROGRAM / INSTANCE      | `address` only                 | —                                    |
| FORGET                  | `address` only                 | —                                    |

Notes:
- `content.time` is intentionally omitted. The response already carries a
  top-level `time` (= `message.time`), which is authoritative for explorer
  display. `content.time` has no denormalized column; reading it would force a
  TOAST read and defeat the optimization.
- Keys whose column value is `NULL` are omitted from the reduced `content`
  (e.g. a STORE with no `ref` simply has no `ref` key).

## Query layer

`content_format` is a field on the shared `BaseMessageQueryParams`, so every
site that builds `find_filters = query_params.model_dump(exclude_none=True)`
must `pop("content_format", ...)` before forwarding to
`make_matching_messages_query` (exactly as `exclude_content` is popped today),
otherwise it would be passed as an unexpected filter kwarg. Affected sites:
list cursor mode, list legacy page mode, and `_send_history_to_ws`.

Replace the current `exclude_content` branch at the three query-construction
sites with a `content_format` switch:

- `FULL`: load `content` (current default behaviour).
- `NONE`: `messages_query.options(defer(MessageDb.content))` (today's
  `excludeContent` behaviour).
- `HEADERS`: also `defer(MessageDb.content)`. No additional projection is
  needed because the denormalized columns are loaded by default. The reduced
  `content` is assembled in Python.

## Response building

`message_to_dict` takes a `content_format: ContentFormat` argument in place of
the `exclude_content: bool`:

- `FULL`: `message.to_dict()` (current path).
- `NONE`: `message.to_dict(exclude={"content"})` (current path).
- `HEADERS`: `message.to_dict(exclude={"content"})`, then set
  `message_dict["content"] = build_headers_content(message)`.

`build_headers_content(message: MessageDb) -> Dict[str, Any]` keys off
`message.type` and reads the denormalized column values to produce the reduced
dict per the field map above. `DENORMALIZED_COLUMNS` stripping, `time`
conversion, and confirmations handling are unchanged.

### WebSocket handling

The websocket path supports two states only (content present / stripped). Both
`none` and `headers` map to stripped there, `full` maps to present:

- `_send_history_to_ws`: treat `content_format != FULL` as the current
  `exclude_content=True` behaviour (`defer` + `message_to_dict` without
  content); `FULL` as today's default.
- Live broadcast: `_WsClient` stores `exclude_content = (content_format != FULL)`
  and the existing `json_full` / `json_no_content` split is unchanged.

This keeps WS behaviour backward compatible (an `excludeContent=true` client is
unaffected) without implementing a third WS variant.

## Backward compatibility

- `excludeContent` remains accepted; documented as deprecated. With no
  `contentFormat`, `excludeContent=true` continues to behave exactly as today
  (resolves to `none`).
- Default behaviour (no params) is unchanged (`full`).

## No migration

The required columns already exist on the `messages` table. No schema change or
backfill.

## Known limitations

- For the rare object-valued `key` (`AggregateContentKey`) or `ref` (`ChainRef`),
  `headers` returns the denormalized string column value, which may be a coerced
  or partial representation of the original nested structure. This is
  pre-existing denormalization behaviour, not introduced here. Consumers needing
  the exact nested structure must use `full`.
- `headers` mode reduces wire payload, response serialization cost, and egress,
  and (thanks to the deferred JSONB) also avoids the TOAST read. It is therefore
  a payload AND server-load win versus `full`, and equivalent in server cost to
  `none`. It is NOT a substitute for `none` when the client genuinely wants no
  content at all.

## Testing

- Query-param parsing and precedence:
  - `contentFormat=full|headers|none` parse correctly.
  - `excludeContent=true` with no `contentFormat` resolves to `none`.
  - explicit `contentFormat` overrides `excludeContent`.
- Reduced content correctness per message type (POST, AGGREGATE, STORE,
  PROGRAM, INSTANCE, FORGET) against the field map.
- `headers` omits `NULL`-valued optional keys (e.g. STORE without `ref`).
- `content` JSONB is not loaded in `headers`/`none` modes (defer applied).
- OpenAPI docstring for the endpoint documents `contentFormat` and the
  deprecation of `excludeContent`.

## Documentation

- Update the endpoint docstring / OpenAPI parameter block in
  `src/aleph/web/controllers/messages.py` to add `contentFormat` and mark
  `excludeContent` deprecated.
