# Permissions API Design

## Overview

Two new API endpoints for looking up authorization grants:
- **Granted**: permissions you delegated to others (forward lookup of security aggregate)
- **Received**: permissions others delegated to you (reverse lookup via GIN-indexed JSONB query)

Backed by the existing `aggregates` table with a new partial GIN index for efficient reverse lookups.

## Context

The security aggregate (`key="security"`) stores authorization delegations per address. Its `content.authorizations[]` array lists delegated addresses and their permission filters (chain, channels, types, post_types, aggregate_keys).

Currently, only forward lookup is possible: fetch a specific address's security aggregate. There is no efficient way to answer "who has granted permissions to address X?" without scanning all security aggregates.

## Endpoints

### `GET /api/v0/authorizations/granted/{address}.json`

Permissions the address gave to others. Thin wrapper over the existing security aggregate lookup.

**Query parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `grantee` | string | Filter by specific grantee address |
| `channels` | string (comma-separated) | Filter by channel name(s) |
| `types` | string (comma-separated) | Filter by message type (POST, STORE, etc.) |
| `postTypes` | string (comma-separated) | Filter by post type |
| `chains` | string (comma-separated) | Filter by chain (ETH, SOL, etc.) |
| `aggregateKeys` | string (comma-separated) | Filter by aggregate key(s) |
| `page` | int | Page number (default: 1) |
| `pagination` | int | Results per page (default: 20, max: 500) |

**Response:**

```json
{
  "authorizations": {
    "0xGranteeA": [
      {"types": ["POST"], "channels": ["chan1"]},
      {"types": ["STORE"]}
    ],
    "0xGranteeB": [
      {"types": ["POST", "STORE"]}
    ]
  },
  "pagination_page": 1,
  "pagination_per_page": 20,
  "pagination_total": 2,
  "pagination_item": "authorizations",
  "address": "0xOwnerAddress"
}
```

### `GET /api/v0/authorizations/received/{address}.json`

Permissions given to the address by others. Reverse lookup via GIN-indexed JSONB containment query.

**Query parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `granter` | string | Filter by specific granter address |
| `channels` | string (comma-separated) | Filter by channel name(s) |
| `types` | string (comma-separated) | Filter by message type (POST, STORE, etc.) |
| `postTypes` | string (comma-separated) | Filter by post type |
| `chains` | string (comma-separated) | Filter by chain (ETH, SOL, etc.) |
| `aggregateKeys` | string (comma-separated) | Filter by aggregate key(s) |
| `page` | int | Page number (default: 1) |
| `pagination` | int | Results per page (default: 20, max: 500) |

**Response:**

```json
{
  "authorizations": {
    "0xGranterA": [
      {"types": ["POST"], "channels": ["chan1"]}
    ],
    "0xGranterB": [
      {"types": ["POST", "STORE"]}
    ]
  },
  "pagination_page": 1,
  "pagination_per_page": 20,
  "pagination_total": 2,
  "pagination_item": "authorizations",
  "address": "0xTargetAddress"
}
```

## Pagination

Pagination is over **addresses** (granters or grantees), not individual authorization entries. A granter with 3 authorization entries for the same grantee counts as 1 pagination item. This matches the grouped-by-address response shape.

## Filtering

Filters are applied post-query, pre-pagination on the authorization entries. The full dataset is fetched, filtered in Python, then paginated. `pagination_total` reflects the count **after** filtering.

- `channels` — keep entries where the entry's `channels` array intersects with the filter values
- `types` — keep entries where the entry's `types` array intersects with the filter values
- `postTypes` — keep entries where the entry's `post_types` array intersects with the filter values
- `chains` — keep entries where the entry's `chain` string (singular) matches any of the filter values. Note: unlike other array fields, each authorization entry stores `chain` as a single string, not an array.
- `aggregateKeys` — keep entries where the entry's `aggregate_keys` array intersects with the filter values
- `granter`/`grantee` — filter on the outer grouping address

If all entries for a granter/grantee are filtered out, that address is excluded from the response and the pagination count.

## Database Changes

### New partial GIN index (Alembic migration)

```sql
CREATE INDEX ix_aggregates_security_authorizations
ON aggregates
USING GIN ((content -> 'authorizations') jsonb_path_ops)
WHERE key = 'security';
```

The partial index (`WHERE key = 'security'`) keeps it small. `jsonb_path_ops` is optimized for containment queries (`@>`), which is what the reverse lookup needs.

No new tables or columns are required.

## Reverse Lookup Query

```sql
SELECT owner, content->'authorizations'
FROM aggregates
WHERE key = 'security'
AND content->'authorizations' @> '[{"address": :target_address}]';
```

This uses the GIN index to efficiently find all security aggregates containing an authorization entry for a given address.

## Authorization Entry Structure

For reference, each entry in `content.authorizations[]` has this shape (all fields except `address` are optional):

```json
{
  "address": "0xGrantee",
  "chain": "ETH",
  "channels": ["chan1"],
  "types": ["POST", "STORE"],
  "post_types": ["amend"],
  "aggregate_keys": ["key1"]
}
```

The response groups these entries by the counter-party address (grantee for the "granted" endpoint, granter/owner for the "received" endpoint). The `address` field is used as the grouping key and is omitted from the entries in the response since it would be redundant.

## Dirty Aggregates

The existing aggregates API refreshes dirty aggregates before serving data. Both endpoints must do the same:

- **Granted endpoint**: refresh the target address's security aggregate if dirty (single aggregate, same as existing pattern).
- **Received endpoint**: the reverse lookup may hit dirty aggregates from other owners. Rather than refreshing all dirty security aggregates (potentially expensive), we accept slightly stale data here. This is an acceptable trade-off: the dirty flag is transient and aggregates are refreshed on a regular basis. If exact freshness is required, the caller can use the granted endpoint from the granter's perspective.

## Error Handling

- **200** with empty `authorizations: {}` — address has no security aggregate, no authorizations, or all entries filtered out.
- **422** — invalid query parameters (bad pagination values, malformed filter params).

We return 200 with empty data rather than 404 because the absence of authorizations is a valid, informative response (the address exists on-chain but has no grants).

## New Files

- `src/aleph/web/controllers/authorizations.py` — endpoint handlers
- `src/aleph/db/accessors/authorizations.py` — DB query functions (forward + reverse lookup)
- Route registration in `src/aleph/web/controllers/routes.py`
- Alembic migration for the GIN index
- Tests for both endpoints, filtering, and pagination

## Scale Assumptions

- Hundreds to thousands of security aggregates
- Fewer writes than reads
- No need for caching or materialized views at this scale
