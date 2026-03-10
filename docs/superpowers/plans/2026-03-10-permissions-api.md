# Permissions API Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two API endpoints for looking up granted and received authorization permissions, with pagination and filtering.

**Architecture:** New `/api/v0/authorizations/granted/{address}.json` and `/api/v0/authorizations/received/{address}.json` endpoints backed by the existing `aggregates` table. The reverse lookup uses a GIN-indexed JSONB containment query. Filtering and pagination are applied in Python after fetching results.

**Tech Stack:** aiohttp, SQLAlchemy 2.0, PostgreSQL JSONB + GIN index, Pydantic validation, Alembic migrations, pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-03-10-permissions-api-design.md`

---

## Chunk 1: Database Layer

### Task 1: Alembic Migration — GIN Index

**Files:**
- Create: `deployment/migrations/versions/0055_b3c4d5e6f7a8_security_authorizations_gin_index.py`

- [ ] **Step 1: Create the migration file**

```python
"""Add GIN index on security aggregate authorizations for reverse lookup

Revision ID: a1b2c3d4e5f6
Revises: 0a1b2c3d4e5f
Create Date: 2026-03-10
"""

from alembic import op

revision = "b3c4d5e6f7a8"
down_revision = "0a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX ix_aggregates_security_authorizations
        ON aggregates
        USING GIN ((content -> 'authorizations') jsonb_path_ops)
        WHERE key = 'security'
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_aggregates_security_authorizations")
```

- [ ] **Step 2: Verify migration applies cleanly**

Run: `hatch run testing:test tests/db/test_aggregates.py -v -x`
Expected: All existing aggregate tests still pass (migration runs as part of test setup).

- [ ] **Step 3: Commit**

```bash
git add deployment/migrations/versions/0055_b3c4d5e6f7a8_security_authorizations_gin_index.py
git commit -m "feat: add GIN index on security aggregate authorizations"
```

---

### Task 2: DB Accessor — Authorization Queries

**Files:**
- Create: `src/aleph/db/accessors/authorizations.py`
- Test: `tests/db/test_authorizations.py`

- [ ] **Step 1: Write the failing test for `get_granted_authorizations`**

Create `tests/db/test_authorizations.py`:

```python
import datetime as dt

import pytest

from aleph.db.accessors.authorizations import get_granted_authorizations
from aleph.db.models import AggregateDb, AggregateElementDb


@pytest.fixture
def security_aggregates(session_factory):
    """Insert security aggregates for testing.

    AggregateDb has a FK to AggregateElementDb, so we must create
    matching element rows first.
    """
    with session_factory() as session:
        # Create aggregate elements (required by FK constraint)
        session.add(
            AggregateElementDb(
                item_hash="hash_a",
                key="security",
                owner="0xOwnerA",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.add(
            AggregateElementDb(
                item_hash="hash_b",
                key="security",
                owner="0xOwnerB",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
            )
        )
        session.add(
            AggregateElementDb(
                item_hash="hash_empty",
                key="security",
                owner="0xOwnerEmpty",
                content={},
                creation_datetime=dt.datetime(2024, 1, 3, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()

        # Owner A grants to B and C
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerA",
                content={
                    "authorizations": [
                        {
                            "address": "0xGranteeB",
                            "types": ["POST"],
                            "channels": ["chan1"],
                            "chain": "ETH",
                        },
                        {
                            "address": "0xGranteeC",
                            "types": ["STORE"],
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_a",
                dirty=False,
            )
        )
        # Owner B grants to A
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerB",
                content={
                    "authorizations": [
                        {
                            "address": "0xOwnerA",
                            "types": ["POST", "STORE"],
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_b",
                dirty=False,
            )
        )
        # Owner with no authorizations key
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerEmpty",
                content={},
                creation_datetime=dt.datetime(2024, 1, 3, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_empty",
                dirty=False,
            )
        )
        session.commit()


def test_get_granted_authorizations(session_factory, security_aggregates):
    """get_granted_authorizations returns the raw aggregate content."""
    with session_factory() as session:
        result = get_granted_authorizations(session=session, owner="0xOwnerA")

    assert result is not None
    assert "authorizations" in result
    auths = result["authorizations"]
    assert len(auths) == 2
    # Raw content still includes the address field
    assert auths[0]["address"] == "0xGranteeB"
    assert auths[1]["address"] == "0xGranteeC"


def test_get_granted_authorizations_no_aggregate(session_factory, security_aggregates):
    with session_factory() as session:
        result = get_granted_authorizations(session=session, owner="0xNobody")

    assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run testing:test tests/db/test_authorizations.py -v -x`
Expected: FAIL with `ModuleNotFoundError: No module named 'aleph.db.accessors.authorizations'`

- [ ] **Step 3: Write minimal implementation for `get_granted_authorizations`**

Create `src/aleph/db/accessors/authorizations.py`:

```python
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from aleph.db.models import AggregateDb
from aleph.types.db_session import DbSession

logger = logging.getLogger(__name__)


def get_granted_authorizations(
    session: DbSession,
    owner: str,
) -> Optional[Dict[str, Any]]:
    """Get the security aggregate content for an owner (forward lookup).

    Returns the raw security aggregate content dict, or None if no
    security aggregate exists for the owner.
    """
    select_stmt = select(AggregateDb.content).where(
        (AggregateDb.key == "security") & (AggregateDb.owner == owner)
    )
    result = session.execute(select_stmt).scalar()
    if result is None:
        return None
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `hatch run testing:test tests/db/test_authorizations.py::test_get_granted_authorizations tests/db/test_authorizations.py::test_get_granted_authorizations_no_aggregate -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for `get_received_authorizations`**

Append to `tests/db/test_authorizations.py`:

```python
from aleph.db.accessors.authorizations import get_received_authorizations


def test_get_received_authorizations(session_factory, security_aggregates):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xOwnerA")

    # 0xOwnerB granted permissions to 0xOwnerA
    assert len(results) == 1
    owner, auths = results[0]
    assert owner == "0xOwnerB"
    assert len(auths) == 1
    assert auths[0]["types"] == ["POST", "STORE"]
    # 'address' field is stripped (redundant with the lookup key)
    assert "address" not in auths[0]


def test_get_received_authorizations_multiple_granters(
    session_factory, security_aggregates
):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xGranteeB")

    # Only 0xOwnerA granted to 0xGranteeB
    assert len(results) == 1
    owner, auths = results[0]
    assert owner == "0xOwnerA"
    assert len(auths) == 1
    assert auths[0]["types"] == ["POST"]
    assert "address" not in auths[0]


def test_get_received_authorizations_none(session_factory, security_aggregates):
    with session_factory() as session:
        results = get_received_authorizations(session=session, address="0xNobody")

    assert results == []
```

- [ ] **Step 6: Run test to verify it fails**

Run: `hatch run testing:test tests/db/test_authorizations.py::test_get_received_authorizations -v -x`
Expected: FAIL with `ImportError: cannot import name 'get_received_authorizations'`

- [ ] **Step 7: Implement `get_received_authorizations`**

Add to `src/aleph/db/accessors/authorizations.py`:

```python
def get_received_authorizations(
    session: DbSession,
    address: str,
) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Reverse lookup: find all security aggregates that grant permissions to address.

    Uses the GIN index on content->'authorizations' for efficient containment query.

    Returns a list of (owner, matching_authorizations) tuples where
    matching_authorizations contains only the entries for the target address.
    """
    select_stmt = select(AggregateDb.owner, AggregateDb.content).where(
        (AggregateDb.key == "security")
        & AggregateDb.content["authorizations"].contains([{"address": address}])
    )
    rows = session.execute(select_stmt).all()

    results = []
    for owner, content in rows:
        all_auths = content.get("authorizations", [])
        # Filter to matching entries and strip the redundant 'address' field
        matching = [
            {k: v for k, v in auth.items() if k != "address"}
            for auth in all_auths
            if auth.get("address") == address
        ]
        if matching:
            results.append((owner, matching))

    return results
```

Note: The `.contains([{"address": address}])` call generates the `@> '[{"address": "..."}]'` containment operator that uses the GIN index. We then filter in Python to extract only the matching entries.

- [ ] **Step 8: Run all accessor tests**

Run: `hatch run testing:test tests/db/test_authorizations.py -v`
Expected: All 5 tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/aleph/db/accessors/authorizations.py tests/db/test_authorizations.py
git commit -m "feat: add authorization DB accessors for forward and reverse lookup"
```

---

## Chunk 2: API Controller and Filtering

### Task 3: Authorization Filtering Logic

**Files:**
- Modify: `src/aleph/db/accessors/authorizations.py` (add filtering)
- Test: `tests/db/test_authorizations.py` (add filter tests)

- [ ] **Step 1: Write failing tests for filtering**

Append to `tests/db/test_authorizations.py`:

```python
from aleph.db.accessors.authorizations import filter_authorizations


@pytest.fixture
def sample_authorizations():
    """Grouped authorization data for testing filters.

    The 'address' field is already stripped at this stage (done by
    _build_grouped_from_content / get_received_authorizations).
    """
    return {
        "0xGranterA": [
            {
                "types": ["POST"],
                "channels": ["chan1", "chan2"],
                "chain": "ETH",
                "post_types": ["amend"],
                "aggregate_keys": [],
            },
            {
                "types": ["STORE"],
                "channels": ["chan3"],
                "chain": "SOL",
            },
        ],
        "0xGranterB": [
            {
                "types": ["POST", "STORE"],
            },
        ],
        "0xGranterC": [
            {
                "types": ["AGGREGATE"],
                "aggregate_keys": ["key1"],
            },
        ],
    }


def test_filter_by_types(sample_authorizations):
    result = filter_authorizations(sample_authorizations, types=["POST"])
    # 0xGranterA has a POST entry, 0xGranterB has POST+STORE, 0xGranterC excluded
    assert "0xGranterA" in result
    assert "0xGranterB" in result
    assert "0xGranterC" not in result
    # Only the POST entry from 0xGranterA, not the STORE one
    assert len(result["0xGranterA"]) == 1
    assert ["POST"] in [e.get("types") for e in result["0xGranterA"]]


def test_filter_by_channels(sample_authorizations):
    result = filter_authorizations(sample_authorizations, channels=["chan1"])
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert "chan1" in result["0xGranterA"][0]["channels"]
    # 0xGranterB has no channels field -> it matches any channel (unrestricted)
    assert "0xGranterB" in result


def test_filter_by_chains(sample_authorizations):
    result = filter_authorizations(sample_authorizations, chains=["ETH"])
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert result["0xGranterA"][0]["chain"] == "ETH"
    # 0xGranterB has no chain -> unrestricted, matches
    assert "0xGranterB" in result


def test_filter_by_post_types(sample_authorizations):
    result = filter_authorizations(sample_authorizations, post_types=["amend"])
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    # 0xGranterB has no post_types -> unrestricted
    assert "0xGranterB" in result


def test_filter_by_aggregate_keys(sample_authorizations):
    result = filter_authorizations(
        sample_authorizations, aggregate_keys=["key1"]
    )
    assert "0xGranterC" in result
    # 0xGranterB has no aggregate_keys -> unrestricted
    assert "0xGranterB" in result
    # 0xGranterA first entry has empty aggregate_keys -> unrestricted
    assert "0xGranterA" in result


def test_filter_no_filters(sample_authorizations):
    result = filter_authorizations(sample_authorizations)
    assert result == sample_authorizations


def test_filter_combined(sample_authorizations):
    result = filter_authorizations(
        sample_authorizations, types=["POST"], chains=["ETH"]
    )
    # Only 0xGranterA has POST+ETH, 0xGranterB has POST but no chain (unrestricted)
    assert "0xGranterA" in result
    assert len(result["0xGranterA"]) == 1
    assert "0xGranterB" in result
    assert "0xGranterC" not in result
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run testing:test tests/db/test_authorizations.py::test_filter_by_types -v -x`
Expected: FAIL with `ImportError: cannot import name 'filter_authorizations'`

- [ ] **Step 3: Implement `filter_authorizations`**

Add to `src/aleph/db/accessors/authorizations.py`:

```python
def filter_authorizations(
    grouped_auths: Dict[str, List[Dict[str, Any]]],
    *,
    channels: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    post_types: Optional[List[str]] = None,
    chains: Optional[List[str]] = None,
    aggregate_keys: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Filter authorization entries and remove addresses with no remaining entries.

    Filtering logic mirrors the permission model: if a filter field is not set
    on an authorization entry, the entry is unrestricted for that dimension and
    matches any filter value.
    """

    def _entry_matches(entry: Dict[str, Any]) -> bool:
        if channels:
            entry_channels = entry.get("channels", [])
            if entry_channels and not set(entry_channels) & set(channels):
                return False

        if types:
            entry_types = entry.get("types", [])
            if entry_types and not set(entry_types) & set(types):
                return False

        if post_types:
            entry_post_types = entry.get("post_types", [])
            if entry_post_types and not set(entry_post_types) & set(post_types):
                return False

        if chains:
            entry_chain = entry.get("chain")
            if entry_chain and entry_chain not in chains:
                return False

        if aggregate_keys:
            entry_akeys = entry.get("aggregate_keys", [])
            if entry_akeys and not set(entry_akeys) & set(aggregate_keys):
                return False

        return True

    result = {}
    for address, entries in grouped_auths.items():
        filtered = [e for e in entries if _entry_matches(e)]
        if filtered:
            result[address] = filtered

    return result
```

- [ ] **Step 4: Run all filter tests**

Run: `hatch run testing:test tests/db/test_authorizations.py -v -k "filter"`
Expected: All 7 filter tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/aleph/db/accessors/authorizations.py tests/db/test_authorizations.py
git commit -m "feat: add authorization filtering logic"
```

---

### Task 4: Pagination Utility

**Files:**
- Modify: `src/aleph/db/accessors/authorizations.py` (add pagination)
- Test: `tests/db/test_authorizations.py` (add pagination tests)

- [ ] **Step 1: Write failing test for pagination**

Append to `tests/db/test_authorizations.py`:

```python
from aleph.db.accessors.authorizations import paginate_authorizations


def test_paginate_first_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=1, pagination=2)
    assert total == 5
    assert len(result) == 2


def test_paginate_second_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=2, pagination=2)
    assert total == 5
    assert len(result) == 2


def test_paginate_last_page():
    data = {f"0xAddr{i}": [{"types": ["POST"]}] for i in range(5)}
    result, total = paginate_authorizations(data, page=3, pagination=2)
    assert total == 5
    assert len(result) == 1


def test_paginate_empty():
    result, total = paginate_authorizations({}, page=1, pagination=20)
    assert total == 0
    assert result == {}


def test_paginate_beyond_range():
    data = {"0xAddr0": [{"types": ["POST"]}]}
    result, total = paginate_authorizations(data, page=5, pagination=20)
    assert total == 1
    assert result == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run testing:test tests/db/test_authorizations.py::test_paginate_first_page -v -x`
Expected: FAIL with `ImportError: cannot import name 'paginate_authorizations'`

- [ ] **Step 3: Implement `paginate_authorizations`**

Add to `src/aleph/db/accessors/authorizations.py`:

```python
def paginate_authorizations(
    grouped_auths: Dict[str, List[Dict[str, Any]]],
    page: int,
    pagination: int,
) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    """Paginate authorization results by address.

    Returns (paginated_dict, total_count) where total_count is the number
    of distinct addresses before pagination.
    """
    total = len(grouped_auths)
    keys = list(grouped_auths.keys())
    start = (page - 1) * pagination
    end = start + pagination
    page_keys = keys[start:end]
    result = {k: grouped_auths[k] for k in page_keys}
    return result, total
```

- [ ] **Step 4: Run pagination tests**

Run: `hatch run testing:test tests/db/test_authorizations.py -v -k "paginate"`
Expected: All 5 pagination tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/aleph/db/accessors/authorizations.py tests/db/test_authorizations.py
git commit -m "feat: add authorization pagination utility"
```

---

### Task 5: API Controller — Endpoints

**Files:**
- Create: `src/aleph/web/controllers/authorizations.py`
- Modify: `src/aleph/web/controllers/routes.py`
- Test: `tests/api/test_authorizations.py`

- [ ] **Step 1: Write failing test for the granted endpoint**

Create `tests/api/test_authorizations.py`:

```python
import datetime as dt

import pytest

from aleph.db.models import AggregateDb, AggregateElementDb

GRANTED_URI = "/api/v0/authorizations/granted/{address}.json"
RECEIVED_URI = "/api/v0/authorizations/received/{address}.json"


@pytest.fixture
def security_aggregates(session_factory):
    """Insert security aggregates for API testing.

    AggregateDb has a FK to AggregateElementDb, so we must create
    matching element rows first.
    """
    with session_factory() as session:
        # Create aggregate elements (required by FK constraint)
        session.add(
            AggregateElementDb(
                item_hash="hash_a",
                key="security",
                owner="0xOwnerA",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            )
        )
        session.add(
            AggregateElementDb(
                item_hash="hash_b",
                key="security",
                owner="0xOwnerB",
                content={"authorizations": []},
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
            )
        )
        session.flush()

        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerA",
                content={
                    "authorizations": [
                        {
                            "address": "0xGranteeB",
                            "types": ["POST"],
                            "channels": ["chan1"],
                            "chain": "ETH",
                        },
                        {
                            "address": "0xGranteeC",
                            "types": ["STORE"],
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_a",
                dirty=False,
            )
        )
        session.add(
            AggregateDb(
                key="security",
                owner="0xOwnerB",
                content={
                    "authorizations": [
                        {
                            "address": "0xGranteeB",
                            "types": ["POST", "STORE"],
                            "chain": "SOL",
                        },
                    ]
                },
                creation_datetime=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                last_revision_hash="hash_b",
                dirty=False,
            )
        )
        session.commit()


@pytest.mark.asyncio
async def test_granted_basic(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA")
    )
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xOwnerA"
    assert "0xGranteeB" in data["authorizations"]
    assert "0xGranteeC" in data["authorizations"]
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_granted_no_aggregate(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xNobody")
    )
    assert response.status == 200
    data = await response.json()
    assert data["authorizations"] == {}
    assert data["pagination_total"] == 0


@pytest.mark.asyncio
async def test_granted_filter_grantee(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"grantee": "0xGranteeB"},
    )
    assert response.status == 200
    data = await response.json()
    assert "0xGranteeB" in data["authorizations"]
    assert "0xGranteeC" not in data["authorizations"]


@pytest.mark.asyncio
async def test_granted_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"pagination": "1", "page": "1"},
    )
    assert response.status == 200
    data = await response.json()
    assert len(data["authorizations"]) == 1
    assert data["pagination_total"] == 2
    assert data["pagination_per_page"] == 1
    assert data["pagination_page"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `hatch run testing:test tests/api/test_authorizations.py::test_granted_basic -v -x`
Expected: FAIL (404 — route not registered)

- [ ] **Step 3: Create the controller**

Create `src/aleph/web/controllers/authorizations.py`:

```python
import logging
from typing import Any, Dict, List, Optional

from aiohttp import web
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import select

from aleph.db.accessors.aggregates import refresh_aggregate
from aleph.db.accessors.authorizations import (
    filter_authorizations,
    get_granted_authorizations,
    get_received_authorizations,
    paginate_authorizations,
)
from aleph.db.models import AggregateDb
from aleph.schemas.messages_query_params import LIST_FIELD_SEPARATOR
from aleph.web.controllers.app_state_getters import get_session_factory_from_request

LOGGER = logging.getLogger(__name__)


class AuthorizationsQueryParams(BaseModel):
    channels: Optional[List[str]] = None
    types: Optional[List[str]] = None
    post_types: Optional[List[str]] = Field(default=None, alias="postTypes")
    chains: Optional[List[str]] = None
    aggregate_keys: Optional[List[str]] = Field(default=None, alias="aggregateKeys")
    pagination: int = Field(default=20, ge=1, le=500, alias="pagination")
    page: int = Field(default=1, ge=1, alias="page")

    @field_validator(
        "channels", "types", "post_types", "chains", "aggregate_keys", mode="before"
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


class GrantedQueryParams(AuthorizationsQueryParams):
    grantee: Optional[str] = None


class ReceivedQueryParams(AuthorizationsQueryParams):
    granter: Optional[str] = None


def _build_grouped_from_content(
    content: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group authorization entries by their 'address' field.

    The 'address' key is stripped from entries since it's redundant
    with the grouping key.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for auth in content.get("authorizations", []):
        addr = auth.get("address", "")
        if addr:
            entry = {k: v for k, v in auth.items() if k != "address"}
            grouped.setdefault(addr, []).append(entry)
    return grouped


async def view_granted_authorizations(request: web.Request) -> web.Response:
    address: str = request.match_info["address"]

    try:
        query_params = GrantedQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        # Refresh dirty security aggregate for this address
        dirty = session.execute(
            select(AggregateDb.key).where(
                (AggregateDb.owner == address)
                & (AggregateDb.key == "security")
                & AggregateDb.dirty
            )
        ).scalar()
        if dirty:
            LOGGER.info("Refreshing dirty security aggregate for %s", address)
            refresh_aggregate(session=session, owner=address, key="security")
            session.commit()

        content = get_granted_authorizations(session=session, owner=address)

    if content is None:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
    else:
        grouped = _build_grouped_from_content(content)

    # Apply grantee filter
    if query_params.grantee:
        grouped = {
            k: v for k, v in grouped.items() if k == query_params.grantee
        }

    # Apply authorization filters
    grouped = filter_authorizations(
        grouped,
        channels=query_params.channels,
        types=query_params.types,
        post_types=query_params.post_types,
        chains=query_params.chains,
        aggregate_keys=query_params.aggregate_keys,
    )

    # Paginate
    paginated, total = paginate_authorizations(
        grouped, page=query_params.page, pagination=query_params.pagination
    )

    return web.json_response(
        {
            "authorizations": paginated,
            "pagination_page": query_params.page,
            "pagination_per_page": query_params.pagination,
            "pagination_total": total,
            "pagination_item": "authorizations",
            "address": address,
        }
    )


async def view_received_authorizations(request: web.Request) -> web.Response:
    address: str = request.match_info["address"]

    try:
        query_params = ReceivedQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        results = get_received_authorizations(session=session, address=address)

    # Build grouped dict: {granter_address: [auth_entries]}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for owner, auths in results:
        grouped[owner] = auths

    # Apply granter filter
    if query_params.granter:
        grouped = {
            k: v for k, v in grouped.items() if k == query_params.granter
        }

    # Apply authorization filters
    grouped = filter_authorizations(
        grouped,
        channels=query_params.channels,
        types=query_params.types,
        post_types=query_params.post_types,
        chains=query_params.chains,
        aggregate_keys=query_params.aggregate_keys,
    )

    # Paginate
    paginated, total = paginate_authorizations(
        grouped, page=query_params.page, pagination=query_params.pagination
    )

    return web.json_response(
        {
            "authorizations": paginated,
            "pagination_page": query_params.page,
            "pagination_per_page": query_params.pagination,
            "pagination_total": total,
            "pagination_item": "authorizations",
            "address": address,
        }
    )
```

- [ ] **Step 4: Register routes**

In `src/aleph/web/controllers/routes.py`:

1. Add to the imports at the top:
```python
from aleph.web.controllers import (
    accounts,
    aggregates,
    authorizations,
    channels,
    ...
)
```

2. Add to the `api_routes` list (after the aggregates routes):
```python
        web.get(
            "/api/v0/authorizations/granted/{address}.json",
            authorizations.view_granted_authorizations,
        ),
        web.get(
            "/api/v0/authorizations/received/{address}.json",
            authorizations.view_received_authorizations,
        ),
```

- [ ] **Step 5: Run the granted endpoint tests**

Run: `hatch run testing:test tests/api/test_authorizations.py -v -k "granted"`
Expected: All 4 granted tests PASS

- [ ] **Step 6: Write failing tests for the received endpoint**

Append to `tests/api/test_authorizations.py`:

```python
@pytest.mark.asyncio
async def test_received_basic(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB")
    )
    assert response.status == 200
    data = await response.json()
    assert data["address"] == "0xGranteeB"
    # Both 0xOwnerA and 0xOwnerB granted to 0xGranteeB
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" in data["authorizations"]
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_received_no_grants(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xNobody")
    )
    assert response.status == 200
    data = await response.json()
    assert data["authorizations"] == {}
    assert data["pagination_total"] == 0


@pytest.mark.asyncio
async def test_received_filter_granter(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"granter": "0xOwnerA"},
    )
    assert response.status == 200
    data = await response.json()
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" not in data["authorizations"]


@pytest.mark.asyncio
async def test_received_filter_chains(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"chains": "ETH"},
    )
    assert response.status == 200
    data = await response.json()
    # 0xOwnerA has ETH chain, 0xOwnerB has SOL chain
    assert "0xOwnerA" in data["authorizations"]
    assert "0xOwnerB" not in data["authorizations"]


@pytest.mark.asyncio
async def test_received_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"pagination": "1", "page": "1"},
    )
    assert response.status == 200
    data = await response.json()
    assert len(data["authorizations"]) == 1
    assert data["pagination_total"] == 2


@pytest.mark.asyncio
async def test_received_filter_types(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        RECEIVED_URI.format(address="0xGranteeB"),
        params={"types": "STORE"},
    )
    assert response.status == 200
    data = await response.json()
    # 0xOwnerB has STORE in types, 0xOwnerA only has POST
    assert "0xOwnerB" in data["authorizations"]
    assert "0xOwnerA" not in data["authorizations"]


@pytest.mark.asyncio
async def test_invalid_pagination(ccn_api_client, security_aggregates):
    response = await ccn_api_client.get(
        GRANTED_URI.format(address="0xOwnerA"),
        params={"pagination": "0"},
    )
    assert response.status == 422
```

- [ ] **Step 7: Run all API tests**

Run: `hatch run testing:test tests/api/test_authorizations.py -v`
Expected: All 11 tests PASS

- [ ] **Step 8: Run linting**

Run: `hatch run linting:fmt && hatch run linting:all`
Expected: No errors

- [ ] **Step 9: Commit**

```bash
git add src/aleph/web/controllers/authorizations.py src/aleph/web/controllers/routes.py tests/api/test_authorizations.py
git commit -m "feat: add authorizations API endpoints with filtering and pagination"
```

---

## Chunk 3: Final Verification

### Task 6: Full Test Suite and Cleanup

- [ ] **Step 1: Run the full test suite to check for regressions**

Run: `hatch run testing:test -v`
Expected: All tests PASS, no regressions

- [ ] **Step 2: Run full linting suite**

Run: `hatch run linting:fmt && hatch run linting:all`
Expected: No errors

- [ ] **Step 3: Fix any issues found**

Address any linting or test failures.

- [ ] **Step 4: Final commit if needed**

Only if cleanup was required in step 3.
