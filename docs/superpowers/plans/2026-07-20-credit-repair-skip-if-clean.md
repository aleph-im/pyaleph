# Credit Repair Skip-If-Clean Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unconditional every-boot rebuild of the `credit_balances` lot cache with cheap set-based screening queries, rebuilding only addresses that actually drifted (and doing a full rebuild only on bootstrap or drain-policy changes).

**Architecture:** The cache (`credit_balances`) is a pure function of the append-only ledger (`credit_history`); both are written in the same transaction, so drift can only come from (1) out-of-order P2P message arrival, (2) a drain-policy code change, or (3) an eager-writer bug. Three set-based SQL screens detect each class; a new single-row `credit_repair_state` table stores the policy version and a `last_update` watermark so the out-of-order screen is incremental. `_repair_credit_balances` becomes: full rebuild if no state row or policy version changed; otherwise screen, rebuild only flagged addresses (normally zero), log drift as WARNING, persist new state.

**Tech Stack:** Python 3, SQLAlchemy 2.0 (ORM + `text()` SQL), Alembic, PostgreSQL 15, pytest (real-PG fixtures via `session_factory`).

## Global Constraints

- Alembic revision IDs MUST be 12-char hex (`0-9a-f` only); filename format `NNNN_<hex_id>_<description>.py` (CLAUDE.md).
- Do NOT add `Co-Authored-By` trailers to any commit message (user preference).
- After development: `hatch run linting:fmt` then `hatch run linting:all`. Known pre-existing failure: the mypy step aborts on a SQLAlchemy-stubs plugin conflict before checking any file — reproduce on the untouched base to confirm it is not caused by this change, then ignore it.
- Branch: create `od/credit-repair-screen` off `main` (do NOT base on `od/cli-repair-toggle`).
- The next free migration number is assumed to be `0065` with head `a7c3e9f2d5b1` (0064). **At execution time, verify with `ls deployment/migrations/versions/ | tail -3`;** if main moved, renumber the file and update `down_revision` to the current head's hex id.

### Test environment (this machine — from project memory)

```bash
# Postgres 15.1 + Redis containers (the suite drops/recreates the public schema and runs alembic each run):
docker start pyaleph-test-pg pyaleph-test-redis 2>/dev/null || {
  docker run -d --name pyaleph-test-pg -p 127.0.0.1:5432:5432 \
    -e POSTGRES_USER=aleph -e POSTGRES_PASSWORD=decentralize-everything \
    -e POSTGRES_DB=aleph postgres:15.1
  docker run -d --name pyaleph-test-redis -p 127.0.0.1:6379:6379 redis:8.4.0
}

# hatch lives at /home/olivier/git/aleph/pyaleph/.venv/bin/hatch (NOT on PATH).
# hatch keys envs by working dir: in a fresh worktree, build the env once with nightly Rust:
HATCH=/home/olivier/git/aleph/pyaleph/.venv/bin/hatch
RUSTUP_TOOLCHAIN=nightly $HATCH env create testing

# The testing env does NOT install the project; inject src via PYTHONPATH:
ENVPY=$($HATCH env find testing)/bin/python
PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -v
```

All "Run:" steps below use `PYTHONPATH=$PWD/src $ENVPY -m pytest ...` — set `HATCH`/`ENVPY` once per shell.

### Background invariants (why the screens are sound — read before implementing)

- `credit_history` is append-only (`INSERT ... ON CONFLICT DO NOTHING`, PK `(credit_ref, credit_index)`); rows are never updated or deleted. Cache mutations and history appends happen in the same transaction (`update_credit_balances_*` in `src/aleph/db/accessors/balances.py`), so crashes cannot desync them.
- `credit_history.id` is **never populated** (not in the COPY column list, no sequence) — do not use it. The insertion-order proxy is `last_update`, set to `utc_now()` (Python-side) once per writer call.
- The eager drain (`_consume_address_credits`) and the repair replay (`_rebuild_credit_lots_for_address` in `src/aleph/repair.py`) implement the same policy: drain in `(message_timestamp, credit_ref, credit_index)` order, skip lots expired at the drain's `message_timestamp`, silently drop over-draw leftover. The eager writer decrements lots in place and **leaves `amount_remaining = 0` rows**; the replay **omits** zero rows. Zero rows are therefore legitimate cache content (and a handy detector: a rebuild purges them, a skip preserves them).
- Zero-amount grants exist (zero-amount transfer fallback writes `+0` history rows and `amount_remaining = 0` cache rows). Screens must not flag them.
- Whitelisted transfer senders write **no** sender drain row (credits from nothing) — no special-casing needed.
- Repair runs at boot **before** jobs/message processing start (`src/aleph/commands.py:195-200`), so there are no concurrent credit writers during screening/rebuild.

Drift taxonomy → screen mapping:

| Drift class | Screen | Mechanism |
|---|---|---|
| Policy/code change | policy version stamp | full rebuild when `REPAIR_POLICY_VERSION` ≠ stored version |
| Out-of-order arrival (late row replay-ordering before an already-applied row, a drain involved) | S3 inversion | self-join on rows newer than watermark, tuple comparison on `(message_timestamp, credit_ref, credit_index)` vs `last_update` order |
| Writer bug, total-changing | S2 conservation | `SUM(cache) < SUM(ledger)` always a bug; strict equality required when no drain, or (no expiring grant AND running ledger sum never negative) |
| Writer bug, per-row | S1 structural | cache row vs its granting history row: orphan / negative / exceeds grant / mismatched attrs |
| Writer bug, total-preserving misattribution | *not screened* | documented residual; only full replay can see it |

S2 soundness: every drain that fully lands keeps `SUM(cache) == SUM(ledger)` exactly. A drain leaves leftover (cache > ledger) only when valid funds were insufficient at its instant — impossible unless the address has an expiring grant (expiry bounce) or the running ledger sum goes negative (over-draw). Hence equality is *required* for addresses with no drains, or with drains but no expiring grants and a never-negative running sum; for all others only `cache >= ledger` is required. Over-approximating "bounce possible" merely weakens the check for that address — never a false flag.

## File Structure

- `deployment/migrations/versions/0065_c5e1a9d3f7b2_credit_repair_state.py` — new: single-row state table.
- `src/aleph/db/models/balances.py` — add `CreditRepairStateDb` (`models/__init__.py` does `from .balances import *` with no `__all__`, so the class is exported automatically — no `__init__.py` edit).
- `src/aleph/db/accessors/balances.py` — add `get_credit_repair_state` / `upsert_credit_repair_state`.
- `src/aleph/repair.py` — add `REPAIR_POLICY_VERSION`, three screen functions, rewire `_repair_credit_balances`. `_rebuild_credit_lots_for_address` is unchanged.
- `tests/db/test_credit_repair.py` — new: all tests for this feature.

---

### Task 1: `credit_repair_state` table, model, accessors

**Files:**
- Create: `deployment/migrations/versions/0065_c5e1a9d3f7b2_credit_repair_state.py`
- Modify: `src/aleph/db/models/balances.py` (append class)
- Modify: `src/aleph/db/accessors/balances.py` (append two functions)
- Test: `tests/db/test_credit_repair.py`

**Interfaces:**
- Consumes: existing `Base`, `DbSession`, `pg_insert`.
- Produces (used by Task 5):
  - `class CreditRepairStateDb` — columns `id: int` (PK, always 1), `policy_version: int`, `history_watermark: dt.datetime`, `last_run: dt.datetime`.
  - `get_credit_repair_state(session: DbSession) -> Optional[CreditRepairStateDb]`
  - `upsert_credit_repair_state(session: DbSession, policy_version: int, history_watermark: dt.datetime, last_run: dt.datetime) -> None`

- [ ] **Step 1: Write the failing test**

Create `tests/db/test_credit_repair.py`:

```python
"""Tests for the skip-if-clean credit balances repair (screens + orchestration)."""

import datetime as dt

from aleph.db.accessors.balances import (
    get_credit_repair_state,
    upsert_credit_repair_state,
)
from aleph.types.db_session import DbSessionFactory


def test_repair_state_roundtrip(session_factory: DbSessionFactory):
    watermark = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    run_time = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)

    with session_factory() as session:
        assert get_credit_repair_state(session) is None

        upsert_credit_repair_state(
            session,
            policy_version=1,
            history_watermark=watermark,
            last_run=run_time,
        )
        session.commit()

    with session_factory() as session:
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == 1
        assert state.history_watermark == watermark
        assert state.last_run == run_time

        # Upsert overwrites the single row in place.
        new_watermark = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        upsert_credit_repair_state(
            session,
            policy_version=2,
            history_watermark=new_watermark,
            last_run=run_time,
        )
        session.commit()

    with session_factory() as session:
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == 2
        assert state.history_watermark == new_watermark
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py::test_repair_state_roundtrip -v`
Expected: FAIL with `ImportError: cannot import name 'get_credit_repair_state'`

- [ ] **Step 3: Write the migration**

First verify `0065` is still free and `a7c3e9f2d5b1` is still head: `ls deployment/migrations/versions/ | tail -3`. Then create `deployment/migrations/versions/0065_c5e1a9d3f7b2_credit_repair_state.py`:

```python
"""Add the credit_repair_state table

Revision ID: c5e1a9d3f7b2
Revises: a7c3e9f2d5b1
Create Date: 2026-07-20

Single-row bookkeeping for the boot-time credit balances repair: which drain
policy version the lot cache was last fully rebuilt under, and the
credit_history insertion watermark (max last_update) up to which out-of-order
arrivals have already been reconciled. Lets the repair skip the per-address
delete-and-replay entirely when set-based screening finds no drift.
"""

import sqlalchemy as sa
from alembic import op

revision = "c5e1a9d3f7b2"
down_revision = "a7c3e9f2d5b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "credit_repair_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("history_watermark", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_run", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="credit_repair_state_pkey"),
        sa.CheckConstraint("id = 1", name="credit_repair_state_single_row"),
    )


def downgrade() -> None:
    op.drop_table("credit_repair_state")
```

- [ ] **Step 4: Add the model**

In `src/aleph/db/models/balances.py`, add `CheckConstraint` to the existing `sqlalchemy` import list, then append at the end of the file:

```python
class CreditRepairStateDb(Base):
    """Single-row bookkeeping for the boot-time credit repair. ``id`` is always
    1. ``policy_version`` is the drain-policy version the lot cache was last
    fully rebuilt under (bump ``aleph.repair.REPAIR_POLICY_VERSION`` when the
    drain semantics change). ``history_watermark`` is the max
    ``credit_history.last_update`` up to which out-of-order arrivals have been
    reconciled."""

    __tablename__ = "credit_repair_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    history_watermark: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    last_run: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    __table_args__ = (
        CheckConstraint("id = 1", name="credit_repair_state_single_row"),
    )
```

No `models/__init__.py` change needed: it does `from .balances import *` and `balances.py` has no `__all__`, so the new class is exported automatically.

- [ ] **Step 5: Add the accessors**

In `src/aleph/db/accessors/balances.py`: add `CreditRepairStateDb` to the existing `from aleph.db.models import (...)` block, then append at the end of the file:

```python
def get_credit_repair_state(session: DbSession) -> Optional[CreditRepairStateDb]:
    """Fetch the single credit repair bookkeeping row, if any."""
    return session.get(CreditRepairStateDb, 1)


def upsert_credit_repair_state(
    session: DbSession,
    policy_version: int,
    history_watermark: dt.datetime,
    last_run: dt.datetime,
) -> None:
    """Insert or overwrite the single credit repair bookkeeping row."""
    stmt = pg_insert(CreditRepairStateDb).values(
        id=1,
        policy_version=policy_version,
        history_watermark=history_watermark,
        last_run=last_run,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[CreditRepairStateDb.id],
        set_={
            "policy_version": stmt.excluded.policy_version,
            "history_watermark": stmt.excluded.history_watermark,
            "last_run": stmt.excluded.last_run,
        },
    )
    session.execute(stmt)
```

(`Optional` is already imported in this module; `pg_insert` too.)

- [ ] **Step 6: Run test to verify it passes**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py::test_repair_state_roundtrip -v`
Expected: PASS (the suite re-runs alembic, exercising the new migration).

- [ ] **Step 7: Commit**

```bash
git add deployment/migrations/versions/0065_c5e1a9d3f7b2_credit_repair_state.py \
        src/aleph/db/models/balances.py src/aleph/db/models/__init__.py \
        src/aleph/db/accessors/balances.py tests/db/test_credit_repair.py
git commit -m "feat(repair): add credit_repair_state bookkeeping table"
```

---

### Task 2: Structural screen (S1)

**Files:**
- Modify: `src/aleph/repair.py`
- Test: `tests/db/test_credit_repair.py`

**Interfaces:**
- Consumes: `AlephCreditBalanceDb`, `AlephCreditHistoryDb` models; writer helpers from `aleph.db.accessors.balances`.
- Produces (used by Task 5): `_find_structural_violations(session: DbSession) -> Set[str]` — addresses with structurally broken cache rows.

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_credit_repair.py`. First extend the imports at the top of the file:

```python
from sqlalchemy import select, update

from aleph.db.accessors.balances import (
    get_credit_repair_state,
    update_credit_balances_distribution,
    update_credit_balances_expense,
    upsert_credit_repair_state,
)
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.repair import _find_structural_violations
```

Then add the seed helpers and tests:

```python
TS_GRANT = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
TS_SPEND = dt.datetime(2026, 1, 2, 12, 0, 0, tzinfo=dt.timezone.utc)


def _grant(session, address, amount, message_hash, ts, expiration=None):
    """Seed one distribution grant through the real writer (cache + history)."""
    entry = {
        "address": address,
        "amount": amount,
        "price": "0.5",
        "tx_hash": "0xabc",
        "provider": "test",
    }
    if expiration is not None:
        entry["expiration"] = int(expiration.timestamp() * 1000)
    update_credit_balances_distribution(
        session=session,
        credits_list=[entry],
        token="ALEPH",
        chain="ETH",
        message_hash=message_hash,
        message_timestamp=ts,
    )


def _spend(session, address, amount, message_hash, ts):
    """Seed one expense through the real writer (drains cache + appends history)."""
    update_credit_balances_expense(
        session=session,
        credits_list=[{"address": address, "amount": amount, "ref": "r"}],
        message_hash=message_hash,
        message_timestamp=ts,
    )


def test_structural_screen_clean(session_factory: DbSessionFactory):
    """A state produced purely by the eager writers passes, including a lot
    drained to exactly zero (the eager writer leaves the zero row)."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 1000, "spend_a", TS_SPEND)  # fully drains
        _grant(session, "0xbbb", 500, "grant_b", TS_GRANT)
        session.commit()

        # Sanity: the fully-drained zero row is still present.
        zero_rows = list(
            session.execute(
                select(AlephCreditBalanceDb).where(
                    AlephCreditBalanceDb.address == "0xaaa",
                    AlephCreditBalanceDb.amount_remaining == 0,
                )
            ).scalars()
        )
        assert len(zero_rows) == 1

        assert _find_structural_violations(session) == set()


def test_structural_screen_flags_orphan_lot(session_factory: DbSessionFactory):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.add(
            AlephCreditBalanceDb(
                address="0xdead",
                credit_ref="no_such_history_row",
                credit_index=0,
                amount_remaining=10,
                expiration_date=None,
                message_timestamp=TS_GRANT,
            )
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xdead"}


def test_structural_screen_flags_remaining_above_grant(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.commit()

        grant_amount = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=grant_amount + 1)
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xaaa"}


def test_structural_screen_flags_mismatched_expiration(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(expiration_date=dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc))
        )
        session.commit()

        assert _find_structural_violations(session) == {"0xaaa"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k structural -v`
Expected: FAIL with `ImportError: cannot import name '_find_structural_violations'`

- [ ] **Step 3: Implement the screen**

In `src/aleph/repair.py`: extend the typing import to `from typing import Any, Dict, Set`, add `from sqlalchemy.orm import aliased`, add `AlephCreditBalanceDb, AlephCreditHistoryDb` to the models import if missing (they are already imported), then add above `_rebuild_credit_lots_for_address`:

```python
def _find_structural_violations(session: DbSession) -> Set[str]:
    """Addresses whose cache rows are structurally inconsistent with their
    granting ``credit_history`` row.

    Flags: orphan lots (no history row for the PK), lots pointing at another
    address's or a negative (drain) history row, negative remainders,
    remainders above the granted amount, and mismatched expiration or
    timestamp. Zero-amount grants are legitimate (zero-amount transfer
    fallback rows), hence ``amount < 0`` rather than ``<= 0``.

    Pure read; over-flagging is safe (worst case an unneeded rebuild).
    """
    grant = aliased(AlephCreditHistoryDb)
    stmt = (
        select(AlephCreditBalanceDb.address)
        .outerjoin(
            grant,
            (grant.credit_ref == AlephCreditBalanceDb.credit_ref)
            & (grant.credit_index == AlephCreditBalanceDb.credit_index),
        )
        .where(
            grant.credit_ref.is_(None)
            | (grant.address != AlephCreditBalanceDb.address)
            | (grant.amount < 0)
            | (AlephCreditBalanceDb.amount_remaining < 0)
            | (AlephCreditBalanceDb.amount_remaining > grant.amount)
            | AlephCreditBalanceDb.expiration_date.is_distinct_from(
                grant.expiration_date
            )
            | AlephCreditBalanceDb.message_timestamp.is_distinct_from(
                grant.message_timestamp
            )
        )
        .distinct()
    )
    return set(session.execute(stmt).scalars())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k structural -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add src/aleph/repair.py tests/db/test_credit_repair.py
git commit -m "feat(repair): structural screen for credit lot cache drift"
```

---

### Task 3: Conservation screen (S2)

**Files:**
- Modify: `src/aleph/repair.py`
- Test: `tests/db/test_credit_repair.py`

**Interfaces:**
- Consumes: seed helpers `_grant` / `_spend` from Task 2's test code.
- Produces (used by Task 5): `_find_conservation_violations(session: DbSession) -> Set[str]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_credit_repair.py` (add `_find_conservation_violations` to the `aleph.repair` import):

```python
def test_conservation_screen_clean(session_factory: DbSessionFactory):
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        session.commit()

        assert _find_conservation_violations(session) == set()


def test_conservation_screen_flags_over_drain(session_factory: DbSessionFactory):
    """Cache total below ledger total is a definite bug in any regime."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        # Tamper: over-drain the lot to zero (still >= 0 and <= grant, so the
        # structural screen stays quiet — this is S2's job).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=0)
        )
        session.commit()

        assert _find_conservation_violations(session) == {"0xaaa"}


def test_conservation_screen_flags_under_drain_strict_address(
    session_factory: DbSessionFactory,
):
    """No expiring grants, never-negative running sum: equality is required,
    so an un-applied drain (cache too high) is flagged."""
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 300, "spend_a", TS_SPEND)
        grant_amount = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        # Tamper: pretend the drain never touched the lot (== grant, so the
        # structural screen stays quiet).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.credit_ref == "grant_a")
            .values(amount_remaining=grant_amount)
        )
        session.commit()

        assert _find_conservation_violations(session) == {"0xaaa"}


def test_conservation_screen_allows_expiry_bounce(session_factory: DbSessionFactory):
    """An expense arriving after the only credit expired legitimately leaves
    cache_sum > ledger_sum; must NOT be flagged."""
    expiry = dt.datetime(2026, 1, 15, tzinfo=dt.timezone.utc)
    after_expiry = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
    with session_factory() as session:
        _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT, expiration=expiry)
        _spend(session, "0xaaa", 400, "spend_a", after_expiry)  # bounces
        session.commit()

        # Sanity: the drain bounced (lot untouched), ledger went down anyway.
        remaining = session.execute(
            select(AlephCreditBalanceDb.amount_remaining).where(
                AlephCreditBalanceDb.credit_ref == "grant_a"
            )
        ).scalar_one()
        assert remaining > 0

        assert _find_conservation_violations(session) == set()


def test_conservation_screen_allows_overdraft(session_factory: DbSessionFactory):
    """Spending more than granted silently drops the excess (both eager and
    replay); cache_sum > ledger_sum is legitimate here; must NOT be flagged."""
    with session_factory() as session:
        _grant(session, "0xaaa", 100, "grant_a", TS_GRANT)
        _spend(session, "0xaaa", 500, "spend_a", TS_SPEND)  # over-draw
        session.commit()

        assert _find_conservation_violations(session) == set()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k conservation -v`
Expected: FAIL with `ImportError: cannot import name '_find_conservation_violations'`

- [ ] **Step 3: Implement the screen**

In `src/aleph/repair.py`, add `text` to the `sqlalchemy` import, then add below `_find_structural_violations`:

```python
# Per-address totals check. SUM(cache) == SUM(ledger) holds exactly whenever
# every drain fully landed. A drain leaves leftover (cache > ledger) only if
# valid funds were insufficient at its instant, which requires an expiring
# grant (expiry bounce) or a negative running ledger sum (over-draw, silently
# dropped by both the eager writer and the replay). So: cache < ledger is
# always a bug; strict equality is additionally required for addresses with
# no drains, or with no expiring grants and a never-negative running sum
# (computed in replay order). Over-approximating "bounce possible" only
# weakens the check for that address — never a false flag.
_CONSERVATION_SCREEN_SQL = text(
    """
    WITH ledger AS (
        SELECT address,
               SUM(amount) AS ledger_sum,
               bool_or(amount < 0) AS has_drain,
               bool_or(amount > 0 AND expiration_date IS NOT NULL)
                   AS has_expiring_grant,
               MIN(running_sum) AS min_running_sum
        FROM (
            SELECT address, amount, expiration_date,
                   SUM(amount) OVER (
                       PARTITION BY address
                       ORDER BY message_timestamp, credit_ref, credit_index
                   ) AS running_sum
            FROM credit_history
        ) ordered
        GROUP BY address
    ),
    cache AS (
        SELECT address, SUM(amount_remaining) AS cache_sum
        FROM credit_balances
        GROUP BY address
    )
    SELECT ledger.address
    FROM ledger
    LEFT JOIN cache USING (address)
    WHERE COALESCE(cache.cache_sum, 0) < ledger.ledger_sum
       OR (
            (NOT ledger.has_drain
             OR (NOT ledger.has_expiring_grant AND ledger.min_running_sum >= 0))
            AND COALESCE(cache.cache_sum, 0) <> ledger.ledger_sum
          )
    """
)


def _find_conservation_violations(session: DbSession) -> Set[str]:
    """Addresses whose cache total is impossible given their ledger total.

    See the SQL comment for the exact rule. Pure read; over-flagging is safe.
    """
    return set(session.execute(_CONSERVATION_SCREEN_SQL).scalars())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k conservation -v`
Expected: 5 PASS

- [ ] **Step 5: Run the structural tests too (no regression)**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/aleph/repair.py tests/db/test_credit_repair.py
git commit -m "feat(repair): conservation screen for credit cache totals"
```

---

### Task 4: Order-inversion screen (S3)

**Files:**
- Modify: `src/aleph/repair.py`
- Test: `tests/db/test_credit_repair.py`

**Interfaces:**
- Consumes: `AlephCreditHistoryDb` (direct ORM inserts to control `last_update`).
- Produces (used by Task 5): `_find_order_inversions(session: DbSession, watermark: dt.datetime) -> Set[str]`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_credit_repair.py` (add `_find_order_inversions` to the `aleph.repair` import):

```python
def _history_row(address, amount, ref, ts, last_update, index=0, expiration=None):
    """Raw history row with a controlled insertion time (last_update)."""
    return AlephCreditHistoryDb(
        address=address,
        amount=amount,
        credit_ref=ref,
        credit_index=index,
        expiration_date=expiration,
        message_timestamp=ts,
        last_update=last_update,
    )


T1 = dt.datetime(2026, 3, 1, 10, 0, 0, tzinfo=dt.timezone.utc)
T2 = dt.datetime(2026, 3, 1, 11, 0, 0, tzinfo=dt.timezone.utc)
T3 = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
MT_EARLY = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
MT_MID = dt.datetime(2026, 2, 10, tzinfo=dt.timezone.utc)
MT_LATE = dt.datetime(2026, 2, 20, tzinfo=dt.timezone.utc)


def test_inversion_screen_flags_late_grant_before_applied_drain(
    session_factory: DbSessionFactory,
):
    """A grant inserted (T3) after a drain (T2) it replay-precedes (MT_MID <
    MT_LATE) is an inversion: the drain was applied without seeing it."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", 200, "g2", MT_MID, T3))  # late arrival
        session.commit()

        assert _find_order_inversions(session, watermark=T2) == {"0xaaa"}


def test_inversion_screen_respects_watermark(session_factory: DbSessionFactory):
    """The same inversion is NOT re-flagged once the watermark has passed the
    late row's insertion time (a prior repair already reconciled it)."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", 200, "g2", MT_MID, T3))
        session.commit()

        assert _find_order_inversions(session, watermark=T3) == set()


def test_inversion_screen_ignores_grant_grant_inversions(
    session_factory: DbSessionFactory,
):
    """Two grants arriving out of order cannot change the final state (lots
    are independent until a drain is involved); must NOT be flagged."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_MID, T1))
        session.add(_history_row("0xaaa", 200, "g2", MT_EARLY, T2))  # late, grant
        session.commit()

        assert _find_order_inversions(session, watermark=T1) == set()


def test_inversion_screen_flags_backdated_drain(session_factory: DbSessionFactory):
    """A drain inserted late with an early timestamp replay-precedes an
    already-applied drain — flag it (rebuild decides if state differs)."""
    with session_factory() as session:
        session.add(_history_row("0xaaa", 1000, "g1", MT_EARLY, T1))
        session.add(_history_row("0xaaa", -500, "e1", MT_LATE, T2))
        session.add(_history_row("0xaaa", -100, "e2", MT_MID, T3))  # backdated
        session.commit()

        assert _find_order_inversions(session, watermark=T2) == {"0xaaa"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k inversion -v`
Expected: FAIL with `ImportError: cannot import name '_find_order_inversions'`

- [ ] **Step 3: Implement the screen**

In `src/aleph/repair.py`, add below `_find_conservation_violations` (add `import datetime as dt` at the top of the file):

```python
# The eager writers apply rows in arrival order; the replay applies them in
# (message_timestamp, credit_ref, credit_index) order. The two agree unless a
# row was inserted after a row it replay-precedes AND a drain is involved
# (grant/grant order never changes the outcome; drains pick lots by replay
# order, so a late grant can retroactively change what an already-applied
# drain should have consumed, and vice versa). credit_history.id is never
# populated (not in the bulk-insert column list, no sequence), so insertion
# order is proxied by last_update, set once per writer call. Rows at or below
# the watermark were reconciled by a previous repair run.
_INVERSION_SCREEN_SQL = text(
    """
    SELECT DISTINCT late.address
    FROM credit_history late
    JOIN credit_history early
      ON early.address = late.address
     AND early.last_update < late.last_update
     AND (early.message_timestamp, early.credit_ref, early.credit_index)
         > (late.message_timestamp, late.credit_ref, late.credit_index)
     AND (late.amount < 0 OR early.amount < 0)
    WHERE late.last_update > :watermark
    """
)


def _find_order_inversions(session: DbSession, watermark: dt.datetime) -> Set[str]:
    """Addresses with out-of-order credit rows inserted since ``watermark``.

    Pure read; over-flagging is safe (the rebuild replay is the fixpoint).
    """
    return set(
        session.execute(_INVERSION_SCREEN_SQL, {"watermark": watermark}).scalars()
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k inversion -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add src/aleph/repair.py tests/db/test_credit_repair.py
git commit -m "feat(repair): out-of-order arrival screen for credit history"
```

---

### Task 5: Rewire `_repair_credit_balances` around the screens

**Files:**
- Modify: `src/aleph/repair.py:201-223` (`_repair_credit_balances`)
- Test: `tests/db/test_credit_repair.py`

**Interfaces:**
- Consumes: `_find_structural_violations`, `_find_conservation_violations`, `_find_order_inversions`, `_rebuild_credit_lots_for_address` (unchanged), `get_credit_repair_state`, `upsert_credit_repair_state`, `CreditRepairStateDb`.
- Produces: `_repair_credit_balances(session_factory: DbSessionFactory) -> None` (same signature as today — `repair_node` needs no change) and module constant `REPAIR_POLICY_VERSION: int = 1`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_credit_repair.py` (add `REPAIR_POLICY_VERSION, _repair_credit_balances` to the `aleph.repair` import; add `func` to the `sqlalchemy` import):

```python
def _seed_drained_and_live(session):
    """0xaaa: fully drained lot (zero row left by the eager writer — its
    presence/absence distinguishes a skip from a rebuild). 0xbbb: live lot."""
    _grant(session, "0xaaa", 1000, "grant_a", TS_GRANT)
    _spend(session, "0xaaa", 1000, "spend_a", TS_SPEND)
    _grant(session, "0xbbb", 500, "grant_b", TS_GRANT)


def _zero_row_count(session, address):
    return session.execute(
        select(func.count())
        .select_from(AlephCreditBalanceDb)
        .where(
            AlephCreditBalanceDb.address == address,
            AlephCreditBalanceDb.amount_remaining == 0,
        )
    ).scalar_one()


def _write_current_state(session):
    """State row as a previous successful repair would have left it."""
    max_last_update = session.execute(
        select(func.max(AlephCreditHistoryDb.last_update))
    ).scalar_one()
    upsert_credit_repair_state(
        session,
        policy_version=REPAIR_POLICY_VERSION,
        history_watermark=max_last_update,
        last_run=max_last_update,
    )


def test_repair_skips_when_clean(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        _write_current_state(session)
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # Skip, not rebuild: the eager writer's zero row survived (a rebuild
        # would have purged it).
        assert _zero_row_count(session, "0xaaa") == 1
        # State row refreshed.
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION


def test_repair_bootstrap_rebuilds_everything(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        session.commit()  # no state row: bootstrap

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # Full rebuild purges the zero row and creates the state row.
        assert _zero_row_count(session, "0xaaa") == 0
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION
        max_last_update = session.execute(
            select(func.max(AlephCreditHistoryDb.last_update))
        ).scalar_one()
        assert state.history_watermark == max_last_update


def test_repair_full_rebuild_on_policy_version_change(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        _seed_drained_and_live(session)
        max_last_update = session.execute(
            select(func.max(AlephCreditHistoryDb.last_update))
        ).scalar_one()
        upsert_credit_repair_state(
            session,
            policy_version=REPAIR_POLICY_VERSION - 1,  # stale policy
            history_watermark=max_last_update,
            last_run=max_last_update,
        )
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        assert _zero_row_count(session, "0xaaa") == 0  # rebuilt
        state = get_credit_repair_state(session)
        assert state is not None
        assert state.policy_version == REPAIR_POLICY_VERSION


def test_repair_rebuilds_only_flagged_addresses(session_factory: DbSessionFactory):
    with session_factory() as session:
        _seed_drained_and_live(session)
        _write_current_state(session)
        # Corrupt only 0xbbb: over-drain its lot (S2 flags it).
        session.execute(
            update(AlephCreditBalanceDb)
            .where(AlephCreditBalanceDb.address == "0xbbb")
            .values(amount_remaining=0)
        )
        session.commit()

    _repair_credit_balances(session_factory)

    with session_factory() as session:
        # 0xbbb was rebuilt back to its full grant...
        bbb_remaining = session.execute(
            select(func.sum(AlephCreditBalanceDb.amount_remaining)).where(
                AlephCreditBalanceDb.address == "0xbbb"
            )
        ).scalar_one()
        bbb_granted = session.execute(
            select(AlephCreditHistoryDb.amount).where(
                AlephCreditHistoryDb.credit_ref == "grant_b"
            )
        ).scalar_one()
        assert bbb_remaining == bbb_granted
        # ...while 0xaaa was left alone (zero row survived).
        assert _zero_row_count(session, "0xaaa") == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -k "repair_skips or bootstrap or policy_version or flagged" -v`
Expected: FAIL — `ImportError: cannot import name 'REPAIR_POLICY_VERSION'`

- [ ] **Step 3: Implement**

In `src/aleph/repair.py`: add imports `from aleph.db.accessors.balances import get_credit_repair_state, upsert_credit_repair_state` (keep the existing import block style). Ensure `import datetime as dt` is present at the top (added in Task 4; add it here if executing this task standalone). Add near the top of the module (after `LOGGER`):

```python
# Version of the lot-cache drain policy implemented by
# _rebuild_credit_lots_for_address and the eager writers in
# aleph.db.accessors.balances. Bump whenever the drain semantics change (order,
# expiration handling, ...): a mismatch with the stored
# credit_repair_state.policy_version forces a full rebuild on the next boot.
REPAIR_POLICY_VERSION = 1

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
```

Replace `_repair_credit_balances` (currently `src/aleph/repair.py:201-223`) with:

```python
def _repair_credit_balances(session_factory: DbSessionFactory) -> None:
    """Bootstrap or repair the credit_balances lot cache from credit_history.

    The cache is written in the same transaction as the append-only history,
    so drift can only come from out-of-order message arrival, a drain-policy
    code change, or a writer bug. Cheap set-based screens detect each class
    and only flagged addresses get the per-address delete-and-replay; a clean
    boot performs no cache writes. A full rebuild happens only on bootstrap
    (no ``credit_repair_state`` row) or when ``REPAIR_POLICY_VERSION``
    changed. Known residual: a total-preserving misattribution bug (right
    total drained from the wrong lot) is invisible to the screens until its
    effects change a total.

    Crash-safe: the state row is only advanced after all rebuilds committed;
    a crash mid-repair re-screens with the old watermark next boot and
    re-rebuilds (idempotent).
    """
    with session_factory() as session:
        state = get_credit_repair_state(session)
        new_watermark = (
            session.execute(
                select(func.max(AlephCreditHistoryDb.last_update))
            ).scalar()
            or _EPOCH
        )

    if state is None or state.policy_version != REPAIR_POLICY_VERSION:
        reason = (
            "bootstrap"
            if state is None
            else f"policy version {state.policy_version} -> {REPAIR_POLICY_VERSION}"
        )
        with session_factory() as session:
            flagged = set(
                session.execute(
                    select(AlephCreditHistoryDb.address).distinct()
                ).scalars()
            )
        LOGGER.info(
            "Credit balances: full rebuild (%s), %d address(es)",
            reason,
            len(flagged),
        )
    else:
        with session_factory() as session:
            structural = _find_structural_violations(session)
            conservation = _find_conservation_violations(session)
            inversions = _find_order_inversions(session, state.history_watermark)
        flagged = structural | conservation | inversions
        if flagged:
            LOGGER.warning(
                "Credit cache drift detected (%d structural, %d conservation, "
                "%d out-of-order): rebuilding %d address(es), e.g. %s",
                len(structural),
                len(conservation),
                len(inversions),
                len(flagged),
                sorted(flagged)[:10],
            )
        else:
            LOGGER.info("Credit balances clean, nothing to repair")

    for i, address in enumerate(sorted(flagged)):
        with session_factory() as session:
            _rebuild_credit_lots_for_address(session, address)
            session.commit()
        if (i + 1) % 500 == 0:
            LOGGER.info("Repaired %d / %d", i + 1, len(flagged))

    with session_factory() as session:
        upsert_credit_repair_state(
            session,
            policy_version=REPAIR_POLICY_VERSION,
            history_watermark=new_watermark,
            last_run=utc_now(),
        )
        session.commit()

    if flagged:
        LOGGER.info(
            "Credit balances repair complete (%d address(es) rebuilt)", len(flagged)
        )
```

`repair_node` is untouched — it already calls `_repair_credit_balances(session_factory)`.

- [ ] **Step 4: Run the whole new test file**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py -v`
Expected: all PASS (18 tests)

- [ ] **Step 5: Run the pre-existing credit tests (regression)**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_balances.py -v`
Expected: all PASS (this file imports `_rebuild_credit_lots_for_address`, which is unchanged)

- [ ] **Step 6: Commit**

```bash
git add src/aleph/repair.py tests/db/test_credit_repair.py
git commit -m "feat(repair): skip credit balances rebuild when screens find no drift"
```

---

### Task 6: Lint, full verification

**Files:**
- Possibly modified by formatters: any of the above.

- [ ] **Step 1: Format**

Run: `hatch run linting:fmt` (use `$HATCH` if `hatch` is not on PATH)
Expected: exits 0; may reformat files.

- [ ] **Step 2: Lint**

Run: `hatch run linting:all`
Expected: ruff/black/isort pass. If the mypy step aborts with the SQLAlchemy-stubs plugin conflict, verify it reproduces on the untouched base (`git stash && hatch run linting:all; git stash pop`) and note it as pre-existing.

- [ ] **Step 3: Re-run both credit test files**

Run: `PYTHONPATH=$PWD/src $ENVPY -m pytest tests/db/test_credit_repair.py tests/db/test_credit_balances.py -v`
Expected: all PASS

- [ ] **Step 4: Commit any formatter fallout**

```bash
git add -u
git commit -m "style: formatter fixes for credit repair screens" || echo "nothing to commit"
```

---

## Self-Review Notes

- **Spec coverage:** policy-version full rebuild (Task 5), S1/S2/S3 screens (Tasks 2–4), skip-when-clean with WARNING-level drift reporting (Task 5), state persistence + watermark (Tasks 1, 5). Residual blind spot (total-preserving misattribution) documented in the `_repair_credit_balances` docstring rather than papered over. The optional "rotating per-boot sample" from the discussion is deliberately left out (YAGNI — add later if a bug of that class ever surfaces).
- **Type consistency:** all three screens return `Set[str]`; `_repair_credit_balances` keeps its `(session_factory: DbSessionFactory) -> None` signature so `repair_node` and `commands.py` need no changes.
- **Ordering caveat (documented in S3 SQL comment):** rows written by the same writer call share one `last_update`, so an inversion *within* a single call is invisible — but a single call writes one message, whose rows the writer applies in replay order already.
