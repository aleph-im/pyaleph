"""
Runs the credit fold on a live node: decides which hourly periods can be
closed, folds them one transaction per period, refolds dirty periods, loads the
genesis snapshot and verifies the latest checkpoint.

The wall clock (`now`) only decides *when* a period is folded (the attestation
grace); nothing it computes depends on it (spec 5.2).

Genesis rows are not period inputs (`collect_inputs` does not produce them):
they come from the genesis STORE file, loaded once. A refold over the genesis
period would delete them, so `refold_keeping_genesis` carries them over and
re-applies them first in the same transaction. Genesis comes first in kind
order, so their lower ids keep the (period, id) replay order of `refold_from`
consistent with the order the fold would have written them in.
"""

import datetime as dt
import logging
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Context, Decimal
from typing import Any, Dict, List, Optional, Protocol, Tuple

from aleph_message.models import ItemHash, ItemType
from configmanager import Config
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from aleph.db.accessors.credit_ledger import (
    genesis_rows_from,
    get_fold_state,
    latest_checkpoint,
    mark_dirty,
    selected_attestation,
)
from aleph.db.models.credit_ledger import CreditPeriodQuoteDb
from aleph.services.credits.addresses import normalize_address
from aleph.services.credits.fold import (
    GLOBAL_ADDRESS,
    GenesisInput,
    PeriodInputs,
    apply_period,
    period_hash,
    refold_from,
    write_checkpoint,
)
from aleph.services.credits.inputs import AttestationPending, collect_inputs
from aleph.services.credits.periods import PERIOD, floor_hour, period_end
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)

ONE_ALEPH = 10**18
SLOT_SECONDS = 12
QUOTE_PLACES = Decimal("1e-18")
QUOTE_CONTEXT = Context(prec=80, rounding=ROUND_FLOOR)


def _from_unix(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    period = dt.datetime.fromtimestamp(int(value), tz=dt.timezone.utc)
    if period != floor_hour(period):
        raise ValueError(f"credit period {value} is not aligned on an hour")
    return period


@dataclass(frozen=True)
class FoldConfig:
    start_period: Optional[dt.datetime]  # credits.attestation.start_period
    genesis_period: Optional[dt.datetime]  # credits.genesis.period
    genesis_hash: Optional[str]  # credits.genesis.hash
    grace_seconds: int = 10800
    checkpoint_every_hours: int = 24

    @property
    def first_period(self) -> Optional[dt.datetime]:
        """The first period the fold closes: the latest configured start."""
        periods = [p for p in (self.start_period, self.genesis_period) if p]
        return max(periods) if periods else None

    @property
    def grace(self) -> dt.timedelta:
        return dt.timedelta(seconds=self.grace_seconds)

    @classmethod
    def from_config(cls, config: Config) -> "FoldConfig":
        credits = config.aleph.credits
        return cls(
            start_period=_from_unix(credits.attestation.start_period.value),
            genesis_period=_from_unix(credits.genesis.period.value),
            genesis_hash=credits.genesis.hash.value or None,
            grace_seconds=int(credits.attestation.grace_seconds.value),
            checkpoint_every_hours=int(credits.fold.checkpoint_every_hours.value),
        )


def _grace_elapsed(period_start: dt.datetime, now: dt.datetime, cfg: FoldConfig):
    return now >= period_end(period_start) + cfg.grace


def period_is_closable(
    session: DbSession, period_start: dt.datetime, now: dt.datetime, cfg: FoldConfig
) -> Tuple[bool, Optional[str]]:
    """
    (closable, blocker): the chain cursor has reached the end of the period,
    and the period has a confirmed attestation or its grace has elapsed on this
    node's clock. The blocker is "chain" or "attestation".
    """
    state = get_fold_state(session)
    end = period_end(period_start)
    if state.chain_cursor_time is None or state.chain_cursor_time < end:
        return False, "chain"
    if selected_attestation(session, period_start) is None and not _grace_elapsed(
        period_start, now, cfg
    ):
        return False, "attestation"
    return True, None


def _is_checkpoint_period(period_start: dt.datetime, cfg: FoldConfig) -> bool:
    hours = int(period_start.timestamp()) // 3600
    return hours % max(cfg.checkpoint_every_hours, 1) == 0


def _genesis_pending(session: DbSession, cfg: FoldConfig) -> bool:
    if not cfg.genesis_hash or cfg.genesis_period is None:
        return False
    return get_fold_state(session).genesis_loaded_hash != cfg.genesis_hash


# --- refold -------------------------------------------------------------------


def refold_keeping_genesis(session: DbSession, period_start: dt.datetime) -> None:
    """
    `refold_from(period_start)`, then re-apply the genesis rows it deleted, in
    the same transaction (see the module docstring).
    """
    carried: Dict[dt.datetime, List[GenesisInput]] = {}
    for row in genesis_rows_from(session, period_start):
        if row.address is None:
            raise ValueError(f"genesis row {row.business_key} has no address")
        carried.setdefault(row.period, []).append(
            GenesisInput(
                address=row.address,
                credits=int(row.amount),
                business_key=row.business_key,
                expiration=row.expiration,
                source_ref=row.source_ref,
            )
        )
    refold_from(session, period_start)
    for period in sorted(carried):
        apply_period(session, period, PeriodInputs(genesis=carried[period]))


# --- quotes -------------------------------------------------------------------


class _Indexer(Protocol):
    web3: Any
    cfg: Any

    async def quote(
        self, token: str, raw_amount: int, block_number: int, apply_bonus: bool = True
    ) -> Tuple[int, int]: ...


async def _block_time(web3: Any, number: int) -> dt.datetime:
    block = await web3.eth.get_block(number)
    return dt.datetime.fromtimestamp(int(block["timestamp"]), tz=dt.timezone.utc)


async def _last_block_until(
    web3: Any, cursor_block: int, cursor_time: dt.datetime, end: dt.datetime
) -> int:
    """Number of the last block with time <= end, below the cursor block."""
    if cursor_time <= end:
        return cursor_block
    hi = cursor_block  # time > end
    lo = cursor_block
    lo_time = cursor_time
    # One slot per block at most, so stepping back by the slots in the gap
    # lands at or before the answer; missed slots only make it land earlier.
    while lo_time > end:
        gap = int((lo_time - end).total_seconds())
        lo = max(lo - max(-(-gap // SLOT_SECONDS), 1), 0)
        lo_time = await _block_time(web3, lo)
        if lo == 0 and lo_time > end:
            raise ValueError(f"no block at or before {end.isoformat()}")
        if lo_time > end:
            hi = lo
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if await _block_time(web3, mid) <= end:
            lo = mid
        else:
            hi = mid
    return lo


async def store_period_quote(
    session: DbSession, period_start: dt.datetime, indexer: _Indexer
) -> Optional[Decimal]:
    """
    Record the ALEPH price of the period, without bonus, at the last finalized
    block of the period, as ALEPH per credit. Informational only (never a fold
    input): an RPC failure is logged and stores nothing.
    """
    end = period_end(period_start)
    state = get_fold_state(session)
    if state.chain_cursor_block is None or state.chain_cursor_time is None:
        LOGGER.warning("No chain cursor: no quote for %s", period_start.isoformat())
        return None
    try:
        block = await _last_block_until(
            indexer.web3, state.chain_cursor_block, state.chain_cursor_time, end
        )
        num, den = await indexer.quote(
            indexer.cfg.aleph, ONE_ALEPH, block, apply_bonus=False
        )
    except Exception as error:
        LOGGER.warning(
            "Could not quote ALEPH for period %s: %s", period_start.isoformat(), error
        )
        return None
    if num <= 0 or den <= 0:
        LOGGER.warning(
            "Invalid ALEPH quote %s/%s for period %s", num, den, period_start
        )
        return None
    # quote() gives credits per raw unit; ALEPH per credit = den / (num * 1e18).
    aleph_per_credit = QUOTE_CONTEXT.divide(
        Decimal(den), QUOTE_CONTEXT.multiply(Decimal(num), Decimal(ONE_ALEPH))
    ).quantize(QUOTE_PLACES, rounding=ROUND_FLOOR, context=QUOTE_CONTEXT)
    stmt = insert(CreditPeriodQuoteDb).values(
        period=period_start, aleph_per_credit=aleph_per_credit, block_number=block
    )
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=["period"],
            set_={
                "aleph_per_credit": stmt.excluded.aleph_per_credit,
                "block_number": stmt.excluded.block_number,
            },
        )
    )
    return aleph_per_credit


# At most this many closed periods without a quote are re-quoted per fold run,
# oldest first, over the whole closed range: an older hole is never left behind
# (it would truncate /credits/revenue for good).
REQUOTE_WINDOW_PERIODS = 168


def unquoted_closed_periods(
    session: DbSession,
    first_period: dt.datetime,
    last_closed: dt.datetime,
    limit: int = REQUOTE_WINDOW_PERIODS,
) -> List[dt.datetime]:
    """
    The oldest `limit` closed periods in `[first_period, last_closed]` without
    a `credit_period_quotes` row, oldest first.
    """
    if first_period > last_closed:
        return []
    series = select(
        func.generate_series(first_period, last_closed, PERIOD).column_valued("period")
    ).subquery()
    stmt = (
        select(series.c.period)
        .where(
            ~select(CreditPeriodQuoteDb.period)
            .where(CreditPeriodQuoteDb.period == series.c.period)
            .exists()
        )
        .order_by(series.c.period)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars())


async def requote_closed_periods(
    session_factory: DbSessionFactory, indexer: _Indexer, cfg: FoldConfig
) -> int:
    """
    Quote the closed periods whose quote failed when they were closed (RPC
    outage), the oldest `REQUOTE_WINDOW_PERIODS` of them per run. Each
    period is quoted in its own short transaction; a failure is logged and
    skipped. Returns the number of periods quoted.
    """
    first = cfg.first_period
    if first is None:
        return 0
    with session_factory() as session:
        last_closed = get_fold_state(session).last_closed_period
        if last_closed is None:
            return 0
        missing = unquoted_closed_periods(session, first, last_closed)
    quoted = 0
    for period in missing:
        try:
            with session_factory() as session:
                if await store_period_quote(session, period, indexer) is not None:
                    session.commit()
                    quoted += 1
        except Exception:
            LOGGER.exception("Re-quoting credit period %s failed", period.isoformat())
    return quoted


# --- closing periods ----------------------------------------------------------


def _refold_target(dirty: dt.datetime, cfg: FoldConfig) -> dt.datetime:
    # Nothing before the configured start is ever folded by the close loop.
    first = cfg.first_period
    return dirty if first is None else max(dirty, first)


async def close_ready_periods(
    session_factory: DbSessionFactory,
    now: dt.datetime,
    settings: Config,
    cfg: FoldConfig,
    indexer: Optional[_Indexer] = None,
) -> int:
    """
    Refold the dirty periods, then close every closable period after the last
    closed one, in order, one transaction per period. Returns the number of
    periods closed (refolded periods included).

    Every transaction starts by locking the fold state row FOR UPDATE, before
    it reads any input: it waits for every open transaction holding the KEY
    SHARE lock `mark_dirty` takes, so their inputs are committed before the
    fold reads, and a later `mark_dirty` sees the new `last_closed_period`. A
    dirty mark found under the lock is refolded first.
    """
    first = cfg.first_period
    if first is None:
        LOGGER.info("Credit fold has no configured start period: nothing to fold")
        return 0

    closed = 0
    while True:
        with session_factory() as session:
            state = get_fold_state(session, for_update=True)
            if state.dirty_from_period is not None:
                dirty = state.dirty_from_period
                target = _refold_target(dirty, cfg)
                LOGGER.info(
                    "Refolding credit ledger from %s (dirty from %s)",
                    target.isoformat(),
                    dirty.isoformat(),
                )
                refold_keeping_genesis(session, target)
                session.commit()
                continue
            period = (
                first
                if state.last_closed_period is None
                else state.last_closed_period + PERIOD
            )
            if (
                cfg.genesis_period is not None
                and period >= cfg.genesis_period
                and _genesis_pending(session, cfg)
            ):
                LOGGER.warning(
                    "Credit fold waits for genesis %s to be loaded (loaded: %s)",
                    cfg.genesis_hash,
                    state.genesis_loaded_hash,
                )
                break
            closable, blocker = period_is_closable(session, period, now, cfg)
            if not closable:
                LOGGER.debug(
                    "Credit period %s not closable yet (%s)",
                    period.isoformat(),
                    blocker,
                )
                break
            try:
                inputs = collect_inputs(
                    session,
                    period,
                    settings,
                    attestation_grace_elapsed=_grace_elapsed(period, now, cfg),
                )
            except AttestationPending:
                LOGGER.info("Credit period %s waits for its attestation", period)
                break
            result = apply_period(session, period, inputs)
            if _is_checkpoint_period(period, cfg):
                write_checkpoint(session, period)
            state.last_closed_period = period
            session.commit()
        LOGGER.info(
            "Closed credit period %s: %d rows, hash %s",
            period.isoformat(),
            result.rows_written,
            result.period_hash,
        )
        closed += 1
        if indexer is not None:
            # Informational, and awaits RPC calls: its own short transaction,
            # after the period commit, so no lock is held across the network.
            with session_factory() as session:
                if await store_period_quote(session, period, indexer) is not None:
                    session.commit()
    return closed


# --- genesis ------------------------------------------------------------------


def _parse_genesis(
    entries: Any, genesis_hash: str, genesis_period: dt.datetime
) -> List[GenesisInput]:
    if not isinstance(entries, list):
        raise ValueError(f"genesis {genesis_hash} is not a JSON list")
    inputs: List[GenesisInput] = []
    for index, entry in enumerate(entries):
        amount = int(entry["amount"])
        if amount <= 0:
            raise ValueError(f"genesis {genesis_hash}[{index}]: amount {amount}")
        expiration = entry.get("expiration")
        inputs.append(
            GenesisInput(
                address=normalize_address(str(entry["address"])),
                credits=amount,
                business_key=f"{genesis_hash}:{index}",
                expiration=(
                    None
                    if expiration is None
                    else dt.datetime.fromtimestamp(int(expiration), tz=dt.timezone.utc)
                ),
                source_ref=entry.get("ref"),
            )
        )
    return inputs


async def load_genesis(
    session: DbSession,
    storage_service: Any,
    genesis_hash: str,
    genesis_period: dt.datetime,
) -> int:
    """
    Write the genesis snapshot (a JSON list of {address, amount, expiration
    (unix seconds or null), ref}) as genesis rows of `genesis_period`, keyed
    `{genesis_hash}:{index}`. Idempotent. Returns the number of rows written.
    Marks the genesis period dirty when it is already closed.

    Refuses (logs an ERROR, writes nothing, returns 0) when a different genesis
    is already loaded: its rows would be credited on top of the old ones.
    Replacing a genesis means clearing the ledger explicitly.
    """
    loaded = get_fold_state(session, for_update=True).genesis_loaded_hash
    if loaded is not None and loaded != genesis_hash:
        LOGGER.error(
            "Credit genesis %s is configured but genesis %s is already loaded: "
            "refusing to load it (clear the ledger to replace a genesis)",
            genesis_hash,
            loaded,
        )
        return 0
    try:
        engine = ItemHash(genesis_hash).item_type
    except ValueError:
        engine = ItemType.ipfs
    content = await storage_service.get_json(
        genesis_hash, engine=engine, timeout=30, tries=3
    )
    inputs = _parse_genesis(content.value, genesis_hash, genesis_period)
    result = apply_period(session, genesis_period, PeriodInputs(genesis=inputs))
    state = get_fold_state(session)
    state.genesis_loaded_hash = genesis_hash
    mark_dirty(session, genesis_period)
    session.flush()
    LOGGER.info(
        "Loaded credit genesis %s: %d of %d rows written",
        genesis_hash,
        result.rows_written,
        len(inputs),
    )
    return result.rows_written


# --- verification -------------------------------------------------------------


def verify_latest_checkpoint(
    session_factory: DbSessionFactory,
    settings: Optional[Config] = None,
    cfg: Optional[FoldConfig] = None,
) -> bool:
    """
    Re-fold the periods since the checkpoint before the latest one (or since
    the configured start) in a transaction that is rolled back, and compare
    every period hash and the latest checkpoint's global hash. On a mismatch,
    mark the interval dirty so the next fold run rebuilds it. Returns True when
    everything matches or there is nothing to verify.
    """
    if settings is None:
        from aleph.config import get_config

        settings = get_config()
    if cfg is None:
        cfg = FoldConfig.from_config(settings)

    with session_factory() as session:
        # Locked for the whole (rolled back) re-fold, like any fold transaction.
        state = get_fold_state(session, for_update=True)
        last_closed = state.last_closed_period
        if last_closed is None:
            return True
        latest = latest_checkpoint(session, GLOBAL_ADDRESS, before=last_closed + PERIOD)
        if latest is None:
            LOGGER.info("No credit checkpoint to verify")
            return True
        previous = latest_checkpoint(session, GLOBAL_ADDRESS, before=latest.period)
        start = previous.period + PERIOD if previous is not None else cfg.first_period
        if start is None:
            return True
        # Read before the refold deletes the checkpoint rows.
        latest_period, expected_global = latest.period, latest.ledger_hash

        periods: List[dt.datetime] = []
        period = start
        while period <= latest_period:
            periods.append(period)
            period += PERIOD
        expected = {p: period_hash(session, p) for p in periods}

        mismatches: List[dt.datetime] = []
        refold_keeping_genesis(session, start)
        for period in periods:
            inputs = collect_inputs(
                session, period, settings, attestation_grace_elapsed=True
            )
            apply_period(session, period, inputs)
            if period_hash(session, period) != expected[period]:
                mismatches.append(period)
        global_hash = write_checkpoint(session, latest_period)
        session.rollback()

    if not mismatches and global_hash == expected_global:
        LOGGER.info("Credit checkpoint %s verified", latest_period.isoformat())
        return True

    LOGGER.error(
        "Credit checkpoint %s does not match a re-fold (periods %s); "
        "re-folding from %s",
        latest_period.isoformat(),
        [p.isoformat() for p in mismatches],
        start.isoformat(),
    )
    with session_factory() as session:
        mark_dirty(session, start)
        session.commit()
    return False
