"""
Fold core of the credit ledger: turns the inputs of one hourly period into
`credit_ledger` rows and keeps the `credit_lots` FIFO cache in step.

Every CCN must produce identical rows and hashes from identical inputs, so this
module never reads the wall clock, never iterates over a dict or a set to
order rows, and never uses floats. Credits are integers; execution amounts are
`ceil(rate * seconds)` computed with a fixed Decimal context.

Within a period rows are applied in kind order (`KIND_ORDER`): genesis,
top-ups, transfers (each transfer writes its `transfer_out` then its
`transfer_in` rows before the next transfer is looked at, in the order the
caller gives, which must be confirmation time then item hash), executions,
storage, then exclusion rows. Lots are drained in `(period, kind order,
business_key)` order, where business keys compare by code point.
"""

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Context, Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from aleph_message.models import PaymentType

from aleph.db.accessors.credit_ledger import (
    checkpoint_rows,
    delete_all_lots,
    delete_checkpoints_from,
    delete_ledger_rows_from,
    get_fold_state,
    get_ledger_rows_by_ids,
    has_ledger_rows_before,
    insert_ledger_row,
    insert_lot,
    latest_checkpoint,
    ledger_addresses_between,
    ledger_row_exists,
    ledger_rows,
    lock_valid_lots,
    lots_with_remaining,
    set_lot_remaining,
    upsert_checkpoint,
)
from aleph.services.credits.periods import KIND_ORDER, PERIOD
from aleph.types.db_session import DbSession

# Kinds that create a lot / drain lots. hold_equivalent does neither.
CREDIT_KINDS = ("genesis", "topup", "transfer_in")
DEBIT_KINDS = ("transfer_out", "execution", "storage")

GLOBAL_ADDRESS = ""
INSUFFICIENT_BALANCE = "insufficient_balance"

# rate is decimal(40, 18) and seconds fit in an int: 80 digits never round.
AMOUNT_CONTEXT = Context(prec=80, rounding=ROUND_CEILING)


@dataclass(frozen=True)
class GenesisInput:
    address: str
    credits: int
    business_key: str  # {snapshot_hash}:{index}
    expiration: Optional[dt.datetime] = None
    source_ref: Optional[str] = None


@dataclass(frozen=True)
class TopupInput:
    address: str
    credits: int
    business_key: str  # {chain_id}:{tx_hash}:{log_index}
    source_ref: Optional[str]
    expiration: Optional[dt.datetime] = None


@dataclass(frozen=True)
class TransferInput:
    message_hash: str
    sender: str
    # (recipient, amount, requested expiration)
    entries: List[Tuple[str, int, Optional[dt.datetime]]]


@dataclass(frozen=True)
class ExecutionInput:
    vm: str
    node: str
    owner: str
    payment_type: str
    seconds: int
    rate: Optional[Decimal]
    excluded_reason: Optional[str] = None


@dataclass(frozen=True)
class StorageInput:
    address: str
    amount: int  # positive: the credits charged
    count: int
    size_mib: int
    seconds: int


@dataclass(frozen=True)
class ExclusionInput:
    kind: str
    business_key: str  # already formed by the caller, per the kind's key shape
    address: Optional[str]
    resource: Optional[str]
    node_id: Optional[str]
    reason: str


@dataclass
class PeriodInputs:
    topups: List[TopupInput] = field(default_factory=list)
    transfers: List[TransferInput] = field(default_factory=list)
    executions: List[ExecutionInput] = field(default_factory=list)
    storage: List[StorageInput] = field(default_factory=list)
    exclusions: List[ExclusionInput] = field(default_factory=list)
    genesis: List[GenesisInput] = field(default_factory=list)


@dataclass(frozen=True)
class PeriodResult:
    rows_written: int
    period_hash: str


@dataclass
class _Lot:
    ledger_id: int
    period: dt.datetime
    kind: str
    business_key: str
    amount_remaining: int
    expiration: Optional[dt.datetime]
    touched: bool = False

    def order_key(self) -> Tuple[dt.datetime, int, str]:
        return (self.period, KIND_ORDER[self.kind], self.business_key)


def _period_unix(period_start: dt.datetime) -> int:
    return int(period_start.timestamp())


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _iso(value: Optional[dt.datetime]) -> Optional[str]:
    return None if value is None else value.astimezone(dt.timezone.utc).isoformat()


def execution_amount(rate: Optional[Decimal], seconds: int) -> int:
    """ceil(rate * seconds) in whole credits, exact."""
    if rate is None:
        return 0
    product = AMOUNT_CONTEXT.multiply(rate, Decimal(seconds))
    return int(
        product.to_integral_value(rounding=ROUND_CEILING, context=AMOUNT_CONTEXT)
    )


# --- lots ---------------------------------------------------------------------


def _load_lots(
    session: DbSession, address: str, period_start: dt.datetime
) -> List[_Lot]:
    """Drainable lots of `address` at `period_start`, locked, in drain order."""
    lots = [
        _Lot(
            ledger_id=row.ledger_id,
            period=row.period,
            kind=row.kind,
            business_key=row.business_key,
            amount_remaining=row.amount_remaining,
            expiration=row.expiration,
        )
        for row in lock_valid_lots(session, address, period_start)
    ]
    lots.sort(key=_Lot.order_key)
    return lots


def _take(lots: List[_Lot], amount: int) -> List[Tuple[int, Optional[dt.datetime]]]:
    """
    Drain `amount` from `lots` in order, in memory. Returns (taken, lot
    expiration) per touched lot. Any excess over the lots is dropped.
    """
    taken: List[Tuple[int, Optional[dt.datetime]]] = []
    remaining = amount
    for lot in lots:
        if remaining <= 0:
            break
        if lot.amount_remaining <= 0:
            continue
        take = min(lot.amount_remaining, remaining)
        lot.amount_remaining -= take
        lot.touched = True
        remaining -= take
        taken.append((take, lot.expiration))
    return taken


def _store(session: DbSession, lots: Sequence[_Lot]) -> None:
    for lot in lots:
        if lot.touched:
            set_lot_remaining(session, lot.ledger_id, lot.amount_remaining)
            lot.touched = False


def _drain(
    session: DbSession, address: str, amount: int, period_start: dt.datetime
) -> None:
    if amount <= 0:
        return
    lots = _load_lots(session, address, period_start)
    _take(lots, amount)
    _store(session, lots)


def _expiration_buckets(
    taken: List[Tuple[int, Optional[dt.datetime]]],
    requested: Optional[dt.datetime],
) -> List[Tuple[int, Optional[dt.datetime]]]:
    """
    Cap each drained portion at min(source lot expiration, requested) and merge
    adjacent portions with the same result, as the legacy
    `_compute_transfer_entries_by_expiration` does.
    """
    buckets: List[Tuple[int, Optional[dt.datetime]]] = []
    for amount, source in taken:
        if source is None:
            effective = requested
        elif requested is None:
            effective = source
        else:
            effective = min(source, requested)
        if buckets and buckets[-1][1] == effective:
            buckets[-1] = (buckets[-1][0] + amount, effective)
        else:
            buckets.append((amount, effective))
    return buckets


# --- rows ---------------------------------------------------------------------


class _Writer:
    def __init__(self, session: DbSession, period_start: dt.datetime):
        self.session = session
        self.period_start = period_start
        self.rows_written = 0

    def exists(self, kind: str, business_key: str) -> bool:
        return ledger_row_exists(self.session, kind, business_key)

    def write(self, **values: Any) -> Optional[int]:
        """Insert a row of this period; None if (kind, business_key) exists."""
        values["period"] = self.period_start
        ledger_id = insert_ledger_row(self.session, values)
        if ledger_id is not None:
            self.rows_written += 1
        return ledger_id

    def credit(
        self,
        kind: str,
        address: str,
        business_key: str,
        amount: int,
        expiration: Optional[dt.datetime],
        source_ref: Optional[str],
    ) -> None:
        ledger_id = self.write(
            address=address,
            kind=kind,
            business_key=business_key,
            amount=amount,
            expiration=expiration,
            source_ref=source_ref,
        )
        if ledger_id is not None and amount > 0:
            insert_lot(
                self.session,
                ledger_id=ledger_id,
                address=address,
                period=self.period_start,
                kind=kind,
                business_key=business_key,
                amount_remaining=amount,
                expiration=expiration,
            )


def _apply_transfer(writer: _Writer, transfer: TransferInput) -> None:
    session, period_start = writer.session, writer.period_start
    out_key = f"{transfer.message_hash}:0"
    if writer.exists("transfer_out", out_key):
        return
    for _, amount, _ in transfer.entries:
        if amount <= 0:
            raise ValueError(
                f"transfer {transfer.message_hash}: non-positive amount {amount}"
            )
    total = sum(amount for _, amount, _ in transfer.entries)

    lots = _load_lots(session, transfer.sender, period_start)
    available = sum(lot.amount_remaining for lot in lots)
    if available < total:
        writer.write(
            address=transfer.sender,
            kind="transfer_out",
            business_key=out_key,
            amount=0,
            source_ref=transfer.message_hash,
            excluded_reason=INSUFFICIENT_BALANCE,
        )
        return

    credits: List[Tuple[str, int, Optional[dt.datetime]]] = []
    for recipient, amount, requested in transfer.entries:
        for bucket_amount, expiration in _expiration_buckets(
            _take(lots, amount), requested
        ):
            credits.append((recipient, bucket_amount, expiration))
    _store(session, lots)

    writer.write(
        address=transfer.sender,
        kind="transfer_out",
        business_key=out_key,
        amount=-total,
        source_ref=transfer.message_hash,
    )
    for index, (recipient, amount, expiration) in enumerate(credits, start=1):
        writer.credit(
            "transfer_in",
            recipient,
            f"{transfer.message_hash}:{index}",
            amount,
            expiration,
            transfer.message_hash,
        )


def _apply_execution(writer: _Writer, execution: ExecutionInput) -> None:
    payment_type = str(getattr(execution.payment_type, "value", execution.payment_type))
    kind = "hold_equivalent" if payment_type == PaymentType.hold.value else "execution"
    key = f"{_period_unix(writer.period_start)}:{execution.vm}:{execution.node}"
    if writer.exists(kind, key):
        return
    common: Dict[str, Any] = dict(
        address=execution.owner,
        kind=kind,
        business_key=key,
        resource=execution.vm,
        node_id=execution.node,
        seconds=execution.seconds,
        rate=execution.rate,
    )
    if execution.excluded_reason is not None:
        writer.write(amount=0, excluded_reason=execution.excluded_reason, **common)
        return
    if payment_type not in (PaymentType.credit.value, PaymentType.hold.value):
        raise ValueError(
            f"execution {execution.vm}: payment type {payment_type} needs an "
            "excluded_reason"
        )
    amount = execution_amount(execution.rate, execution.seconds)
    if kind == "hold_equivalent":
        writer.write(amount=amount, **common)
        return
    _drain(writer.session, execution.owner, amount, writer.period_start)
    writer.write(amount=-amount, **common)


def _apply_storage(writer: _Writer, storage: StorageInput) -> None:
    key = f"{_period_unix(writer.period_start)}:{storage.address}"
    if writer.exists("storage", key):
        return
    _drain(writer.session, storage.address, storage.amount, writer.period_start)
    writer.write(
        address=storage.address,
        kind="storage",
        business_key=key,
        amount=-storage.amount,
        seconds=storage.seconds,
        entry_count=storage.count,
        size_mib=storage.size_mib,
    )


def apply_period(
    session: DbSession, period_start: dt.datetime, inputs: PeriodInputs
) -> PeriodResult:
    """
    Write the ledger rows of one period and update the lots, in kind order.
    Re-applying the same inputs is a no-op: a row whose (kind, business_key)
    already exists is treated as applied and its lot effect is skipped.
    """
    writer = _Writer(session, period_start)
    period_unix = _period_unix(period_start)

    for genesis in sorted(inputs.genesis, key=lambda g: g.business_key):
        if not writer.exists("genesis", genesis.business_key):
            writer.credit(
                "genesis",
                genesis.address,
                genesis.business_key,
                genesis.credits,
                genesis.expiration,
                genesis.source_ref,
            )

    for topup in sorted(inputs.topups, key=lambda t: t.business_key):
        if not writer.exists("topup", topup.business_key):
            writer.credit(
                "topup",
                topup.address,
                topup.business_key,
                topup.credits,
                topup.expiration,
                topup.source_ref,
            )

    # Order matters (a transfer can spend credits received from an earlier one);
    # the caller orders transfers by confirmation time then item hash.
    for transfer in inputs.transfers:
        _apply_transfer(writer, transfer)

    for execution in sorted(
        inputs.executions, key=lambda e: f"{period_unix}:{e.vm}:{e.node}"
    ):
        _apply_execution(writer, execution)

    for storage in sorted(inputs.storage, key=lambda s: s.address):
        _apply_storage(writer, storage)

    for exclusion in sorted(
        inputs.exclusions, key=lambda x: (KIND_ORDER[x.kind], x.business_key)
    ):
        if not writer.exists(exclusion.kind, exclusion.business_key):
            writer.write(
                address=exclusion.address,
                kind=exclusion.kind,
                business_key=exclusion.business_key,
                amount=0,
                resource=exclusion.resource,
                node_id=exclusion.node_id,
                excluded_reason=exclusion.reason,
            )

    session.flush()
    return PeriodResult(
        rows_written=writer.rows_written,
        period_hash=period_hash(session, period_start),
    )


# --- hashes and checkpoints ---------------------------------------------------


def _rows_hash(rows: Sequence[Any]) -> str:
    """
    sha256 of the canonical JSON list of (address, kind, business_key, amount,
    node_id, seconds, excluded_reason), rows ordered by (kind order,
    business_key).
    """
    ordered = sorted(rows, key=lambda r: (KIND_ORDER[r.kind], r.business_key))
    return _canonical_hash(
        [
            [
                r.address,
                r.kind,
                r.business_key,
                int(r.amount),
                r.node_id,
                r.seconds,
                r.excluded_reason,
            ]
            for r in ordered
        ]
    )


def period_hash(session: DbSession, period_start: dt.datetime) -> str:
    """Hash of every ledger row of the period (an empty period hashes `[]`)."""
    return _rows_hash(
        ledger_rows(session, after=period_start - PERIOD, until=period_start)
    )


def _address_ledger_hash(
    session: DbSession, address: str, period_start: dt.datetime
) -> str:
    """
    sha256(previous checkpoint hash of the address, or "", + "|" + "|".join of
    "{period_unix}:{hash of the address's rows in that period}" for every
    period with rows since that checkpoint, up to period_start included).
    """
    previous = latest_checkpoint(session, address, before=period_start)
    rows = ledger_rows(
        session,
        after=previous.period if previous is not None else None,
        until=period_start,
        address=address,
    )
    by_period: Dict[dt.datetime, List[Any]] = {}
    for row in rows:
        by_period.setdefault(row.period, []).append(row)
    segments = [
        f"{_period_unix(period)}:{_rows_hash(by_period[period])}"
        for period in sorted(by_period)
    ]
    prefix = previous.ledger_hash if previous is not None else ""
    return _sha256(prefix + "|" + "|".join(segments))


def write_checkpoint(session: DbSession, period_start: dt.datetime) -> str:
    """
    Snapshot the lots after `period_start` was folded, with a per-address hash
    chain and a global hash (row address ''). Returns the global hash.

    Addresses written: those with a lot holding credit, plus those with ledger
    rows since the previous global checkpoint.
    """
    previous_global = latest_checkpoint(session, GLOBAL_ADDRESS, before=period_start)
    lots_by_address: Dict[str, List[Any]] = {}
    for lot in lots_with_remaining(session):
        lots_by_address.setdefault(lot.address, []).append(lot)

    addresses = set(lots_by_address)
    addresses.update(
        ledger_addresses_between(
            session,
            after=previous_global.period if previous_global is not None else None,
            until=period_start,
        )
    )

    address_hashes: List[str] = []
    for address in sorted(addresses):
        lots = sorted(
            lots_by_address.get(address, []),
            key=lambda lot: (lot.period, KIND_ORDER[lot.kind], lot.business_key),
        )
        ledger_hash = _address_ledger_hash(session, address, period_start)
        upsert_checkpoint(
            session,
            period=period_start,
            address=address,
            lots=[
                {
                    "ledger_id": lot.ledger_id,
                    "amount_remaining": int(lot.amount_remaining),
                    "expiration": _iso(lot.expiration),
                }
                for lot in lots
            ],
            ledger_hash=ledger_hash,
        )
        address_hashes.append(ledger_hash)

    global_hash = _sha256("|".join(address_hashes))
    upsert_checkpoint(
        session,
        period=period_start,
        address=GLOBAL_ADDRESS,
        lots=[],
        ledger_hash=global_hash,
    )
    session.flush()
    return global_hash


# --- refold -------------------------------------------------------------------


def _restore_checkpoint(session: DbSession, period: dt.datetime) -> None:
    entries: List[Tuple[str, Dict[str, Any]]] = []
    for checkpoint in checkpoint_rows(session, period):
        if checkpoint.address == GLOBAL_ADDRESS:
            continue
        for lot in checkpoint.lots:
            entries.append((checkpoint.address, lot))
    sources = get_ledger_rows_by_ids(session, (lot["ledger_id"] for _, lot in entries))
    for address, lot in entries:
        source = sources.get(lot["ledger_id"])
        if source is None or source.address != address:
            raise ValueError(
                f"checkpoint {period.isoformat()}: lot {lot['ledger_id']} of "
                f"{address} has no matching ledger row"
            )
        expiration = lot["expiration"]
        insert_lot(
            session,
            ledger_id=source.id,
            address=address,
            period=source.period,
            kind=source.kind,
            business_key=source.business_key,
            amount_remaining=int(lot["amount_remaining"]),
            expiration=(
                None if expiration is None else dt.datetime.fromisoformat(expiration)
            ),
        )


def _replay_rows(session: DbSession, rows: Sequence[Any]) -> None:
    """
    Re-derive lots from existing ledger rows, through the same lot logic as
    apply_period. Rows come in (period, id) order: the order apply_period wrote
    them, which keeps transfers of one hour in their original sequence.
    """
    for row in rows:
        if row.excluded_reason is not None:
            continue
        if row.kind in CREDIT_KINDS:
            if row.amount > 0:
                insert_lot(
                    session,
                    ledger_id=row.id,
                    address=row.address,
                    period=row.period,
                    kind=row.kind,
                    business_key=row.business_key,
                    amount_remaining=int(row.amount),
                    expiration=row.expiration,
                )
        elif row.kind in DEBIT_KINDS:
            _drain(session, row.address, -int(row.amount), row.period)


def refold_from(session: DbSession, period_start: dt.datetime) -> None:
    """
    Drop every ledger row and checkpoint from `period_start` on and rebuild the
    lots as they stood at the end of the previous period, so that the fold can
    re-apply periods from `period_start` forward.
    """
    delete_all_lots(session)
    delete_ledger_rows_from(session, period_start)
    delete_checkpoints_from(session, period_start)

    checkpoint = latest_checkpoint(session, GLOBAL_ADDRESS, before=period_start)
    if checkpoint is not None:
        _restore_checkpoint(session, checkpoint.period)
        _replay_rows(
            session,
            ledger_rows(session, after=checkpoint.period, before=period_start),
        )
    else:
        _replay_rows(session, ledger_rows(session, before=period_start))

    state = get_fold_state(session)
    last_closed: Optional[dt.datetime] = None
    if checkpoint is not None or has_ledger_rows_before(session, period_start):
        last_closed = period_start - PERIOD
        if state.last_closed_period is not None:
            last_closed = min(last_closed, state.last_closed_period)
    state.last_closed_period = last_closed
    state.dirty_from_period = None
    session.flush()
