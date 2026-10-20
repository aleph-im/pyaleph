import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import Row, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models.credit_ledger import (
    BillingRateDb,
    BillingResourceDb,
    CreditAttestationDb,
    CreditCheckpointDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditLotDb,
    CreditTopupDb,
)
from aleph.db.models.messages import ForgottenMessageDb, MessageDb
from aleph.services.credits.periods import PERIOD, floor_hour
from aleph.types.db_session import DbSession

# Credit transfers are fold inputs keyed on their confirmation time.
CREDIT_TRANSFER_POST_TYPE = "aleph_credit_transfer"


def get_fold_state(session: DbSession, for_update: bool = False) -> CreditFoldStateDb:
    """
    The singleton fold state row. `for_update` locks it until the transaction
    ends (and reloads it): fold transactions and dirty marks take this lock, so
    an input committed concurrently with a period close is either seen by the
    fold or sees the new `last_closed_period` and marks the period dirty.
    """
    stmt = select(CreditFoldStateDb).where(CreditFoldStateDb.id == 1)
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    return session.execute(stmt).scalar_one()


def _last_closed_period(session: DbSession, share_lock: bool = False):
    # A column query: always read from the database, never the identity map.
    stmt = select(CreditFoldStateDb.last_closed_period).where(CreditFoldStateDb.id == 1)
    if share_lock:
        stmt = stmt.with_for_update(read=True)
    return session.execute(stmt).scalar_one()


def mark_dirty(session: DbSession, period_start: dt.datetime) -> bool:
    """
    Flag an already-closed period for re-folding.

    Returns True if the period was closed and has been marked, False if there is
    nothing to re-fold yet.

    Called on every confirmed paid message, so the exclusive fold-state lock is
    only taken when a mark is possible. A fold transaction closes exactly one
    period (`last_closed_period + 1h`) and a refold only moves the mark back,
    so a period more than one hour past the committed mark cannot be the one
    being folded right now: no lock is needed to return False. With no period
    closed yet, the first close may be in flight; a shared lock waits for it
    without serialising the message transactions among themselves.
    """
    last_closed = _last_closed_period(session)
    if last_closed is not None and period_start > last_closed + PERIOD:
        return False
    if last_closed is None:
        last_closed = _last_closed_period(session, share_lock=True)
        if last_closed is None or period_start > last_closed:
            return False
    state = get_fold_state(session, for_update=True)
    if state.last_closed_period is None or period_start > state.last_closed_period:
        return False
    state.dirty_from_period = (
        period_start
        if state.dirty_from_period is None
        else min(state.dirty_from_period, period_start)
    )
    return True


def get_billing_resource(
    session: DbSession, item_hash: str
) -> Optional[BillingResourceDb]:
    return session.execute(
        select(BillingResourceDb).where(BillingResourceDb.item_hash == item_hash)
    ).scalar_one_or_none()


def get_first_confirmed_at(session: DbSession, item_hash: str) -> Optional[dt.datetime]:
    """
    Read first_confirmed_at from the DB: the in-memory MessageDb being processed
    predates the confirmation insert whose trigger sets it.
    """
    return session.execute(
        select(MessageDb.first_confirmed_at).where(MessageDb.item_hash == item_hash)
    ).scalar_one_or_none()


def upsert_billing_resource(
    session: DbSession, message: MessageDb, cost_spec: Dict[str, Any]
) -> None:
    """
    Insert the billing snapshot of a paid message. On conflict nothing changes,
    except first_confirmed_at when it was still null.
    """
    content = message.content
    # A message without a payment section is a hold message (the legacy
    # default): it is billed as hold_equivalent, never drained from credits.
    payment_type = PaymentType(message.payment_type or PaymentType.hold.value)
    confirmed_at = get_first_confirmed_at(session, message.item_hash)
    stmt = insert(BillingResourceDb).values(
        item_hash=message.item_hash,
        type=MessageType(message.type),
        owner=message.owner or content["address"],
        payment_type=payment_type,
        first_confirmed_at=confirmed_at,
        cost_spec=cost_spec,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[BillingResourceDb.item_hash],
        set_={"first_confirmed_at": stmt.excluded.first_confirmed_at},
        where=(
            BillingResourceDb.first_confirmed_at.is_(None)
            & stmt.excluded.first_confirmed_at.is_not(None)
        ),
    ).returning(BillingResourceDb.item_hash)
    written = session.execute(stmt).first()
    if written is not None and confirmed_at is not None:
        # Processed after its confirmation: the hour it starts billing in may
        # already be closed.
        mark_dirty(session, floor_hour(confirmed_at))


def refresh_billing_confirmation(
    session: DbSession, item_hash: str, confirmed_at: dt.datetime
) -> None:
    """
    first_confirmed_at = LEAST(existing, confirmed_at), or confirmed_at if null.
    A change re-folds the hour of the new value if it is already closed.
    """
    changed = session.execute(
        update(BillingResourceDb)
        .where(
            BillingResourceDb.item_hash == item_hash,
            or_(
                BillingResourceDb.first_confirmed_at.is_(None),
                BillingResourceDb.first_confirmed_at > confirmed_at,
            ),
        )
        .values(first_confirmed_at=confirmed_at)
        .returning(BillingResourceDb.item_hash)
    ).first()
    if changed is not None:
        mark_dirty(session, floor_hour(confirmed_at))


def set_billing_end_of_life(
    session: DbSession,
    item_hash: str,
    at: dt.datetime,
    reason: str,
    provisional: bool = False,
) -> None:
    """
    Close a billing resource. Sets end_of_life when it is null and overwrites a
    provisional value. A non-provisional value is final, except that a
    non-provisional `forgotten` end of life only ever moves down to a lower
    `forgotten` time: the FORGET may be confirmed several times (one packing
    node each) and every CCN must converge on the earliest confirmation,
    whatever the order it saw them in. It is never raised, and a `removed`
    end of life is never touched.

    A change re-folds from the hour of the earlier of the old and new values,
    if that hour is already closed.
    """
    current = session.execute(
        select(
            BillingResourceDb.end_of_life,
            BillingResourceDb.end_of_life_provisional,
            BillingResourceDb.end_reason,
        )
        .where(BillingResourceDb.item_hash == item_hash)
        .with_for_update()
    ).one_or_none()
    if current is None:
        return
    previous, previous_provisional, previous_reason = current
    if previous is not None and not previous_provisional:
        lowers_forget = (
            reason == "forgotten"
            and not provisional
            and previous_reason == "forgotten"
            and at < previous
        )
        if lowers_forget:
            session.execute(
                update(BillingResourceDb)
                .where(BillingResourceDb.item_hash == item_hash)
                .values(end_of_life=at)
            )
            mark_dirty(session, floor_hour(at))
        return
    session.execute(
        update(BillingResourceDb)
        .where(
            BillingResourceDb.item_hash == item_hash,
            or_(
                BillingResourceDb.end_of_life.is_(None),
                BillingResourceDb.end_of_life_provisional.is_(True),
            ),
        )
        .values(
            end_of_life=at,
            end_reason=reason,
            end_of_life_provisional=provisional,
        )
    )
    if previous != at:
        mark_dirty(session, floor_hour(at if previous is None else min(previous, at)))


def apply_confirmation_to_billing(session: DbSession, message: MessageDb) -> None:
    """
    A new confirmation reached an already-processed message: refresh its own
    billing start and, for a FORGET, turn the provisional end of life of its
    targets into the chain confirmation time.
    """
    confirmed_at = get_first_confirmed_at(session, message.item_hash)
    if confirmed_at is None:
        return
    refresh_billing_confirmation(session, message.item_hash, confirmed_at)
    mark_transfer_dirty(session, message, confirmed_at)
    mark_attestation_dirty(session, message)
    if message.type == MessageType.forget:
        # Direct targets, plus the ones forgotten by cascade (e.g. VM updates),
        # which only the forgotten_messages record ties back to this FORGET.
        target_hashes = set(message.content.get("hashes", []))
        target_hashes.update(
            session.execute(
                select(ForgottenMessageDb.item_hash).where(
                    ForgottenMessageDb.forgotten_by.contains([message.item_hash])
                )
            ).scalars()
        )
        for target_hash in sorted(target_hashes):
            set_billing_end_of_life(
                session, target_hash, confirmed_at, "forgotten", provisional=False
            )


def mark_transfer_dirty(
    session: DbSession,
    message: MessageDb,
    confirmed_at: Optional[dt.datetime] = None,
) -> None:
    """
    A credit transfer is folded in the hour of its first confirmation: when that
    hour is already closed (late confirmation, or a message processed after its
    confirmation), re-fold it. Only a lower confirmation time changes the
    input, but a spurious mark costs one refold and a missed one loses the
    transfer, so any confirmation of a transfer marks. `confirmed_at` is read
    from the database when not given.
    """
    if (
        message.type != MessageType.post
        or message.content_type != CREDIT_TRANSFER_POST_TYPE
    ):
        return
    if confirmed_at is None:
        confirmed_at = get_first_confirmed_at(session, message.item_hash)
    if confirmed_at is not None:
        mark_dirty(session, floor_hour(confirmed_at))


def mark_attestation_dirty(session: DbSession, message: MessageDb) -> None:
    """
    A confirmation of an attestation post can change which attestation the
    fold selects for its period (a first confirmation, or a lower one that
    reorders the confirmed posts): re-fold the period if it is closed. Marked
    on every confirmation of such a post, which is rare; a spurious mark only
    costs a refold.
    """
    if message.type != MessageType.post:
        return
    period = attestation_period(session, message.item_hash)
    if period is not None:
        mark_dirty(session, period)


def iter_live_billing_resources(
    session: DbSession,
    type_: Optional[MessageType],
    payment_type: Optional[PaymentType],
    period_start: dt.datetime,
    period_end: dt.datetime,
) -> Iterable[BillingResourceDb]:
    """Resources alive at some point of [period_start, period_end)."""
    stmt = select(BillingResourceDb).where(
        BillingResourceDb.first_confirmed_at < period_end,
        or_(
            BillingResourceDb.end_of_life.is_(None),
            BillingResourceDb.end_of_life > period_start,
        ),
    )
    if type_ is not None:
        stmt = stmt.where(BillingResourceDb.type == type_)
    if payment_type is not None:
        stmt = stmt.where(BillingResourceDb.payment_type == payment_type)
    return session.execute(stmt.order_by(BillingResourceDb.item_hash)).scalars().all()


def get_billing_rate(
    session: DbSession, item_hash: str, pricing_version: str
) -> Optional[Decimal]:
    return session.execute(
        select(BillingRateDb.rate_credit).where(
            BillingRateDb.item_hash == item_hash,
            BillingRateDb.pricing_version == pricing_version,
        )
    ).scalar_one_or_none()


def upsert_billing_rate(
    session: DbSession, item_hash: str, pricing_version: str, rate: Decimal
) -> None:
    session.execute(
        insert(BillingRateDb)
        .values(item_hash=item_hash, pricing_version=pricing_version, rate_credit=rate)
        .on_conflict_do_nothing(index_elements=["item_hash", "pricing_version"])
    )


def upsert_topup(session: DbSession, row: CreditTopupDb) -> bool:
    """Insert a top-up; an existing (chain_id, tx_hash, log_index) is left alone."""
    result = session.execute(
        insert(CreditTopupDb)
        .values(
            chain_id=row.chain_id,
            tx_hash=row.tx_hash,
            log_index=row.log_index,
            block_number=row.block_number,
            block_time=row.block_time,
            from_address=row.from_address,
            token=row.token,
            raw_amount=row.raw_amount,
            quote_num=row.quote_num,
            quote_den=row.quote_den,
            credits=row.credits,
            status=row.status,
            ledger_period=row.ledger_period,
        )
        .on_conflict_do_nothing(index_elements=["chain_id", "tx_hash", "log_index"])
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


def topups_for_period(
    session: DbSession, period_start: dt.datetime, period_end: dt.datetime
) -> List[CreditTopupDb]:
    return list(
        session.execute(
            select(CreditTopupDb)
            .where(
                CreditTopupDb.status == "credited",
                CreditTopupDb.block_time >= period_start,
                CreditTopupDb.block_time < period_end,
            )
            .order_by(
                CreditTopupDb.block_time,
                CreditTopupDb.tx_hash,
                CreditTopupDb.log_index,
            )
        ).scalars()
    )


def unquoted_topups(session: DbSession) -> List[CreditTopupDb]:
    return list(
        session.execute(
            select(CreditTopupDb)
            .where(CreditTopupDb.status == "unquoted")
            .order_by(
                CreditTopupDb.block_number,
                CreditTopupDb.tx_hash,
                CreditTopupDb.log_index,
            )
        ).scalars()
    )


def active_attestation(
    session: DbSession, period_start: dt.datetime
) -> Optional[CreditAttestationDb]:
    """
    The row whose (informational) status is `active`: the reception-order
    view of the period. The fold does not use it, see `selected_attestation`.
    """
    return session.execute(
        select(CreditAttestationDb).where(
            CreditAttestationDb.period == period_start,
            CreditAttestationDb.status == "active",
        )
    ).scalar_one_or_none()


def selected_attestation(
    session: DbSession, period_start: dt.datetime
) -> Optional[str]:
    """
    Item hash of the attestation the fold uses for the period (spec 7.3), or
    None when no attestation of the period is confirmed yet.

    Deterministic from the database, whatever the order the posts were
    received in: among the period's attestation posts (indexed only from
    whitelisted senders) that have a chain confirmation, drop every post that
    another post of that confirmed set supersedes, then take the lowest
    (first_confirmed_at, item_hash). An unconfirmed post counts as not
    received yet.
    """
    rows = session.execute(
        select(
            CreditAttestationDb.item_hash,
            CreditAttestationDb.supersedes,
            MessageDb.first_confirmed_at,
        )
        .join(MessageDb, MessageDb.item_hash == CreditAttestationDb.item_hash)
        .where(
            CreditAttestationDb.period == period_start,
            MessageDb.first_confirmed_at.is_not(None),
        )
    ).all()
    confirmed = {row.item_hash for row in rows}
    superseded = {
        row.supersedes
        for row in rows
        if row.supersedes in confirmed and row.supersedes != row.item_hash
    }
    candidates = sorted(
        (row.first_confirmed_at, row.item_hash)
        for row in rows
        if row.item_hash not in superseded
    )
    return candidates[0][1] if candidates else None


def attestation_period(session: DbSession, item_hash: str) -> Optional[dt.datetime]:
    """Period of an indexed attestation post, None for any other message."""
    return session.execute(
        select(CreditAttestationDb.period).where(
            CreditAttestationDb.item_hash == item_hash
        )
    ).scalar_one_or_none()


def record_attestation(
    session: DbSession,
    item_hash: str,
    period_start: dt.datetime,
    sender: str,
    supersedes: Optional[str],
) -> str:
    """
    Index an execution-intervals post under its period and return its status.

    Every post is kept: the fold picks one at fold time from the confirmed
    posts (`selected_attestation`). The status only records the reception
    order view (the first post is active, a post superseding the active one
    replaces it, any other is a duplicate) and is informational.

    A post processed after its confirmation can change the selection of an
    already-closed period: the period is marked dirty. An unconfirmed post is
    marked by its confirmation (`apply_confirmation_to_billing`).
    Idempotent per item hash.
    """
    existing_row = session.get(CreditAttestationDb, item_hash)
    if existing_row is not None:
        return existing_row.status

    status = "duplicate"
    current = active_attestation(session, period_start)
    if current is None:
        status = "active"
    elif supersedes is not None and supersedes == current.item_hash:
        current.status = "superseded"
        # Free the partial unique index slot before inserting the new active row.
        session.flush()
        status = "active"

    session.add(
        CreditAttestationDb(
            item_hash=item_hash,
            period=period_start,
            sender=sender,
            supersedes=supersedes,
            status=status,
        )
    )
    session.flush()
    if get_first_confirmed_at(session, item_hash) is not None:
        mark_dirty(session, period_start)
    return status


# --- Ledger, lots and checkpoints (fold core) ---------------------------------


def ledger_row_exists(session: DbSession, kind: str, business_key: str) -> bool:
    return (
        session.execute(
            select(CreditLedgerDb.id).where(
                CreditLedgerDb.kind == kind,
                CreditLedgerDb.business_key == business_key,
            )
        ).scalar_one_or_none()
        is not None
    )


def insert_ledger_row(session: DbSession, values: Dict[str, Any]) -> Optional[int]:
    """
    Insert a ledger row; returns its id, or None when (kind, business_key)
    already exists (the row counts as already applied).
    """
    return session.execute(
        insert(CreditLedgerDb)
        .values(**values)
        .on_conflict_do_nothing(index_elements=["kind", "business_key"])
        .returning(CreditLedgerDb.id)
    ).scalar_one_or_none()


def insert_lot(
    session: DbSession,
    ledger_id: int,
    address: str,
    period: dt.datetime,
    kind: str,
    business_key: str,
    amount_remaining: int,
    expiration: Optional[dt.datetime],
) -> None:
    session.execute(
        insert(CreditLotDb).values(
            ledger_id=ledger_id,
            address=address,
            period=period,
            kind=kind,
            business_key=business_key,
            amount_remaining=amount_remaining,
            expiration=expiration,
        )
    )


def lock_valid_lots(
    session: DbSession, address: str, period_start: dt.datetime
) -> List[Row]:
    """
    Lots of `address` that can be drained in the period starting at
    `period_start`: remaining credit, granted in or before that period and not
    expired at its start. Locked FOR UPDATE. Returned in ledger_id order (lock
    order only); the caller sorts them into drain order.
    """
    return list(
        session.execute(
            select(
                CreditLotDb.ledger_id,
                CreditLotDb.period,
                CreditLotDb.kind,
                CreditLotDb.business_key,
                CreditLotDb.amount_remaining,
                CreditLotDb.expiration,
            )
            .where(
                CreditLotDb.address == address,
                CreditLotDb.amount_remaining > 0,
                CreditLotDb.period <= period_start,
                or_(
                    CreditLotDb.expiration.is_(None),
                    CreditLotDb.expiration > period_start,
                ),
            )
            .order_by(CreditLotDb.ledger_id)
            .with_for_update()
        ).all()
    )


def set_lot_remaining(session: DbSession, ledger_id: int, amount: int) -> None:
    session.execute(
        update(CreditLotDb)
        .where(CreditLotDb.ledger_id == ledger_id)
        .values(amount_remaining=amount)
    )


def lots_with_remaining(session: DbSession) -> List[Row]:
    """Every lot with credit left (expired or not), in ledger_id order."""
    return list(
        session.execute(
            select(
                CreditLotDb.ledger_id,
                CreditLotDb.address,
                CreditLotDb.period,
                CreditLotDb.kind,
                CreditLotDb.business_key,
                CreditLotDb.amount_remaining,
                CreditLotDb.expiration,
            )
            .where(CreditLotDb.amount_remaining > 0)
            .order_by(CreditLotDb.ledger_id)
        ).all()
    )


def ledger_rows(
    session: DbSession,
    after: Optional[dt.datetime] = None,
    until: Optional[dt.datetime] = None,
    before: Optional[dt.datetime] = None,
    address: Optional[str] = None,
) -> List[Row]:
    """
    Ledger rows with `after < period <= until` and `period < before` (each bound
    optional), in (period, id) order.
    """
    stmt = select(*_ledger_columns())
    if after is not None:
        stmt = stmt.where(CreditLedgerDb.period > after)
    if until is not None:
        stmt = stmt.where(CreditLedgerDb.period <= until)
    if before is not None:
        stmt = stmt.where(CreditLedgerDb.period < before)
    if address is not None:
        stmt = stmt.where(CreditLedgerDb.address == address)
    return list(
        session.execute(stmt.order_by(CreditLedgerDb.period, CreditLedgerDb.id)).all()
    )


def ledger_addresses_between(
    session: DbSession, after: Optional[dt.datetime], until: dt.datetime
) -> List[str]:
    """Non-null addresses with ledger rows in `after < period <= until`."""
    stmt = select(CreditLedgerDb.address).where(
        CreditLedgerDb.address.is_not(None), CreditLedgerDb.period <= until
    )
    if after is not None:
        stmt = stmt.where(CreditLedgerDb.period > after)
    return [
        address
        for address in session.execute(stmt.distinct()).scalars()
        if address is not None
    ]


def _ledger_columns() -> tuple:
    return (
        CreditLedgerDb.id,
        CreditLedgerDb.period,
        CreditLedgerDb.address,
        CreditLedgerDb.kind,
        CreditLedgerDb.business_key,
        CreditLedgerDb.amount,
        CreditLedgerDb.node_id,
        CreditLedgerDb.seconds,
        CreditLedgerDb.expiration,
        CreditLedgerDb.excluded_reason,
    )


def latest_checkpoints(
    session: DbSession, addresses: List[str], before: dt.datetime
) -> Dict[str, Row]:
    """
    Per address of `addresses`: (address, period, ledger_hash) of its most
    recent checkpoint with period < `before`. Addresses without one are absent.
    """
    if not addresses:
        return {}
    rows = session.execute(
        select(
            CreditCheckpointDb.address,
            CreditCheckpointDb.period,
            CreditCheckpointDb.ledger_hash,
        )
        .where(
            CreditCheckpointDb.address.in_(addresses),
            CreditCheckpointDb.period < before,
        )
        .distinct(CreditCheckpointDb.address)
        .order_by(CreditCheckpointDb.address, CreditCheckpointDb.period.desc())
    ).all()
    return {row.address: row for row in rows}


def ledger_rows_since_checkpoints(
    session: DbSession, addresses: List[str], until: dt.datetime
) -> List[Row]:
    """
    For every address of `addresses`, its ledger rows with period <= `until`
    and after its latest checkpoint before `until` (all of them when it has
    none), in one query, ordered by (address, period, id).
    """
    if not addresses:
        return []
    previous = (
        select(
            CreditCheckpointDb.address.label("address"),
            func.max(CreditCheckpointDb.period).label("period"),
        )
        .where(
            CreditCheckpointDb.address.in_(addresses),
            CreditCheckpointDb.period < until,
        )
        .group_by(CreditCheckpointDb.address)
        .subquery()
    )
    stmt = (
        select(*_ledger_columns())
        .outerjoin(previous, previous.c.address == CreditLedgerDb.address)
        .where(
            CreditLedgerDb.address.in_(addresses),
            CreditLedgerDb.period <= until,
            or_(
                previous.c.period.is_(None),
                CreditLedgerDb.period > previous.c.period,
            ),
        )
        .order_by(CreditLedgerDb.address, CreditLedgerDb.period, CreditLedgerDb.id)
    )
    return list(session.execute(stmt).all())


def has_ledger_rows_before(session: DbSession, period_start: dt.datetime) -> bool:
    return (
        session.execute(
            select(CreditLedgerDb.id)
            .where(CreditLedgerDb.period < period_start)
            .limit(1)
        ).scalar_one_or_none()
        is not None
    )


def get_ledger_rows_by_ids(session: DbSession, ids: Iterable[int]) -> Dict[int, Row]:
    id_list = list(ids)
    if not id_list:
        return {}
    return {
        row.id: row
        for row in session.execute(
            select(
                CreditLedgerDb.id,
                CreditLedgerDb.address,
                CreditLedgerDb.period,
                CreditLedgerDb.kind,
                CreditLedgerDb.business_key,
            ).where(CreditLedgerDb.id.in_(id_list))
        ).all()
    }


def delete_all_lots(session: DbSession) -> None:
    session.execute(delete(CreditLotDb).execution_options(synchronize_session=False))


def delete_ledger_rows_from(session: DbSession, period_start: dt.datetime) -> None:
    """Delete ledger rows with period >= period_start. Lots must be gone first."""
    session.execute(
        delete(CreditLedgerDb)
        .where(CreditLedgerDb.period >= period_start)
        .execution_options(synchronize_session=False)
    )


def genesis_rows_from(
    session: DbSession, period_start: dt.datetime
) -> List[CreditLedgerDb]:
    """Genesis ledger rows with period >= period_start, in (period, id) order."""
    return list(
        session.execute(
            select(CreditLedgerDb)
            .where(
                CreditLedgerDb.kind == "genesis",
                CreditLedgerDb.period >= period_start,
            )
            .order_by(CreditLedgerDb.period, CreditLedgerDb.id)
        ).scalars()
    )


def delete_checkpoints_from(session: DbSession, period_start: dt.datetime) -> None:
    session.execute(
        delete(CreditCheckpointDb)
        .where(CreditCheckpointDb.period >= period_start)
        .execution_options(synchronize_session=False)
    )


def latest_checkpoint(
    session: DbSession, address: str, before: dt.datetime
) -> Optional[CreditCheckpointDb]:
    """The most recent checkpoint row of `address` with period < `before`."""
    return session.execute(
        select(CreditCheckpointDb)
        .where(
            CreditCheckpointDb.address == address,
            CreditCheckpointDb.period < before,
        )
        .order_by(CreditCheckpointDb.period.desc())
        .limit(1)
    ).scalar_one_or_none()


def checkpoint_rows(
    session: DbSession, period: dt.datetime
) -> List[CreditCheckpointDb]:
    """Every checkpoint row (addresses and the global '' row) for `period`."""
    return list(
        session.execute(
            select(CreditCheckpointDb)
            .where(CreditCheckpointDb.period == period)
            .order_by(CreditCheckpointDb.address)
        ).scalars()
    )


def upsert_checkpoint(
    session: DbSession,
    period: dt.datetime,
    address: str,
    lots: List[Dict[str, Any]],
    ledger_hash: str,
) -> None:
    stmt = insert(CreditCheckpointDb).values(
        period=period, address=address, lots=lots, ledger_hash=ledger_hash
    )
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=["period", "address"],
            set_={"lots": stmt.excluded.lots, "ledger_hash": stmt.excluded.ledger_hash},
        )
    )
