import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import (
    DECIMAL,
    BigInteger,
    Integer,
    Row,
    String,
    case,
    cast,
    delete,
    func,
    literal,
    null,
    or_,
    select,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.attributes import set_committed_value

from aleph.db.models.balances import AlephCreditHistoryDb
from aleph.db.models.credit_ledger import (
    BillingRateDb,
    BillingResourceDb,
    CreditAttestationDb,
    CreditCheckpointDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditLotDb,
    CreditPeriodQuoteDb,
    CreditTopupDb,
)
from aleph.db.models.messages import ForgottenMessageDb, MessageDb
from aleph.services.credits.addresses import normalize_address
from aleph.services.credits.periods import KIND_ORDER, floor_hour
from aleph.types.db_session import DbSession

# Credit transfers are fold inputs keyed on their confirmation time.
CREDIT_TRANSFER_POST_TYPE = "aleph_credit_transfer"


def get_fold_state(session: DbSession, for_update: bool = False) -> CreditFoldStateDb:
    """
    The singleton fold state row. `for_update` locks it until the transaction
    ends (and reloads it): fold transactions take this lock before reading
    their inputs, and wait for the KEY SHARE lock that `mark_dirty` holds in
    every transaction writing an input, so such an input is either seen by the
    fold or sees the new `last_closed_period` and marks the period dirty.
    """
    stmt = select(CreditFoldStateDb).where(CreditFoldStateDb.id == 1)
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    return session.execute(stmt).scalar_one()


def mark_dirty(session: DbSession, period_start: dt.datetime) -> bool:
    """
    Flag an already-closed period for re-folding.

    Returns True if the period was closed and has been marked, False if there is
    nothing to re-fold yet.

    Every call takes a FOR KEY SHARE lock on the fold state row, held until the
    calling transaction ends. A fold transaction locks the row FOR UPDATE before
    it reads any input, so it waits for every in-flight transaction that wrote
    an input and called this function: once the fold runs, those inputs are
    committed and visible to it. Conversely, a transaction calling this after
    the fold committed reads the new `last_closed_period` and marks the period.
    A period that is not closed yet (`last_closed_period` is None, which a
    refold can bring back, or before `period_start`) is left alone, but the
    lock is still held: that period may be the one the fold closes next.

    KEY SHARE locks do not conflict with each other, nor with the FOR NO KEY
    UPDATE lock taken by the conditional UPDATE below, so message transactions
    never wait for one another to read the state; two of them marking at the
    same time serialise on the UPDATE only.
    """
    last_closed = session.execute(
        select(CreditFoldStateDb.last_closed_period)
        .where(CreditFoldStateDb.id == 1)
        .with_for_update(read=True, key_share=True)
    ).scalar_one()
    if last_closed is None or period_start > last_closed:
        return False
    marked = session.execute(
        update(CreditFoldStateDb)
        .where(
            CreditFoldStateDb.id == 1,
            CreditFoldStateDb.last_closed_period >= period_start,
        )
        .values(
            dirty_from_period=func.least(
                func.coalesce(CreditFoldStateDb.dirty_from_period, period_start),
                period_start,
            )
        )
        .returning(CreditFoldStateDb.dirty_from_period)
        .execution_options(synchronize_session=False)
    ).first()
    if marked is None:
        return False
    # Keep a loaded fold state object (e.g. held by the caller) in sync with
    # the row, without discarding its other pending changes.
    state = session.identity_map.get(session.identity_key(CreditFoldStateDb, 1))
    if state is not None:
        set_committed_value(state, "dirty_from_period", marked[0])
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
        # Stored in ledger form so every lookup is an indexed equality.
        owner=normalize_address(message.owner or content["address"]),
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
        CreditLedgerDb.rate,
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


# --- API reads (spec 10) ------------------------------------------------------
#
# Read-only queries behind the /api/v0/credits endpoints and the v2 paths of the
# legacy credit endpoints. They read the v2 tables whatever `serve_v2` says, so
# they also serve the shadow run.


def kind_order_expr(kind_column: Any) -> Any:
    """SQL form of KIND_ORDER for `kind_column` (unknown kinds sort last)."""
    return case(
        {kind: rank for kind, rank in KIND_ORDER.items()},
        value=kind_column,
        else_=len(KIND_ORDER),
    )


def period_kind_totals(
    session: DbSession, period_start: dt.datetime
) -> Dict[str, Tuple[int, int]]:
    """{kind: (rows, sum of amount)} of the period's ledger rows, exclusions included."""
    rows = session.execute(
        select(
            CreditLedgerDb.kind,
            func.count().label("rows"),
            func.coalesce(func.sum(CreditLedgerDb.amount), 0).label("amount"),
        )
        .where(CreditLedgerDb.period == period_start)
        .group_by(CreditLedgerDb.kind)
    ).all()
    return {row.kind: (int(row.rows), int(row.amount)) for row in rows}


def attestation_sender(session: DbSession, item_hash: str) -> Optional[str]:
    return session.execute(
        select(CreditAttestationDb.sender).where(
            CreditAttestationDb.item_hash == item_hash
        )
    ).scalar_one_or_none()


def global_checkpoint_hash(
    session: DbSession, period_start: dt.datetime
) -> Optional[str]:
    """The global ('' address) checkpoint hash written for the period, if any."""
    return session.execute(
        select(CreditCheckpointDb.ledger_hash).where(
            CreditCheckpointDb.period == period_start,
            CreditCheckpointDb.address == "",
        )
    ).scalar_one_or_none()


def revenue_sums(session: DbSession, start: dt.datetime, end: dt.datetime) -> List[Row]:
    """
    (period, kind, node_id, amount) sums of the billed (non-excluded)
    `execution`, `hold_equivalent` and `storage` rows with
    `start <= period < end`. Amounts keep their ledger sign.
    """
    return list(
        session.execute(
            select(
                CreditLedgerDb.period,
                CreditLedgerDb.kind,
                CreditLedgerDb.node_id,
                func.sum(CreditLedgerDb.amount).label("amount"),
            )
            .where(
                CreditLedgerDb.period >= start,
                CreditLedgerDb.period < end,
                CreditLedgerDb.kind.in_(("execution", "hold_equivalent", "storage")),
                CreditLedgerDb.excluded_reason.is_(None),
            )
            .group_by(
                CreditLedgerDb.period, CreditLedgerDb.kind, CreditLedgerDb.node_id
            )
        ).all()
    )


def period_quotes(
    session: DbSession, start: dt.datetime, end: dt.datetime
) -> Dict[dt.datetime, Decimal]:
    """{period: ALEPH per credit} stored for `start <= period < end`."""
    rows = session.execute(
        select(CreditPeriodQuoteDb.period, CreditPeriodQuoteDb.aleph_per_credit).where(
            CreditPeriodQuoteDb.period >= start, CreditPeriodQuoteDb.period < end
        )
    ).all()
    return {row.period: row.aleph_per_credit for row in rows}


def _exclusion_filters(
    period: Optional[dt.datetime], reason: Optional[str], address: Optional[str]
) -> List[Any]:
    filters: List[Any] = [CreditLedgerDb.excluded_reason.is_not(None)]
    if period is not None:
        filters.append(CreditLedgerDb.period == period)
    if reason is not None:
        filters.append(CreditLedgerDb.excluded_reason == reason)
    if address is not None:
        filters.append(CreditLedgerDb.address == normalize_address(address))
    return filters


def get_exclusions(
    session: DbSession,
    period: Optional[dt.datetime] = None,
    reason: Optional[str] = None,
    address: Optional[str] = None,
    page: int = 1,
    pagination: int = 100,
) -> List[CreditLedgerDb]:
    """Exclusion rows, (period desc, kind order, business key) ordered."""
    stmt = (
        select(CreditLedgerDb)
        .where(*_exclusion_filters(period, reason, address))
        .order_by(
            CreditLedgerDb.period.desc(),
            kind_order_expr(CreditLedgerDb.kind),
            CreditLedgerDb.business_key,
        )
        .offset((page - 1) * pagination)
        .limit(pagination)
    )
    return list(session.execute(stmt).scalars())


def count_exclusions(
    session: DbSession,
    period: Optional[dt.datetime] = None,
    reason: Optional[str] = None,
    address: Optional[str] = None,
) -> int:
    return session.execute(
        select(func.count()).where(*_exclusion_filters(period, reason, address))
    ).scalar_one()


def _topup_address_filter(address: str) -> Any:
    # from_address is stored checksummed; the table is small (a few rows a day).
    return func.lower(CreditTopupDb.from_address) == normalize_address(address)


def get_topups_by_address(
    session: DbSession, address: str, page: int = 1, pagination: int = 100
) -> List[CreditTopupDb]:
    """The address's indexed top-ups, newest first."""
    stmt = (
        select(CreditTopupDb)
        .where(_topup_address_filter(address))
        .order_by(
            CreditTopupDb.block_number.desc(),
            CreditTopupDb.log_index.desc(),
            CreditTopupDb.chain_id,
            CreditTopupDb.tx_hash,
        )
        .offset((page - 1) * pagination)
        .limit(pagination)
    )
    return list(session.execute(stmt).scalars())


def count_topups_by_address(session: DbSession, address: str) -> int:
    return session.execute(
        select(func.count()).where(_topup_address_filter(address))
    ).scalar_one()


# --- /api/v0/credit_balances from the lots ------------------------------------


def _lot_balance_query(min_balance: int) -> Any:
    balance = func.sum(CreditLotDb.amount_remaining).label("balance")
    return (
        select(CreditLotDb.address, balance)
        .where(
            CreditLotDb.amount_remaining > 0,
            or_(CreditLotDb.expiration.is_(None), CreditLotDb.expiration > func.now()),
        )
        .group_by(CreditLotDb.address)
        .having(func.sum(CreditLotDb.amount_remaining) >= min_balance)
    )


def get_credit_balances_v2(
    session: DbSession,
    page: int = 1,
    pagination: int = 100,
    min_balance: int = 0,
    after_address: Optional[str] = None,
    cursor_mode: bool = False,
) -> List[Tuple[str, int]]:
    """
    `get_credit_balances` over `credit_lots`: per address, the sum of its
    unexpired lots as of the last closed period. The provisional accrual of the
    open hours is not subtracted (it needs every address's live resources and
    rates; the per-address balance endpoint does it). Only addresses with a
    positive sum are listed. Same pagination contract as the legacy accessor.
    """
    query = _lot_balance_query(min_balance).order_by(CreditLotDb.address.asc())
    if after_address is not None:
        query = query.where(CreditLotDb.address > after_address)
    if after_address is not None or cursor_mode:
        if pagination:
            query = query.limit(pagination + 1)
    else:
        query = query.offset((page - 1) * pagination)
        if pagination:
            query = query.limit(pagination)
    return [(row.address, int(row.balance)) for row in session.execute(query).all()]


def count_credit_balances_v2(session: DbSession, min_balance: int = 0) -> int:
    sub = _lot_balance_query(min_balance).subquery()
    return session.execute(select(func.count()).select_from(sub)).scalar_one()


# --- credit history from the ledger -------------------------------------------


def credit_history_source(address: str, cutover: Optional[dt.datetime]) -> Any:
    """
    The credit history of `address` as one UNION ALL subquery with the legacy
    `credit_history` columns plus `node_id`, `seconds`, `excluded_reason` and
    two keyset tiebreakers, `tb_ref` (text) and `tb_index` (int):

    - legacy rows with `message_timestamp < cutover` (address compared as
      given, as the legacy endpoint does), tiebreakers (credit_ref,
      credit_index);
    - ledger rows of `normalize_address(address)` with `period >= cutover`,
      `hold_equivalent` rows left out (a notional amount of a hold-paid VM,
      not a balance movement). Mapping: payment_method = kind, origin =
      resource, origin_ref = source_ref, tx_hash = source_ref for top-ups,
      credit_ref = source_ref (business_key when the row has none),
      credit_index = 0, message_timestamp = period, expiration_date =
      expiration, expense_count/size_mib = count/size_mib. Tiebreakers
      ("{kind order}:{business_key}", 0), so rows of one period sort by
      (kind order, business key), in the direction of the requested sort.

    `cutover` None (no genesis or start period configured) keeps every row of
    both tables.
    """
    legacy = select(
        AlephCreditHistoryDb.amount.label("amount"),
        AlephCreditHistoryDb.price.label("price"),
        AlephCreditHistoryDb.bonus_amount.label("bonus_amount"),
        AlephCreditHistoryDb.tx_hash.label("tx_hash"),
        AlephCreditHistoryDb.token.label("token"),
        AlephCreditHistoryDb.chain.label("chain"),
        AlephCreditHistoryDb.provider.label("provider"),
        AlephCreditHistoryDb.origin.label("origin"),
        AlephCreditHistoryDb.origin_ref.label("origin_ref"),
        AlephCreditHistoryDb.payment_method.label("payment_method"),
        AlephCreditHistoryDb.credit_ref.label("credit_ref"),
        AlephCreditHistoryDb.credit_index.label("credit_index"),
        AlephCreditHistoryDb.expiration_date.label("expiration_date"),
        AlephCreditHistoryDb.message_timestamp.label("message_timestamp"),
        AlephCreditHistoryDb.expense_count.label("expense_count"),
        AlephCreditHistoryDb.expense_size_mib.label("expense_size_mib"),
        cast(null(), String).label("node_id"),
        cast(null(), Integer).label("seconds"),
        cast(null(), String).label("excluded_reason"),
        AlephCreditHistoryDb.credit_ref.label("tb_ref"),
        AlephCreditHistoryDb.credit_index.label("tb_index"),
    ).where(AlephCreditHistoryDb.address == address)
    if cutover is not None:
        legacy = legacy.where(AlephCreditHistoryDb.message_timestamp < cutover)

    ledger = select(
        CreditLedgerDb.amount.label("amount"),
        cast(null(), DECIMAL).label("price"),
        cast(null(), BigInteger).label("bonus_amount"),
        case(
            (CreditLedgerDb.kind == "topup", CreditLedgerDb.source_ref), else_=null()
        ).label("tx_hash"),
        cast(null(), String).label("token"),
        cast(null(), String).label("chain"),
        cast(null(), String).label("provider"),
        CreditLedgerDb.resource.label("origin"),
        CreditLedgerDb.source_ref.label("origin_ref"),
        CreditLedgerDb.kind.label("payment_method"),
        func.coalesce(CreditLedgerDb.source_ref, CreditLedgerDb.business_key).label(
            "credit_ref"
        ),
        literal(0, Integer).label("credit_index"),
        CreditLedgerDb.expiration.label("expiration_date"),
        CreditLedgerDb.period.label("message_timestamp"),
        cast(CreditLedgerDb.entry_count, BigInteger).label("expense_count"),
        cast(CreditLedgerDb.size_mib, DECIMAL).label("expense_size_mib"),
        CreditLedgerDb.node_id.label("node_id"),
        CreditLedgerDb.seconds.label("seconds"),
        CreditLedgerDb.excluded_reason.label("excluded_reason"),
        func.concat(
            cast(kind_order_expr(CreditLedgerDb.kind), String),
            ":",
            CreditLedgerDb.business_key,
        ).label("tb_ref"),
        literal(0, Integer).label("tb_index"),
    ).where(
        CreditLedgerDb.address == normalize_address(address),
        CreditLedgerDb.kind != "hold_equivalent",
    )
    if cutover is not None:
        ledger = ledger.where(CreditLedgerDb.period >= cutover)

    return union_all(legacy, ledger).subquery("credit_history_v2")


def consumed_execution_credits(
    session: DbSession, item_hash: str, since: Optional[dt.datetime]
) -> int:
    """Credits the billed `execution` rows of a VM charged, from `since` on."""
    stmt = select(func.coalesce(func.sum(-CreditLedgerDb.amount), 0)).where(
        CreditLedgerDb.kind == "execution",
        CreditLedgerDb.resource == item_hash,
        CreditLedgerDb.excluded_reason.is_(None),
    )
    if since is not None:
        stmt = stmt.where(CreditLedgerDb.period >= since)
    return int(session.execute(stmt).scalar_one())
