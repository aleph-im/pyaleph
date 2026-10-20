import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models.credit_ledger import (
    BillingRateDb,
    BillingResourceDb,
    CreditFoldStateDb,
)
from aleph.db.models.messages import ForgottenMessageDb, MessageDb
from aleph.types.db_session import DbSession


def get_fold_state(session: DbSession) -> CreditFoldStateDb:
    return session.execute(
        select(CreditFoldStateDb).where(CreditFoldStateDb.id == 1)
    ).scalar_one()


def mark_dirty(session: DbSession, period_start: dt.datetime) -> bool:
    """
    Flag an already-closed period for re-folding.

    Returns True if the period was closed and has been marked, False if there is
    nothing to re-fold yet.
    """
    state = get_fold_state(session)
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
    payment_type = PaymentType(message.payment_type or PaymentType.hold.value)
    stmt = insert(BillingResourceDb).values(
        item_hash=message.item_hash,
        type=MessageType(message.type),
        owner=message.owner or content["address"],
        payment_type=payment_type,
        first_confirmed_at=get_first_confirmed_at(session, message.item_hash),
        cost_spec=cost_spec,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[BillingResourceDb.item_hash],
        set_={
            "first_confirmed_at": func.coalesce(
                BillingResourceDb.first_confirmed_at,
                stmt.excluded.first_confirmed_at,
            )
        },
    )
    session.execute(stmt)


def refresh_billing_confirmation(
    session: DbSession, item_hash: str, confirmed_at: dt.datetime
) -> None:
    """first_confirmed_at = LEAST(existing, confirmed_at), or confirmed_at if null."""
    session.execute(
        update(BillingResourceDb)
        .where(BillingResourceDb.item_hash == item_hash)
        .values(
            first_confirmed_at=func.least(
                BillingResourceDb.first_confirmed_at, confirmed_at
            )
        )
    )


def set_billing_end_of_life(
    session: DbSession,
    item_hash: str,
    at: dt.datetime,
    reason: str,
    provisional: bool = False,
) -> None:
    """
    Close a billing resource. Sets end_of_life when it is null, overwrites a
    provisional value, and never touches a non-provisional one.
    """
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
