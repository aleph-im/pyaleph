"""
Credit balances served from the v2 ledger (spec 5.9).

The balance of an address is the remaining amount of its unexpired lots in
`credit_lots`, as of the last closed period, minus a provisional accrual for
the hours the fold has not closed yet: every live credit-paid resource of the
address is charged `rate × seconds` since the end of the last closed period.
The provisional part is computed on every read and never written.

`aleph.credits.serve_v2` has two effects, gated differently:

* ingest decisions on ledger inputs (credit transfers always PROCESSED when
  well-formed, distribution and expense posts inert) follow the configured
  flag immediately (`serve_v2_configured`). They must not depend on node-local
  state, or a transfer could be rejected on one node and processed on another
  during their respective cutover windows, a divergence REJECTED makes
  permanent;
* reads (balance API, ingest admission check, removal cron, history) switch
  to this module only once the fold has closed a period (`serve_v2_active`):
  before that the v2 lots do not hold the genesis balances yet, so serving
  them would zero every account. Such a node logs an ERROR and keeps serving
  the legacy tables.

Addresses: every lookup goes through `normalize_address` (EVM addresses
lowercased), so a checksummed and a lowercased address read the same balance.
`billing_resources.owner` is stored in that form too, so owner lookups are
indexed equalities.
"""

import datetime as dt
import logging
from typing import Iterable, List, Optional, Tuple

from aleph_message.models import MessageType, PaymentType
from sqlalchemy import func, or_, select

from aleph.config import get_config
from aleph.db.accessors.balances import CreditBalanceDetail, get_credit_balance
from aleph.db.models.credit_ledger import (
    BillingResourceDb,
    CreditFoldStateDb,
    CreditLedgerDb,
    CreditLotDb,
)
from aleph.services.credits.addresses import normalize_address
from aleph.services.credits.cost_spec import CostSpecError
from aleph.services.credits.fold import execution_amount
from aleph.services.credits.inputs import _is_on_demand_program
from aleph.services.credits.periods import KIND_ORDER, floor_hour, period_end
from aleph.services.credits.rates import rate_for
from aleph.services.pricing_utils import pricing_model_at
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)

# Credit-paid resources that accrue while alive (on-demand PROGRAMs excluded).
ACCRUING_TYPES = (
    MessageType.instance,
    MessageType.program,
    MessageType.v_program,
    MessageType.store,
)

_NOT_READY_MESSAGE = (
    "aleph.credits.serve_v2 is enabled but the credit fold has never closed a "
    "period (credit_fold_state.last_closed_period is NULL): refusing to serve "
    "credit balances from the v2 ledger, serving the legacy tables instead"
)
_not_ready_logged = False


# --- serving switch -----------------------------------------------------------


def serve_v2_configured() -> bool:
    """
    The `serve_v2` flag alone. Gates ingest decisions on ledger inputs, which
    must be identical on every node whatever the state of its fold.
    """
    return bool(get_config().aleph.credits.serve_v2.value)


def last_closed_period(session: DbSession) -> Optional[dt.datetime]:
    """The fold's last closed period; None if none, or no fold state row."""
    return session.execute(
        select(CreditFoldStateDb.last_closed_period).where(CreditFoldStateDb.id == 1)
    ).scalar_one_or_none()


def check_serve_v2_at_startup(session: DbSession) -> bool:
    """
    Startup check: whether this node serves the v2 ledger. Logs an ERROR when
    `serve_v2` is set on a node whose fold has never closed a period.
    """
    if not serve_v2_configured():
        return False
    if last_closed_period(session) is None:
        LOGGER.error(_NOT_READY_MESSAGE)
        return False
    LOGGER.info("Serving credit balances from the v2 ledger")
    return True


def serve_v2_active(session: DbSession) -> bool:
    """
    `serve_v2` is configured and the fold has closed at least one period.

    Evaluated on every call (one primary-key read), so every process of the
    node switches at the same moment, the first period the fold closes, and
    never serves an empty lot cache.
    """
    global _not_ready_logged

    if not serve_v2_configured():
        return False
    if last_closed_period(session) is None:
        if not _not_ready_logged:
            LOGGER.error(_NOT_READY_MESSAGE)
            _not_ready_logged = True
        return False
    return True


# --- balance ------------------------------------------------------------------


def _owner_matches(address: str):
    return BillingResourceDb.owner == normalize_address(address)


def _accrual_since(session: DbSession) -> Optional[dt.datetime]:
    last_closed = last_closed_period(session)
    return None if last_closed is None else period_end(last_closed)


def _seconds(start: dt.datetime, end: dt.datetime) -> int:
    if end <= start:
        return 0
    return int((end - start).total_seconds())


def _accruing_resources(
    session: DbSession, address: str, since: dt.datetime, now: dt.datetime
) -> Iterable[BillingResourceDb]:
    stmt = (
        select(BillingResourceDb)
        .where(
            _owner_matches(address),
            BillingResourceDb.payment_type == PaymentType.credit,
            BillingResourceDb.type.in_(ACCRUING_TYPES),
            or_(
                BillingResourceDb.first_confirmed_at.is_(None),
                BillingResourceDb.first_confirmed_at < now,
            ),
            or_(
                BillingResourceDb.end_of_life.is_(None),
                BillingResourceDb.end_of_life > since,
            ),
        )
        .order_by(BillingResourceDb.item_hash)
    )
    return session.execute(stmt).scalars().all()


def provisional_accrual(
    session: DbSession, address: str, since: dt.datetime, now: dt.datetime
) -> int:
    """
    Credits the live credit-paid resources of `address` consume between
    `since` (end of the last closed period) and `now`, at the rate in force at
    `since`: per resource `ceil(rate × seconds)` with
    `seconds = min(now, end_of_life) - max(since, first_confirmed_at)`, clipped
    at 0. Resources whose rate cannot be computed accrue nothing (the fold
    excludes them as unpriceable).
    """
    if now <= since:
        return 0
    pricing = None
    total = 0
    for resource in _accruing_resources(session, address, since, now):
        if _is_on_demand_program(resource):
            continue
        start = max(since, resource.first_confirmed_at or since)
        end = min(now, resource.end_of_life or now)
        seconds = _seconds(start, end)
        if seconds <= 0:
            continue
        if pricing is None:
            pricing = pricing_model_at(session, since)
        try:
            rate = rate_for(session, resource, since, pricing=pricing)
        except CostSpecError:
            continue
        total += execution_amount(rate, seconds)
    return total


def _lots_total(session: DbSession, address: str, now: dt.datetime) -> int:
    return int(
        session.execute(
            select(func.coalesce(func.sum(CreditLotDb.amount_remaining), 0)).where(
                CreditLotDb.address == normalize_address(address),
                CreditLotDb.amount_remaining > 0,
                or_(CreditLotDb.expiration.is_(None), CreditLotDb.expiration > now),
            )
        ).scalar_one()
    )


def get_credit_balance_v2(
    session: DbSession, address: str, now: dt.datetime, closed_only: bool = False
) -> int:
    """
    Remaining credits of `address` at `now`: unexpired lots minus (unless
    `closed_only`) the provisional accrual since the last closed period.
    Never negative.
    """
    total = _lots_total(session, address, now)
    if not closed_only:
        since = _accrual_since(session)
        if since is not None:
            total -= provisional_accrual(session, address, since, now)
    return max(0, total)


def get_credit_balance_details_v2(
    session: DbSession,
    address: str,
    now: Optional[dt.datetime] = None,
    closed_only: bool = False,
) -> Tuple[int, List[CreditBalanceDetail]]:
    """
    `get_credit_balance_v2` with a per-expiration breakdown: the provisional
    accrual is drained from the lots in the fold's drain order, then the
    remainders are grouped by expiration, sorted as the legacy breakdown
    (non-expiring first, then by expiration). `now` defaults to the wall clock.
    """
    if now is None:
        now = utc_now()
    rows = session.execute(
        select(
            CreditLotDb.period,
            CreditLotDb.kind,
            CreditLotDb.business_key,
            CreditLotDb.amount_remaining,
            CreditLotDb.expiration,
        ).where(
            CreditLotDb.address == normalize_address(address),
            CreditLotDb.amount_remaining > 0,
            or_(CreditLotDb.expiration.is_(None), CreditLotDb.expiration > now),
        )
    ).all()
    lots = sorted(rows, key=lambda r: (r.period, KIND_ORDER[r.kind], r.business_key))
    remaining = [row.amount_remaining for row in lots]

    to_drain = 0
    if not closed_only:
        since = _accrual_since(session)
        if since is not None:
            to_drain = provisional_accrual(session, address, since, now)
    for index, amount in enumerate(remaining):
        if to_drain <= 0:
            break
        take = min(amount, to_drain)
        remaining[index] -= take
        to_drain -= take

    by_expiration: dict = {}
    for row, amount in zip(lots, remaining):
        if amount > 0:
            by_expiration[row.expiration] = (
                by_expiration.get(row.expiration, 0) + amount
            )
    details = [
        CreditBalanceDetail(expiration_date=expiration, amount=amount)
        for expiration, amount in sorted(
            by_expiration.items(),
            key=lambda item: (item[0] is not None, item[0] or dt.datetime.min),
        )
    ]
    return sum(remaining), details


def get_credit_balance_any(
    session: DbSession, address: str, now: Optional[dt.datetime] = None
) -> int:
    """
    The credit balance every reader uses: the v2 ledger when `serve_v2` is
    active, otherwise the legacy `credit_balances` lots (unchanged behaviour).
    """
    if serve_v2_active(session):
        return get_credit_balance_v2(
            session, address, now if now is not None else utc_now()
        )
    return get_credit_balance(session, address, now)


# --- removal cron ---------------------------------------------------------------


def addresses_with_ledger_activity_since(
    session: DbSession, since: dt.datetime
) -> List[str]:
    """
    Addresses (ledger form) with a ledger row in a period ending after
    `since`, i.e. in the period containing `since` or a later one. Sorted, so
    a capped cron run defers accounts deterministically.
    """
    stmt = (
        select(CreditLedgerDb.address)
        .where(
            CreditLedgerDb.address.is_not(None),
            CreditLedgerDb.period >= floor_hour(since),
        )
        .distinct()
        .order_by(CreditLedgerDb.address)
    )
    return [address for address in session.execute(stmt).scalars() if address]


def owners_of_live_credit_resources(session: DbSession) -> List[str]:
    """
    Owners (ledger form) of credit-paid resources that are still alive, so the
    removal cron checks accounts with running resources even when the fold
    writes no ledger rows (stalled fold).
    """
    stmt = (
        select(BillingResourceDb.owner)
        .where(
            BillingResourceDb.payment_type == PaymentType.credit,
            BillingResourceDb.type.in_(ACCRUING_TYPES),
            BillingResourceDb.end_of_life.is_(None),
        )
        .distinct()
        .order_by(BillingResourceDb.owner)
    )
    return list(session.execute(stmt).scalars())


def fold_lag_exceeded(
    session: DbSession, now: dt.datetime, max_lag: dt.timedelta
) -> Optional[dt.datetime]:
    """
    The last closed period if the fold is more than `max_lag` behind `now`
    (end of that period), None otherwise or when nothing was ever closed.
    """
    last_closed = last_closed_period(session)
    if last_closed is not None and period_end(last_closed) < now - max_lag:
        return last_closed
    return None
