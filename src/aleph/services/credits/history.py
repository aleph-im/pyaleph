"""
Credit history and consumed credits served under `aleph.credits.serve_v2`
(spec 10): the frozen legacy `credit_history` rows before the cutover period,
the `credit_ledger` rows from it on.
"""

import datetime as dt
from decimal import Decimal
from typing import Dict, Optional, Tuple

from aleph_message.models import MessageType, PaymentType
from configmanager import Config

from aleph.db.accessors.balances import get_total_consumed_credits
from aleph.db.accessors.credit_ledger import (
    consumed_execution_credits,
    get_billing_resource,
)
from aleph.db.models.credit_ledger import BillingResourceDb
from aleph.services.credits.balance import last_closed_period
from aleph.services.credits.cost_spec import CostSpecError
from aleph.services.credits.fold import execution_amount
from aleph.services.credits.lifecycle import FoldConfig
from aleph.services.credits.periods import PERIOD, floor_hour, period_end
from aleph.services.credits.rates import rate_for
from aleph.services.pricing_utils import get_pricing_versions
from aleph.types.db_session import DbSession


def history_cutover(config: Config) -> Optional[dt.datetime]:
    """
    The first period served from the ledger: `credits.genesis.period`, or the
    fold's first period when no genesis is configured. None when neither is
    set (then both tables are served whole).
    """
    fold_cfg = FoldConfig.from_config(config)
    return fold_cfg.genesis_period or fold_cfg.first_period


def store_consumed_credits(
    session: DbSession,
    resource: BillingResourceDb,
    since: Optional[dt.datetime],
    last_closed: dt.datetime,
) -> int:
    """
    Credits a credit-paid STORE consumed over the closed periods from `since`
    (or its confirmation hour) to `last_closed` included, recomputed with the
    fold's per-file formula: per period `ceil(rate × seconds)` with the rate in
    force at the period start and the seconds of the period the file was
    alive. The ledger only keeps per-address storage rows, so the per-file
    figure is computed on demand. An unpriceable period counts 0, as the fold
    excludes it.
    """
    if resource.first_confirmed_at is None:
        return 0
    period = floor_hour(resource.first_confirmed_at)
    if since is not None:
        period = max(period, since)
    if resource.end_of_life is not None:
        last_closed = min(last_closed, floor_hour(resource.end_of_life))

    versions = get_pricing_versions(session)
    version_index = 0
    rates: Dict[str, Optional[Decimal]] = {}
    total = 0
    while period <= last_closed:
        while (
            version_index + 1 < len(versions)
            and versions[version_index + 1][0] <= period
        ):
            version_index += 1
        _, version, model = versions[version_index]

        lo = max(resource.first_confirmed_at, period)
        end = period_end(period)
        hi = end if resource.end_of_life is None else min(resource.end_of_life, end)
        if hi > lo:
            if version not in rates:
                try:
                    rates[version] = rate_for(
                        session,
                        resource,
                        period,
                        pricing=(version, model),
                        write=False,
                    )
                except CostSpecError:
                    rates[version] = None
            rate = rates[version]
            if rate is not None:
                total += execution_amount(rate, int((hi - lo).total_seconds()))
        period += PERIOD
    return total


def consumed_credits_v2(
    session: DbSession, item_hash: str, cutover: Optional[dt.datetime]
) -> Tuple[int, int, int]:
    """
    (legacy, execution, storage) credits consumed by a resource: the legacy
    `credit_expense` rows before `cutover`, the billed ledger `execution` rows
    from it on, and for a credit-paid STORE its per-file storage charge over
    the closed periods from it on.
    """
    legacy = int(
        get_total_consumed_credits(session, item_hash=item_hash, before=cutover)
    )
    execution = consumed_execution_credits(session, item_hash, since=cutover)
    storage = 0
    last_closed = last_closed_period(session)
    resource = get_billing_resource(session, item_hash)
    if (
        last_closed is not None
        and resource is not None
        and resource.type == MessageType.store
        and resource.payment_type == PaymentType.credit
    ):
        storage = store_consumed_credits(session, resource, cutover, last_closed)
    return legacy, execution, storage
