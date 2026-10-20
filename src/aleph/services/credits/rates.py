import datetime as dt
from decimal import Decimal
from typing import Dict, Optional, Tuple

from aleph.db.accessors.credit_ledger import get_billing_rate, upsert_billing_rate
from aleph.db.models.credit_ledger import BillingResourceDb
from aleph.services.cost import _get_settings
from aleph.services.credits import cost_spec
from aleph.services.pricing_utils import pricing_model_at
from aleph.toolkit.constants import ProductPriceType
from aleph.types.cost import ProductPricing
from aleph.types.db_session import DbSession

PricingVersion = Tuple[str, Dict[ProductPriceType, ProductPricing]]


def rate_for(
    session: DbSession,
    resource: BillingResourceDb,
    at: dt.datetime,
    *,
    pricing: Optional[PricingVersion] = None,
    write: bool = True,
) -> Decimal:
    """
    Credit rate per second of `resource` under the pricing in force at `at`,
    cached per (item_hash, pricing version). A CostSpecError propagates and
    nothing is cached for it.

    `pricing` is the (version, model) of `pricing_model_at(session, at)` when
    the caller already has it: the pricing timeline is then not walked again.

    `write=False` computes a missing rate without caching it: read paths (API,
    admission, removal cron) must not insert into `billing_rates`, where a
    conflicting uncommitted insert from an in-flight fold would block them
    until the fold commits.
    """
    version, model = pricing if pricing is not None else pricing_model_at(session, at)
    cached = get_billing_rate(session, resource.item_hash, version)
    if cached is not None:
        return cached
    rate = cost_spec.rate_from_spec(
        session, resource.cost_spec, resource.item_hash, model, _get_settings(session)
    )
    if write:
        upsert_billing_rate(session, resource.item_hash, version, rate)
    return rate
