"""
Utility functions for pricing model creation and management.
"""

import datetime as dt
from typing import Dict, List, Union

from aleph.db.accessors.aggregates import (
    get_aggregate_elements,
    merge_aggregate_elements,
)
from aleph.db.models import AggregateElementDb
from aleph.toolkit.constants import (
    DEFAULT_PRICE_AGGREGATE,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    ProductPriceType,
)
from aleph.types.cost import ProductPricing
from aleph.types.db_session import DbSession


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge ``override`` onto a copy of ``base``.

    Nested dicts are merged key-by-key so a *partial* override inherits any
    leaf it does not set from ``base`` instead of dropping it. Non-dict values
    (and dict-vs-non-dict conflicts) are taken from ``override``.

    Used to backfill the pricing model from ``DEFAULT_PRICE_AGGREGATE``: a
    pricing aggregate update that replaced e.g. ``INSTANCE.price.compute_unit``
    without a ``credit`` field — as happened on chain during the 2025-02 →
    2025-10 window, before credit pricing existed — otherwise resolves to
    ``cost_credit = 0`` for every item priced in that window.
    """
    result = dict(base)
    for key, value in override.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = _deep_merge(existing, value)
        else:
            result[key] = value
    return result


def build_pricing_model_from_aggregate(
    aggregate_content: Dict[Union[ProductPriceType, str], dict],
) -> Dict[ProductPriceType, ProductPricing]:
    """
    Build a complete pricing model from an aggregate content dictionary.

    This function converts the DEFAULT_PRICE_AGGREGATE format or any pricing aggregate
    content into a dictionary of ProductPricing objects that can be used by the cost
    calculation functions.

    Args:
        aggregate_content: Dictionary containing pricing information with ProductPriceType as keys

    Returns:
        Dictionary mapping ProductPriceType to ProductPricing objects
    """
    pricing_model: Dict[ProductPriceType, ProductPricing] = {}

    for price_type, pricing_data in aggregate_content.items():
        try:
            price_type = ProductPriceType(price_type)
            pricing_model[price_type] = ProductPricing.from_aggregate(
                price_type, aggregate_content
            )
        except (KeyError, ValueError) as e:
            # Log the error but continue processing other price types
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to parse pricing for {price_type}: {e}")

    return pricing_model


def build_default_pricing_model() -> Dict[ProductPriceType, ProductPricing]:
    """
    Build the default pricing model from DEFAULT_PRICE_AGGREGATE constant.

    Returns:
        Dictionary mapping ProductPriceType to ProductPricing objects
    """
    return build_pricing_model_from_aggregate(DEFAULT_PRICE_AGGREGATE)


def get_pricing_aggregate_history(session: DbSession) -> List[AggregateElementDb]:
    """
    Get all pricing aggregate updates in chronological order.

    Args:
        session: Database session

    Returns:
        List of AggregateElementDb objects ordered by creation_datetime
    """
    aggregate_elements = get_aggregate_elements(
        session=session, owner=PRICE_AGGREGATE_OWNER, key=PRICE_AGGREGATE_KEY
    )
    return list(aggregate_elements)


def get_pricing_timeline(
    session: DbSession,
) -> List[tuple[dt.datetime, Dict[ProductPriceType, ProductPricing]]]:
    """
    Get the complete pricing timeline with timestamps and pricing models.

    This function returns a chronologically ordered list of pricing changes,
    useful for processing messages in chronological order and applying the
    correct pricing at each point in time.

    This properly merges aggregate elements up to each point in time to create
    the cumulative pricing state, similar to how _update_aggregate works.

    Args:
        session: Database session

    Returns:
        List of tuples containing (timestamp, pricing_model)
    """
    pricing_elements = get_pricing_aggregate_history(session)

    timeline = []

    # Add default pricing as the initial state
    timeline.append(
        (dt.datetime.min.replace(tzinfo=dt.timezone.utc), build_default_pricing_model())
    )

    # Build cumulative pricing models by merging elements up to each timestamp
    elements_so_far = []
    for element in pricing_elements:
        elements_so_far.append(element)

        # Merge all elements up to this point to get the cumulative state.
        # The shallow aggregate-merge semantics (how on-chain elements combine
        # with each other) are unchanged.
        merged_content = merge_aggregate_elements(elements_so_far)

        # Backfill missing leaves from the default aggregate, but only for the
        # product types actually present on chain — types never priced on
        # chain stay absent, preserving the timeline's semantics. This rescues
        # a partial update that replaced e.g. INSTANCE.price.compute_unit
        # without a `credit` field (as happened during the 2025-02 → 2025-10
        # window, before credit pricing existed on chain): the credit leaf
        # falls back to the default instead of resolving to cost_credit=0.
        # On-chain values always win where present.
        full_content = {
            price_type: _deep_merge(
                DEFAULT_PRICE_AGGREGATE.get(price_type, {}), pricing_data
            )
            for price_type, pricing_data in merged_content.items()
        }

        # Build pricing model from the merged content
        pricing_model = build_pricing_model_from_aggregate(full_content)
        timeline.append((element.creation_datetime, pricing_model))

    return timeline
