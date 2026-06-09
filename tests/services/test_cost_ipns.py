from decimal import Decimal

import pytest
from aleph_message.models import StoreContent

from aleph.services.cost import get_total_and_detailed_costs
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory

IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
MSG_HASH = "7f2d09b2c4e1a8f3d6b5c2e9a1f4d7b8e3c6a9f2d5b8e1c4a7f0d3b6e9c2a5f8"


def _ipns_content(max_size_mib=100):
    return StoreContent.model_validate(
        {
            "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "time": 1780000000.0,
            "item_type": "ipns",
            "item_hash": IPNS_NAME,
            "max_size_mib": max_size_mib,
        }
    )


def test_ipns_costs_contain_quota_and_name_fee(session_factory: DbSessionFactory):
    content = _ipns_content()
    with session_factory() as session:
        total, costs = get_total_and_detailed_costs(session, content, MSG_HASH)

    cost_types = {cost.type for cost in costs}
    assert CostType.STORAGE in cost_types
    assert CostType.IPNS in cost_types
    assert total > Decimal(0)

    storage_cost = next(c for c in costs if c.type == CostType.STORAGE)
    fee_cost = next(c for c in costs if c.type == CostType.IPNS)
    assert storage_cost.cost_hold > Decimal(0)
    assert fee_cost.cost_hold > Decimal(0)


def test_ipns_costs_scale_with_quota(session_factory: DbSessionFactory):
    with session_factory() as session:
        total_small, _ = get_total_and_detailed_costs(
            session, _ipns_content(max_size_mib=10), MSG_HASH
        )
        total_large, _ = get_total_and_detailed_costs(
            session, _ipns_content(max_size_mib=1000), MSG_HASH
        )
    assert total_large > total_small
