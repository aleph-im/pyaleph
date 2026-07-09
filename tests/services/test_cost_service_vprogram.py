from decimal import Decimal
from typing import cast

import pytest
from aleph_message.models import VerifiableProgramContent
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH

from aleph.services.cost import _get_product_price_type, get_detailed_costs
from aleph.toolkit.constants import (
    DEFAULT_PRICE_AGGREGATE,
    DEFAULT_SETTINGS_AGGREGATE,
    ProductPriceType,
)
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.settings import Settings


@pytest.fixture
def vprogram_content() -> VerifiableProgramContent:
    return VerifiableProgramContent.model_validate(VPROGRAM_CONTENT)


def test_vprogram_price_type(vprogram_content):
    settings = Settings.from_aggregate(DEFAULT_SETTINGS_AGGREGATE)
    price_type = _get_product_price_type(
        vprogram_content, settings, DEFAULT_PRICE_AGGREGATE
    )
    assert price_type == ProductPriceType.INSTANCE_CONFIDENTIAL


def test_vprogram_detailed_costs(
    session_factory: DbSessionFactory,
    vprogram_content,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        costs = get_detailed_costs(
            session, vprogram_content, item_hash=VPROGRAM_ITEM_HASH
        )

    execution_costs = [c for c in costs if c.type == CostType.EXECUTION]
    assert len(execution_costs) == 1
    execution = execution_costs[0]
    assert execution.name == ProductPriceType.INSTANCE_CONFIDENTIAL
    assert execution.payment_type == "credit"
    # 2 vcpus / 2048 MiB memory = 2 compute units on the instance_confidential
    # tier; the credit price is per hour in the aggregate.
    assert Decimal(execution.cost_credit) > 0
    assert execution.owner == VPROGRAM_CONTENT["address"]

    # Verity-bound volumes are STORE-paid artifacts, not execution volumes:
    # no cost row should reference the verified volume's ref.
    volumes = cast(list, VPROGRAM_CONTENT["volumes"])
    verified_volume_ref = volumes[0]["ref"]
    assert all(c.ref != verified_volume_ref for c in costs)
