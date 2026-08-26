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
from aleph.types.cost import CostType, ProductPricing, resolve_price_type_key
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
    assert price_type == ProductPriceType.VPROGRAM


def test_vprogram_pricing_defaults_match_confidential():
    vprogram = ProductPricing.from_aggregate(
        ProductPriceType.VPROGRAM, DEFAULT_PRICE_AGGREGATE
    )
    confidential = ProductPricing.from_aggregate(
        ProductPriceType.INSTANCE_CONFIDENTIAL, DEFAULT_PRICE_AGGREGATE
    )
    assert vprogram.type == ProductPriceType.VPROGRAM
    assert vprogram.price.compute_unit.credit == confidential.price.compute_unit.credit
    assert vprogram.price.storage.credit == confidential.price.storage.credit
    assert vprogram.compute_unit.disk_mib == 20480
    assert vprogram.compute_unit.memory_mib == 2048
    assert vprogram.compute_unit.vcpus == 1


def test_vprogram_pricing_falls_back_to_confidential_when_key_missing():
    aggregate = {
        k: v
        for k, v in DEFAULT_PRICE_AGGREGATE.items()
        if k != ProductPriceType.VPROGRAM
    }
    assert ProductPriceType.VPROGRAM not in aggregate
    assert (
        resolve_price_type_key(ProductPriceType.VPROGRAM, aggregate.keys())
        == ProductPriceType.INSTANCE_CONFIDENTIAL
    )
    pricing = ProductPricing.from_aggregate(ProductPriceType.VPROGRAM, aggregate)
    # The product keeps its own identity; only the numbers are borrowed.
    assert pricing.type == ProductPriceType.VPROGRAM
    assert pricing.price.compute_unit.credit == Decimal("28500")


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
    assert execution.name == ProductPriceType.VPROGRAM
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
