import datetime as dt
from decimal import Decimal

import pytest
from aleph_message.models import VerifiableProgramContent
from messages.test_vprogram import VPROGRAM_CONTENT, VPROGRAM_ITEM_HASH
from pydantic import ValidationError

from aleph.db.accessors.files import insert_message_file_pin
from aleph.db.models import StoredFileDb
from aleph.schemas.cost_estimation_messages import CostEstimationVProgramContent
from aleph.services.cost import (
    _get_product_price_type,
    get_detailed_costs,
    get_total_and_detailed_costs,
)
from aleph.toolkit.constants import (
    DEFAULT_PRICE_AGGREGATE,
    DEFAULT_SETTINGS_AGGREGATE,
    HOUR,
    ProductPriceType,
)
from aleph.types.cost import CostType, ProductPricing, RefVolume, resolve_price_type_key
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.types.settings import Settings

MIB = 1024 * 1024
BUNDLE_REF = "ba" * 32


def artifact_refs() -> dict[str, str]:
    """Artifact name -> STORE message hash, as the cost rows name them."""
    refs = {
        "workload": VPROGRAM_CONTENT["workload"]["ref"],
        "workload:hash_tree": VPROGRAM_CONTENT["workload"]["hash_tree"],
    }
    for i, volume in enumerate(VPROGRAM_CONTENT["volumes"]):
        refs[f"#{i}:{volume['comment']}"] = volume["ref"]
        refs[f"#{i}:{volume['comment']}:hash_tree"] = volume["hash_tree"]
    return refs


def pin_file(session: DbSession, ref: str, size_bytes: int) -> None:
    file_hash = ref[::-1]
    session.add(StoredFileDb(hash=file_hash, size=size_bytes, type=FileType.FILE))
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=file_hash,
        owner=VPROGRAM_CONTENT["address"],
        item_hash=ref,
        ref=None,
        created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    )


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


def test_vprogram_artifacts_under_allowance_are_free(
    session_factory: DbSessionFactory,
    vprogram_content,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        for ref in artifact_refs().values():
            pin_file(session, ref, 1 * MIB)
        session.commit()

        costs = get_detailed_costs(
            session, vprogram_content, item_hash=VPROGRAM_ITEM_HASH
        )

    by_name = {c.name: c for c in costs if c.type == CostType.EXECUTION_VPROGRAM_VOLUME}
    assert set(by_name) == set(artifact_refs())
    for name, ref in artifact_refs().items():
        assert by_name[name].ref == ref
        assert Decimal(by_name[name].cost_credit) > 0

    discount = next(c for c in costs if c.type == CostType.EXECUTION_VOLUME_DISCOUNT)
    volume_total = sum(Decimal(c.cost_credit) for c in by_name.values())
    # 4 x 1 MiB is far under the 2 CU x 20480 MiB allowance: fully discounted.
    assert Decimal(discount.cost_credit) == -volume_total

    execution = next(c for c in costs if c.type == CostType.EXECUTION)
    total = sum(Decimal(c.cost_credit) for c in costs)
    assert total == Decimal(execution.cost_credit)


def test_vprogram_artifacts_over_allowance_bill_the_excess(
    session_factory: DbSessionFactory,
    vprogram_content,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    workload_mib = 50 * 1024
    # The other 3 artifacts (workload:hash_tree + the one verified volume's
    # ref and hash_tree) must sit above the per-artifact minimum-credit-cost
    # floor (1 credit/hour, ~5.57 MiB at this price) so the residual can be
    # compared against a plain linear formula.
    other_mib = 10
    with session_factory() as session:
        for name, ref in artifact_refs().items():
            pin_file(
                session,
                ref,
                workload_mib * MIB if name == "workload" else other_mib * MIB,
            )
        session.commit()

        costs = get_detailed_costs(
            session, vprogram_content, item_hash=VPROGRAM_ITEM_HASH
        )

    pricing = ProductPricing.from_aggregate(
        ProductPriceType.VPROGRAM, DEFAULT_PRICE_AGGREGATE
    )
    price_per_mib_credit = pricing.price.storage.credit / HOUR
    footprint_mib = Decimal(workload_mib + 3 * other_mib)
    allowance_mib = Decimal(2 * 20480)

    execution = next(c for c in costs if c.type == CostType.EXECUTION)
    total = sum(Decimal(c.cost_credit) for c in costs)
    residual = total - Decimal(execution.cost_credit)
    expected = (footprint_mib - allowance_mib) * price_per_mib_credit
    assert abs(residual - expected) < Decimal("0.000001")


def test_vprogram_unpinned_artifact_is_skipped(
    session_factory: DbSessionFactory,
    vprogram_content,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    # No file pins at all: no artifact rows, no crash (mirrors legacy
    # missing-ref behaviour for immutable volumes).
    with session_factory() as session:
        costs = get_detailed_costs(
            session, vprogram_content, item_hash=VPROGRAM_ITEM_HASH
        )
    assert not [c for c in costs if c.type == CostType.EXECUTION_VPROGRAM_VOLUME]


def test_vprogram_extra_volumes_are_billed(
    session_factory: DbSessionFactory,
    vprogram_content,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    with session_factory() as session:
        pin_file(session, BUNDLE_REF, 3 * 1024 * MIB)
        session.commit()

        runtime = RefVolume(
            CostType.EXECUTION_VPROGRAM_VOLUME, BUNDLE_REF, False, "runtime"
        )
        _, costs = get_total_and_detailed_costs(
            session, vprogram_content, VPROGRAM_ITEM_HASH, extra_volumes=[runtime]
        )

    row = next(c for c in costs if c.name == "runtime")
    assert row.type == CostType.EXECUTION_VPROGRAM_VOLUME
    assert row.ref == BUNDLE_REF
    assert Decimal(row.cost_credit) > 0


def test_vprogram_estimate_uses_estimated_sizes_without_pins(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    content_dict = {
        **VPROGRAM_CONTENT,
        "workload": {
            **VPROGRAM_CONTENT["workload"],
            "estimated_size_mib": 50 * 1024,
            "estimated_hash_tree_size_mib": 400,
        },
        "volumes": [
            {
                **VPROGRAM_CONTENT["volumes"][0],
                "estimated_size_mib": 1024,
                "estimated_hash_tree_size_mib": 8,
            }
        ],
        "runtime_estimated_size_mib": 3 * 1024,
    }
    content = CostEstimationVProgramContent.model_validate(content_dict)

    with session_factory() as session:
        # Nothing is pinned: every size must come from the estimates.
        costs = get_detailed_costs(session, content, item_hash=VPROGRAM_ITEM_HASH)

    by_name = {c.name: c for c in costs if c.type == CostType.EXECUTION_VPROGRAM_VOLUME}
    assert set(by_name) == {
        "workload",
        "workload:hash_tree",
        "#0:model weights",
        "#0:model weights:hash_tree",
        "runtime",
    }
    pricing = ProductPricing.from_aggregate(
        ProductPriceType.VPROGRAM, DEFAULT_PRICE_AGGREGATE
    )
    price_per_mib_credit = pricing.price.storage.credit / HOUR
    footprint_mib = Decimal(50 * 1024 + 400 + 1024 + 8 + 3 * 1024)
    allowance_mib = Decimal(2 * 20480)
    execution = next(c for c in costs if c.type == CostType.EXECUTION)
    total = sum(Decimal(c.cost_credit) for c in costs)
    expected = (footprint_mib - allowance_mib) * price_per_mib_credit
    assert abs(total - Decimal(execution.cost_credit) - expected) < Decimal("0.000001")


def test_vprogram_runtime_estimate_wins_over_an_extra_runtime_volume(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Cost rows are keyed by (type, name): a caller-supplied `runtime`
    volume must not add a second row next to the one already built from
    `runtime_estimated_size_mib`. The estimate wins."""
    runtime_estimate_mib = 3 * 1024
    content_dict = {
        **VPROGRAM_CONTENT,
        "runtime_estimated_size_mib": runtime_estimate_mib,
    }
    content = CostEstimationVProgramContent.model_validate(content_dict)

    with session_factory() as session:
        # Pinned at a size nothing like the estimate, so the surviving row is
        # unambiguous.
        pin_file(session, BUNDLE_REF, 50 * 1024 * MIB)
        session.commit()

        costs = get_detailed_costs(
            session,
            content,
            item_hash=VPROGRAM_ITEM_HASH,
            extra_volumes=[
                RefVolume(
                    CostType.EXECUTION_VPROGRAM_VOLUME, BUNDLE_REF, False, "runtime"
                )
            ],
        )

    runtime_rows = [c for c in costs if c.name == "runtime"]
    assert len(runtime_rows) == 1
    row = runtime_rows[0]
    assert row.type == CostType.EXECUTION_VPROGRAM_VOLUME
    # Sized from the estimate, and pointing at the manifest ref rather than
    # the bundle the dropped extra volume named.
    assert row.ref == VPROGRAM_CONTENT["runtime"]["ref"]
    pricing = ProductPricing.from_aggregate(
        ProductPriceType.VPROGRAM, DEFAULT_PRICE_AGGREGATE
    )
    price_per_mib_credit = pricing.price.storage.credit / HOUR
    expected = Decimal(runtime_estimate_mib) * price_per_mib_credit
    assert abs(Decimal(row.cost_credit) - expected) < Decimal("0.000001")


def test_vprogram_verified_volume_without_comment_gets_a_fallback_name(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """An empty `comment` must not produce a bare `#0:` row name: fall back
    to the cost type, as the immutable-volume branch does for `mount`."""
    content_dict = {
        **VPROGRAM_CONTENT,
        "volumes": [{**VPROGRAM_CONTENT["volumes"][0], "comment": ""}],
    }
    content = VerifiableProgramContent.model_validate(content_dict)

    with session_factory() as session:
        volume = VPROGRAM_CONTENT["volumes"][0]
        pin_file(session, volume["ref"], 10 * MIB)
        pin_file(session, volume["hash_tree"], 10 * MIB)
        session.commit()

        costs = get_detailed_costs(session, content, item_hash=VPROGRAM_ITEM_HASH)

    names = {c.name for c in costs if c.type == CostType.EXECUTION_VPROGRAM_VOLUME}
    assert "#0:EXECUTION_VPROGRAM_VOLUME" in names
    assert "#0:EXECUTION_VPROGRAM_VOLUME:hash_tree" in names
    assert "#0:" not in names


@pytest.mark.parametrize(
    "content_overrides",
    [
        {"runtime_estimated_size_mib": -1},
        {"workload": {**VPROGRAM_CONTENT["workload"], "estimated_size_mib": -1}},
        {
            "workload": {
                **VPROGRAM_CONTENT["workload"],
                "estimated_hash_tree_size_mib": -1,
            }
        },
        {
            "volumes": [
                {**VPROGRAM_CONTENT["volumes"][0], "estimated_size_mib": -1},
            ]
        },
        {
            "volumes": [
                {
                    **VPROGRAM_CONTENT["volumes"][0],
                    "estimated_hash_tree_size_mib": -1,
                },
            ]
        },
    ],
)
def test_vprogram_negative_estimates_are_rejected(content_overrides):
    """A negative estimate would subtract from the billed footprint."""
    with pytest.raises(ValidationError):
        CostEstimationVProgramContent.model_validate(
            {**VPROGRAM_CONTENT, **content_overrides}
        )


def test_vprogram_estimate_rejects_more_than_max_verified_volumes():
    from aleph_message.models.execution.vprogram import MAX_VERIFIED_VOLUMES
    from pydantic import ValidationError

    volume = VPROGRAM_CONTENT["volumes"][0]
    content_dict = {
        **VPROGRAM_CONTENT,
        "volumes": [volume] * (MAX_VERIFIED_VOLUMES + 1),
    }
    with pytest.raises(ValidationError):
        CostEstimationVProgramContent.model_validate(content_dict)
    CostEstimationVProgramContent.model_validate(
        {**VPROGRAM_CONTENT, "volumes": [volume] * MAX_VERIFIED_VOLUMES}
    )
