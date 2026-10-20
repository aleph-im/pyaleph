import copy
import datetime as dt
from decimal import Decimal
from typing import Any, Dict

from aleph_message.models import InstanceContent

from aleph.db.accessors.files import insert_message_file_pin
from aleph.db.models import StoredFileDb
from aleph.services.cost import (
    _get_settings,
    get_detailed_costs,
    get_total_and_detailed_costs,
)
from aleph.services.credits.cost_spec import rate_from_spec
from aleph.services.pricing_utils import build_pricing_model_from_aggregate
from aleph.toolkit.constants import DEFAULT_PRICE_AGGREGATE, MiB, ProductPriceType
from aleph.toolkit.costs import format_cost
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType

OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
ROOTFS_REF = "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595"
DATA_REF = "ab" * 32
DATA_SIZE_BYTES = 3 * MiB + 12345  # not a whole number of MiB


def instance_content(**overrides: Any) -> Dict[str, Any]:
    content: Dict[str, Any] = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {"ref": ROOTFS_REF, "use_latest": False},
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": OWNER,
        "volumes": [
            {"ref": DATA_REF, "use_latest": False, "mount": "/data"},
        ],
        "metadata": {"name": "spec rates"},
        "resources": {"vcpus": 2, "memory": 4096, "seconds": 30},
        "allow_amend": False,
        "payment": {"chain": "ETH", "type": "credit"},
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    content.update(overrides)
    return content


def default_model():
    return build_pricing_model_from_aggregate(copy.deepcopy(DEFAULT_PRICE_AGGREGATE))


def doubled_model(price_type: ProductPriceType):
    aggregate = copy.deepcopy(DEFAULT_PRICE_AGGREGATE)
    key = price_type if price_type in aggregate else price_type.value
    credit = Decimal(aggregate[key]["price"]["compute_unit"]["credit"])
    aggregate[key]["price"]["compute_unit"]["credit"] = str(credit * 2)
    return build_pricing_model_from_aggregate(aggregate)


def pin_file(session, ref: str, size_bytes: int) -> None:
    file_hash = ref[::-1]
    session.add(StoredFileDb(hash=file_hash, size=size_bytes, type=FileType.FILE))
    session.flush()
    insert_message_file_pin(
        session=session,
        file_hash=file_hash,
        owner=OWNER,
        item_hash=ref,
        ref=None,
        created=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
    )


def test_rate_from_spec_matches_live_computation_for_instance(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    content = InstanceContent.model_validate(instance_content())
    with session_factory() as session:
        pin_file(session, DATA_REF, DATA_SIZE_BYTES)
        live_total, live_costs = get_total_and_detailed_costs(session, content, "live")
        assert any(c.type == CostType.EXECUTION_VOLUME_INMUTABLE for c in live_costs)

        spec = {
            "type": "INSTANCE",
            "content": instance_content(),
            "volume_sizes_bytes": {DATA_REF: DATA_SIZE_BYTES},
            "file_size_bytes": None,
        }
        rate = rate_from_spec(
            session, spec, "live", default_model(), _get_settings(session)
        )

    assert rate == live_total
    assert rate > 0


def test_rate_from_spec_uses_given_pricing_model_not_db(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    spec = {
        "type": "INSTANCE",
        "content": instance_content(volumes=[]),
        "volume_sizes_bytes": {},
        "file_size_bytes": None,
    }
    with session_factory() as session:
        settings = _get_settings(session)
        base = rate_from_spec(session, spec, "h", default_model(), settings)
        doubled = rate_from_spec(
            session, spec, "h", doubled_model(ProductPriceType.INSTANCE), settings
        )
        # The DB aggregate is untouched: the live computation gives the base.
        live_total, _ = get_total_and_detailed_costs(
            session, InstanceContent.model_validate(spec["content"]), "h"
        )

    assert live_total == base
    assert doubled > base
    # Only the compute-unit price changed; storage is covered by the allowance.
    assert abs(doubled - base * 2) <= Decimal("1e-17")


def test_rate_from_spec_for_store_uses_spec_file_size(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    store_content = {
        "address": OWNER,
        "time": 1701099523.849,
        "item_type": "ipfs",
        "item_hash": "Qm" + "a" * 44,
        "payment": {"chain": "ETH", "type": "credit"},
    }
    spec = {
        "type": "STORE",
        "content": store_content,
        "volume_sizes_bytes": {},
        "file_size_bytes": 100 * MiB,
    }
    model = default_model()
    with session_factory() as session:  # no files row at all
        rate = rate_from_spec(session, spec, "s", model, _get_settings(session))

    price_per_mib_second = model[ProductPriceType.STORAGE].price.storage.credit / 3600
    assert rate == format_cost(Decimal(100) * price_per_mib_second)


def test_rate_from_spec_for_gpu_instance_uses_given_model(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    content = instance_content(
        volumes=[],
        resources={"vcpus": 8, "memory": 16384, "seconds": 30},
        requirements={
            "node": {
                "node_hash": "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
            },
            "gpu": [
                {
                    "vendor": "NVIDIA",
                    "device_name": "AD102 [GeForce RTX 4090]",
                    "device_class": "0300",
                    "device_id": "10de:2684",
                }
            ],
        },
    )
    spec = {
        "type": "INSTANCE",
        "content": content,
        "volume_sizes_bytes": {},
        "file_size_bytes": None,
    }
    parsed = InstanceContent.model_validate(content)
    with session_factory() as session:
        settings = _get_settings(session)
        live_total, live_costs = get_total_and_detailed_costs(session, parsed, "g")
        base = rate_from_spec(session, spec, "g", default_model(), settings)
        doubled = rate_from_spec(
            session,
            spec,
            "g",
            doubled_model(ProductPriceType.INSTANCE_GPU_STANDARD),
            settings,
        )
        detailed = get_detailed_costs(session, parsed, "g")

    assert base == live_total
    execution = sum(
        Decimal(c.cost_credit) for c in detailed if c.type == CostType.EXECUTION
    )
    assert execution > 0
    # Doubling the standard-tier unit price adds exactly the execution cost.
    assert doubled == format_cost(base + execution)
