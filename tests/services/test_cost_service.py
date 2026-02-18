from decimal import Decimal
from math import ceil
from unittest.mock import Mock

import pytest
from aleph_message.models import (
    ExecutableContent,
    InstanceContent,
    Payment,
    PaymentType,
)

from aleph.db.models import AggregateDb
from aleph.schemas.cost_estimation_messages import CostEstimationProgramContent
from aleph.services.cost import (
    _get_additional_storage_price,
    _get_price_aggregate,
    _get_product_price,
    _get_settings,
    _get_settings_aggregate,
    get_total_and_detailed_costs,
)
from aleph.toolkit.constants import HOUR, MIN_CREDIT_COST_PER_HOUR
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory


class StoredFileDb:
    pass


@pytest.fixture
def fixture_hold_instance_message() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_hold_instance_message_complete() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 50,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 100,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 100,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 100,
            },
        ],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_flow_instance_message() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "payment": {
            "chain": "AVAX",
            "receiver": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "type": "superfluid",
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_flow_instance_message_complete() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 50,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 1024,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 10240,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 51200,
            },
        ],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "payment": {
            "chain": "AVAX",
            "receiver": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "type": "superfluid",
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_hold_program_message_complete() -> ExecutableContent:
    content = {
        "on": {"http": True, "persistent": False},
        "code": {
            "ref": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
            "encoding": "zip",
            "entrypoint": "main:app",
            "use_latest": True,
            "estimated_size_mib": 2048,
        },
        "time": 1740986893.735,
        "type": "vm-function",
        "address": "0xAD8ac12Ae5bC9f6D902cBDd2f0Dd70F43e522BC2",
        "payment": {"type": "hold", "chain": "ETH"},
        "runtime": {
            "ref": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
            "comment": "Aleph Alpine Linux with Python 3.8",
            "use_latest": True,
            "estimated_size_mib": 1024,
        },
        "data": {
            "ref": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
            "mount": "/data",
            "encoding": "zip",
            "use_latest": True,
            "estimated_size_mib": 2048,
        },
        "volumes": [
            {
                "mount": "/mount1",
                "ref": "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
                "use_latest": True,
                "estimated_size_mib": 512,
            },
            {
                "mount": "/mount2",
                "persistence": "host",
                "name": "pers1",
                "size_mib": 1024,
            },
        ],
        "metadata": {"name": "My program", "description": "My program description"},
        "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
        "variables": {},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
    }

    return CostEstimationProgramContent.model_validate(content)


def test_compute_cost(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_hold_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session, content=fixture_hold_instance_message, item_hash="abab"
        )
        assert cost == Decimal("1000")


def test_compute_cost_conf(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_hold_instance_message,
):
    message_dict = fixture_hold_instance_message.model_dump()

    # Convert the message to conf
    message_dict["environment"].update(
        {
            "hypervisor": "qemu",  # Add qemu to the environment
            "trusted_execution": {
                "policy": 1,
                "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
            },
        }
    )

    rebuilt_message = InstanceContent.model_validate(message_dict)

    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session, content=rebuilt_message, item_hash="abab"
        )
        assert cost == 2000


def test_get_additional_storage_price(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_hold_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        content = fixture_hold_instance_message
        settings = _get_settings(session)
        pricing = _get_product_price(
            session=session, content=content, settings=settings
        )

        cost = _get_additional_storage_price(
            session=session,
            content=content,
            item_hash="abab",
            pricing=pricing,
            payment_type=PaymentType.hold,
        )
        additional_cost = sum(c.cost_hold for c in cost)

        assert additional_cost == 0


def test_compute_cost_instance_complete(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_hold_instance_message_complete,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session,
            content=fixture_hold_instance_message_complete,
            item_hash="abab",
        )
        assert cost == 1017.50


def test_compute_cost_program_complete(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_hold_program_message_complete,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session,
            content=fixture_hold_program_message_complete,
            item_hash="asdf",
        )
        assert cost == Decimal("630.400000000000000000")


def test_compute_flow_cost(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_flow_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session, content=fixture_flow_instance_message, item_hash="abab"
        )

        assert cost == Decimal("0.000015277777777777")


def test_compute_flow_cost_conf(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_flow_instance_message,
):
    message_dict = fixture_flow_instance_message.model_dump()

    # Convert the message to conf
    message_dict["environment"].update(
        {
            "hypervisor": "qemu",  # Add qemu to the environment
            "trusted_execution": {
                "policy": 1,
                "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
            },
        }
    )

    rebuilt_message = InstanceContent.model_validate(message_dict)

    # Proceed with the test
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session, content=rebuilt_message, item_hash="abab"
        )

        assert cost == Decimal("0.000030555555555555")


def test_compute_flow_cost_complete(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_flow_instance_message_complete,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost, _ = get_total_and_detailed_costs(
            session=session,
            content=fixture_flow_instance_message_complete,
            item_hash="abab",
        )

        assert cost == Decimal("0.000032243382777775")


def test_default_settings_aggregates(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        aggregate = _get_settings_aggregate(session)
        assert isinstance(aggregate, dict)


def test_default_price_aggregates(
    session_factory: DbSessionFactory,
):
    with session_factory() as session:
        price_aggregate = _get_price_aggregate(session=session)
        assert isinstance(price_aggregate, dict)


def test_default_settings_aggregates_db(
    session_factory: DbSessionFactory, fixture_settings_aggregate_in_db
):
    with session_factory() as session:
        aggregate = _get_settings_aggregate(session)
        assert isinstance(aggregate, AggregateDb)


def test_default_price_aggregates_db(
    session_factory: DbSessionFactory, fixture_product_prices_aggregate_in_db
):
    with session_factory() as session:
        price_aggregate = _get_price_aggregate(session=session)
        assert isinstance(price_aggregate, AggregateDb)


def test_minimum_credit_cost_per_hour_for_small_volume(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Test that small volume costs meet minimum 1 credit per hour."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 1,  # Very small volume
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "payment": Payment(type=PaymentType.credit),
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    instance_content = InstanceContent.model_validate(content)

    with session_factory() as session:
        _, details = get_total_and_detailed_costs(
            session=session, content=instance_content, item_hash="test_hash"
        )

        # Find the rootfs volume cost
        rootfs_cost = next(
            (d for d in details if d.type == "EXECUTION_INSTANCE_VOLUME_ROOTFS"), None
        )
        assert rootfs_cost is not None

        # Verify the cost per hour rounds up to at least MIN_CREDIT_COST_PER_HOUR
        cost_per_hour = Decimal(rootfs_cost.cost_credit) * Decimal(HOUR)
        assert ceil(cost_per_hour) >= MIN_CREDIT_COST_PER_HOUR
        # Ensure it doesn't exceed MIN_CREDIT_COST_PER_HOUR (would ceil to 2)
        assert cost_per_hour <= Decimal(MIN_CREDIT_COST_PER_HOUR)


# GPU Test Fixtures


@pytest.fixture
def fixture_single_gpu_instance_message() -> InstanceContent:
    """Single GPU instance with RTX 4090 (Standard tier, 6 compute units)."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Single GPU Test"},
        "resources": {"vcpus": 8, "memory": 16384, "seconds": 30},
        "requirements": {
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
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_multiple_same_gpu_instance_message() -> InstanceContent:
    """Multiple same GPU instance with 2x A100 (Premium tier, 32 compute units total)."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Multi Same GPU Test"},
        "resources": {"vcpus": 16, "memory": 32768, "seconds": 30},
        "requirements": {
            "node": {
                "node_hash": "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
            },
            "gpu": [
                {
                    "vendor": "NVIDIA",
                    "device_name": "GA100 [A100 SXM4 80GB]",
                    "device_class": "0300",
                    "device_id": "10de:20b2",
                },
                {
                    "vendor": "NVIDIA",
                    "device_name": "GA100 [A100 SXM4 80GB]",
                    "device_class": "0300",
                    "device_id": "10de:20b2",
                },
            ],
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_mixed_same_tier_gpu_instance_message() -> InstanceContent:
    """Mixed same-tier GPU instance with RTX 4090 + L40S (Standard tier, 18 compute units total)."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Mixed Same Tier GPU Test"},
        "resources": {"vcpus": 12, "memory": 24576, "seconds": 30},
        "requirements": {
            "node": {
                "node_hash": "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
            },
            "gpu": [
                {
                    "vendor": "NVIDIA",
                    "device_name": "AD102 [GeForce RTX 4090]",
                    "device_class": "0300",
                    "device_id": "10de:2684",
                },
                {
                    "vendor": "NVIDIA",
                    "device_name": "AD102 [L40S]",
                    "device_class": "0300",
                    "device_id": "10de:26b9",
                },
            ],
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_mixed_tier_gpu_instance_message() -> InstanceContent:
    """Mixed-tier GPU instance with A100 + RTX 4090 (Premium + Standard, 22 compute units total)."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Mixed Tier GPU Test"},
        "resources": {"vcpus": 12, "memory": 24576, "seconds": 30},
        "requirements": {
            "node": {
                "node_hash": "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
            },
            "gpu": [
                {
                    "vendor": "NVIDIA",
                    "device_name": "GA100 [A100 SXM4 80GB]",
                    "device_class": "0300",
                    "device_id": "10de:20b2",
                },
                {
                    "vendor": "NVIDIA",
                    "device_name": "AD102 [GeForce RTX 4090]",
                    "device_class": "0300",
                    "device_id": "10de:2684",
                },
            ],
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    return InstanceContent.model_validate(content)


@pytest.fixture
def fixture_unknown_gpu_instance_message() -> InstanceContent:
    """GPU instance with unknown device_id."""
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Unknown GPU Test"},
        "resources": {"vcpus": 8, "memory": 16384, "seconds": 30},
        "requirements": {
            "node": {
                "node_hash": "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
            },
            "gpu": [
                {
                    "vendor": "NVIDIA",
                    "device_name": "Unknown GPU",
                    "device_class": "0300",
                    "device_id": "ffff:ffff",  # Invalid device ID
                }
            ],
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
            "hypervisor": "qemu",
        },
        "authorized_keys": [],
    }
    return InstanceContent.model_validate(content)


# GPU Test Cases


def test_compute_cost_single_gpu_standard(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_single_gpu_instance_message,
):
    """Test cost calculation for single GPU (RTX 4090)."""
    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session,
            content=fixture_single_gpu_instance_message,
            item_hash="gpu_single",
        )

        # RTX 4090: 6 compute units × $0.28/hour = $1.68/hour
        # Expected hold cost: $1680 (in smallest unit)
        assert cost == Decimal("1680")

        # Should have execution cost entries (GPU + storage)
        execution_costs = [d for d in details if d.type == CostType.EXECUTION]
        assert len(execution_costs) == 1
        assert execution_costs[0].name == "instance_gpu_standard"


def test_compute_cost_multiple_same_gpu_premium(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_multiple_same_gpu_instance_message,
):
    """Test cost calculation for multiple same GPUs (2x A100)."""
    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session,
            content=fixture_multiple_same_gpu_instance_message,
            item_hash="gpu_multi_same",
        )

        # 2x A100: 2 × 16 = 32 compute units × $0.56/hour = $17.92/hour
        # Expected hold cost: $17920 (in smallest unit)
        assert cost == Decimal("17920")

        # Should have one execution cost entry for premium tier
        execution_costs = [d for d in details if d.type == CostType.EXECUTION]
        assert len(execution_costs) == 1
        assert execution_costs[0].name == "instance_gpu_premium"


def test_compute_cost_multiple_different_gpu_same_tier(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_mixed_same_tier_gpu_instance_message,
):
    """Test cost calculation for different GPUs in same tier (RTX 4090 + L40S)."""
    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session,
            content=fixture_mixed_same_tier_gpu_instance_message,
            item_hash="gpu_mixed_same_tier",
        )

        # RTX 4090 (6 CU) + L40S (12 CU) = 18 CU × $0.28/hour = $5.04/hour
        # Expected hold cost: $5040 (in smallest unit)
        assert cost == Decimal("5040")

        # Should have one execution cost entry for standard tier
        execution_costs = [d for d in details if d.type == CostType.EXECUTION]
        assert len(execution_costs) == 1
        assert execution_costs[0].name == "instance_gpu_standard"


def test_compute_cost_mixed_tier_gpu(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_mixed_tier_gpu_instance_message,
):
    """Test multi-tier GPU cost calculation (A100 + RTX 4090)."""
    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session,
            content=fixture_mixed_tier_gpu_instance_message,
            item_hash="gpu_mixed_tier",
        )

        # A100: 16 CU × $0.56 = $8.96/hour
        # RTX 4090: 6 CU × $0.28 = $1.68/hour
        # Total: $10.64/hour = $10640 (in smallest unit)
        assert cost == Decimal("10640")

        # Should have TWO execution cost entries (one per tier)
        execution_costs = [d for d in details if d.type == CostType.EXECUTION]
        assert len(execution_costs) == 2

        # Verify both tiers are present
        tier_names = {ec.name for ec in execution_costs}
        assert "instance_gpu_premium" in tier_names
        assert "instance_gpu_standard" in tier_names

        # Verify individual tier costs
        premium_cost = next(
            ec for ec in execution_costs if ec.name == "instance_gpu_premium"
        )
        standard_cost = next(
            ec for ec in execution_costs if ec.name == "instance_gpu_standard"
        )

        assert premium_cost.cost_hold == Decimal("8960")  # 16 × $0.56
        assert standard_cost.cost_hold == Decimal("1680")  # 6 × $0.28


def test_compute_cost_unknown_gpu_raises_error(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
    fixture_unknown_gpu_instance_message,
):
    """Test that unknown GPU device_id raises ValueError."""
    with session_factory() as session:
        with pytest.raises(ValueError, match="not found in compatible GPUs"):
            get_total_and_detailed_costs(
                session=session,
                content=fixture_unknown_gpu_instance_message,
                item_hash="gpu_unknown",
            )
