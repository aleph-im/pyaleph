from decimal import Decimal
from unittest.mock import Mock

import pytest
from aleph_message.models import ExecutableContent, InstanceContent, PaymentType

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

    return InstanceContent.parse_obj(content)


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

    return InstanceContent.parse_obj(content)


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

    return InstanceContent.parse_obj(content)


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

    return InstanceContent.parse_obj(content)


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

    return CostEstimationProgramContent.parse_obj(content)


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
    message_dict = fixture_hold_instance_message.dict()

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

    rebuilt_message = InstanceContent.parse_obj(message_dict)

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
    message_dict = fixture_flow_instance_message.dict()

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

    rebuilt_message = InstanceContent.parse_obj(message_dict)

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
