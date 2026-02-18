"""Tests for volume size calculation in cost service."""

import datetime as dt
from decimal import Decimal

import pytest
from aleph_message.models import InstanceContent, PaymentType

from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.files import MessageFilePinDb
from aleph.db.models.files import StoredFileDb as StoredFileDbActual
from aleph.schemas.cost_estimation_messages import CostEstimationProgramContent
from aleph.services.cost import get_cost_component_size_mib
from aleph.toolkit.constants import MiB
from aleph.types.cost import CostType
from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def fixture_hold_instance_message() -> InstanceContent:
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
def fixture_hold_instance_message_complete() -> InstanceContent:
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
def fixture_hold_program_message_complete() -> CostEstimationProgramContent:
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


def test_get_size_mib_for_execution_instance_volume_rootfs(
    fixture_hold_instance_message,
):
    """Test that size_mib is correctly retrieved for EXECUTION_INSTANCE_VOLUME_ROOTFS."""
    content = fixture_hold_instance_message

    # Create cost component for rootfs
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION_INSTANCE_VOLUME_ROOTFS,
        name="EXECUTION_INSTANCE_VOLUME_ROOTFS",
        ref="6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
        payment_type=PaymentType.hold,
        cost_hold=Decimal("0.5"),
        cost_stream=Decimal("0.0000001"),
        cost_credit=Decimal("0.1"),
    )

    # Get size using the helper function
    size_mib = get_cost_component_size_mib(None, cost, content)

    # Assert size matches rootfs.size_mib from content
    assert size_mib == 20480.0


def test_get_size_mib_for_execution_volume_persistent(
    fixture_hold_instance_message_complete,
):
    """Test that size_mib is correctly retrieved for EXECUTION_VOLUME_PERSISTENT."""
    content = fixture_hold_instance_message_complete

    # Create cost component for persistent volume #1 (sqlite-data: 100 MiB)
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION_VOLUME_PERSISTENT,
        name="#1:/var/lib/sqlite",
        ref=None,
        payment_type=PaymentType.hold,
        cost_hold=Decimal("0.5"),
        cost_stream=Decimal("0.0000001"),
        cost_credit=Decimal("0.1"),
    )

    # Get size using the helper function
    size_mib = get_cost_component_size_mib(None, cost, content)

    # Assert size matches the volume size_mib from content
    assert size_mib == 100.0


def test_get_size_mib_for_execution_program_volume_code(
    session_factory: DbSessionFactory,
):
    """Test that size_mib is correctly retrieved for EXECUTION_PROGRAM_VOLUME_CODE from file."""
    with session_factory() as session:
        # Create a test file for code volume
        file_hash = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe"
        store_msg_hash = "code_store_msg_hash_0001"
        file_size = 200 * MiB  # 200 MiB
        test_file = StoredFileDbActual(
            hash=file_hash,
            size=file_size,
            type="file",
        )
        session.add(test_file)

        # Link the store message hash to the file via MessageFilePinDb
        pin = MessageFilePinDb(
            file_hash=file_hash,
            item_hash=store_msg_hash,
            created=dt.datetime.now(tz=dt.timezone.utc),
            owner="0xTestAddress",
        )
        session.add(pin)
        session.flush()

        # Create cost component for code volume
        cost = AccountCostsDb(
            owner="0xTestAddress",
            item_hash="test_item_hash",
            type=CostType.EXECUTION_PROGRAM_VOLUME_CODE,
            name="EXECUTION_PROGRAM_VOLUME_CODE",
            ref=store_msg_hash,
            payment_type=PaymentType.hold,
            cost_hold=Decimal("0.5"),
            cost_stream=Decimal("0.0000001"),
            cost_credit=Decimal("0.1"),
        )

        # Get size using the helper function (real file size should be used)
        size_mib = get_cost_component_size_mib(session, cost, None)

        # Assert size matches file size
        assert size_mib == 200.0


def test_get_size_mib_for_execution_program_volume_code_with_estimation(
    fixture_hold_program_message_complete,
):
    """Test that size_mib falls back to estimated_size_mib for EXECUTION_PROGRAM_VOLUME_CODE."""
    content = fixture_hold_program_message_complete

    # Create cost component for code volume (no file in DB)
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION_PROGRAM_VOLUME_CODE,
        name="EXECUTION_PROGRAM_VOLUME_CODE",
        ref="cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
        payment_type=PaymentType.hold,
        cost_hold=Decimal("0.5"),
        cost_stream=Decimal("0.0000001"),
        cost_credit=Decimal("0.1"),
    )

    # Get size using the helper function (should fall back to estimated_size_mib)
    # Use None for session to skip file lookup
    size_mib = get_cost_component_size_mib(None, cost, content)

    # Assert size matches estimated_size_mib from content
    assert size_mib == 2048.0


def test_get_size_mib_for_execution_volume_inmutable_from_file(
    session_factory: DbSessionFactory,
):
    """Test that size_mib is correctly retrieved for EXECUTION_VOLUME_INMUTABLE from file."""
    with session_factory() as session:
        # Create a test file for immutable volume
        file_hash = "immutable_volume_hash_123"
        store_msg_hash = "immutable_store_msg_hash_0001"
        file_size = 512 * MiB  # 512 MiB
        test_file = StoredFileDbActual(
            hash=file_hash,
            size=file_size,
            type="file",
        )
        session.add(test_file)

        # Link the store message hash to the file via MessageFilePinDb
        pin = MessageFilePinDb(
            file_hash=file_hash,
            item_hash=store_msg_hash,
            created=dt.datetime.now(tz=dt.timezone.utc),
            owner="0xTestAddress",
        )
        session.add(pin)
        session.flush()

        # Create cost component for immutable volume
        cost = AccountCostsDb(
            owner="0xTestAddress",
            item_hash="test_item_hash",
            type=CostType.EXECUTION_VOLUME_INMUTABLE,
            name="#0:/mount1",
            ref=store_msg_hash,
            payment_type=PaymentType.hold,
            cost_hold=Decimal("0.5"),
            cost_stream=Decimal("0.0000001"),
            cost_credit=Decimal("0.1"),
        )

        # Get size using the helper function (real file size should be used)
        size_mib = get_cost_component_size_mib(session, cost, None)

        # Assert size matches file size
        assert size_mib == 512.0


def test_get_size_mib_for_execution_volume_inmutable_with_estimation(
    fixture_hold_program_message_complete,
):
    """Test that size_mib falls back to estimated_size_mib for EXECUTION_VOLUME_INMUTABLE."""
    content = fixture_hold_program_message_complete

    # Create cost component for immutable volume #0 (estimated: 512 MiB)
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION_VOLUME_INMUTABLE,
        name="#0:/mount1",
        ref="cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe",
        payment_type=PaymentType.hold,
        cost_hold=Decimal("0.5"),
        cost_stream=Decimal("0.0000001"),
        cost_credit=Decimal("0.1"),
    )

    # Get size using the helper function (should fall back to estimated_size_mib)
    size_mib = get_cost_component_size_mib(None, cost, content)

    # Assert size matches estimated_size_mib from content.volumes[0]
    assert size_mib == 512.0


def test_get_size_mib_prioritizes_real_file_over_estimation(
    session_factory: DbSessionFactory, fixture_hold_program_message_complete
):
    """Test that real file size is prioritized over estimated_size_mib."""
    content = fixture_hold_program_message_complete

    with session_factory() as session:
        # Create a test file with DIFFERENT size than estimated
        file_hash = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe"
        store_msg_hash = "priority_store_msg_hash_0001"
        file_size = 3000 * MiB  # 3000 MiB (different from estimated 2048)
        test_file = StoredFileDbActual(
            hash=file_hash,
            size=file_size,
            type="file",
        )
        session.add(test_file)

        # Link the store message hash to the file via MessageFilePinDb
        pin = MessageFilePinDb(
            file_hash=file_hash,
            item_hash=store_msg_hash,
            created=dt.datetime.now(tz=dt.timezone.utc),
            owner="0xTestAddress",
        )
        session.add(pin)
        session.flush()

        # Create cost component for code volume
        cost = AccountCostsDb(
            owner="0xTestAddress",
            item_hash="test_item_hash",
            type=CostType.EXECUTION_PROGRAM_VOLUME_CODE,
            name="EXECUTION_PROGRAM_VOLUME_CODE",
            ref=store_msg_hash,
            payment_type=PaymentType.hold,
            cost_hold=Decimal("0.5"),
            cost_stream=Decimal("0.0000001"),
            cost_credit=Decimal("0.1"),
        )

        # Get size using the helper function (should use REAL file size, not estimated)
        size_mib = get_cost_component_size_mib(session, cost, content)

        # Assert size matches REAL file size, not estimated
        assert size_mib == 3000.0
        assert size_mib != 2048.0  # Not the estimated size


def test_get_size_mib_for_execution_returns_none():
    """Test that size_mib returns None for EXECUTION cost type."""
    # Create an EXECUTION-type cost component
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION,
        name="Execution",
        ref=None,
        payment_type=PaymentType.hold,
        cost_hold=Decimal("0.5"),
        cost_stream=Decimal("0.0000001"),
        cost_credit=Decimal("0.1"),
    )

    # Get size using the helper function
    size_mib = get_cost_component_size_mib(None, cost, None)

    # Assert size is None for EXECUTION type
    assert size_mib is None


def test_get_size_mib_for_execution_volume_discount_returns_none():
    """Test that size_mib returns None for EXECUTION_VOLUME_DISCOUNT cost type."""
    # Create an EXECUTION_VOLUME_DISCOUNT cost component
    cost = AccountCostsDb(
        owner="0xTestAddress",
        item_hash="test_item_hash",
        type=CostType.EXECUTION_VOLUME_DISCOUNT,
        name="EXECUTION_VOLUME_DISCOUNT",
        ref=None,
        payment_type=PaymentType.hold,
        cost_hold=Decimal("-0.5"),
        cost_stream=Decimal("-0.0000001"),
        cost_credit=Decimal("-0.1"),
    )

    # Get size using the helper function
    size_mib = get_cost_component_size_mib(None, cost, None)

    # Assert size is None for EXECUTION_VOLUME_DISCOUNT type
    assert size_mib is None
