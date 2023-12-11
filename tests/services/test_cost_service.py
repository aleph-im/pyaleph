import pytest
from aleph.types.db_session import DbSession

from aleph_message.models import ExecutableContent, InstanceContent

from aleph.services.cost import (
    compute_cost,
    get_additional_storage_price,
)

from unittest.mock import Mock


class StoredFileDb:
    pass


@pytest.fixture
def fixture_instance_message() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True
            },
            "size_mib": 20480,
            "persistence": "host"
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {
            "name": "Test Debian 12"
        },
        "resources": {
            "vcpus": 1,
            "memory": 2048,
            "seconds": 30
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ]
    }

    return InstanceContent.parse_obj(content)

@pytest.fixture
def fixture_instance_message_complete() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True
            },
            "size_mib": 20480,
            "persistence": "host"
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
        "metadata": {
            "name": "Test Debian 12"
        },
        "resources": {
            "vcpus": 1,
            "memory": 2048,
            "seconds": 30
        },
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ]
    }

    return InstanceContent.parse_obj(content)


def test_compute_cost(fixture_instance_message):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch('_get_file_from_ref', return_value=file_db)
    cost = compute_cost(content=fixture_instance_message, session=DbSession())
    assert cost == 2000


def test_get_additional_storage_price(fixture_instance_message):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch('_get_file_from_ref', return_value=file_db)
    cost = get_additional_storage_price(content=fixture_instance_message, session=DbSession())
    assert cost == 0


def test_compute_cost_complete(fixture_instance_message_complete):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch('_get_file_from_ref', return_value=file_db)
    cost = compute_cost(content=fixture_instance_message_complete, session=DbSession())
    assert cost == 2017.50
