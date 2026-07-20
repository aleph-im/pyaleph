import datetime as dt

import pytest
import pytz
from aleph_message.models.execution import MachineType
from sqlalchemy import func, select

from aleph.db.accessors.vms import (
    delete_vm,
    get_instance,
    get_program,
    get_vms_dependent_volumes,
    get_vprogram,
)
from aleph.db.models import ProgramDb, RuntimeDb, VProgramDb, VProgramVerifiedVolumeDb
from aleph.types.db_session import DbSessionFactory
from aleph.types.vms import VmType

VPROGRAM_HASH = "1a" * 32
RUNTIME_REF = "2b" * 32
WORKLOAD_REF = "3c" * 32
WORKLOAD_HASH_TREE = "4d" * 32
VOLUME_REF = "5e" * 32
VOLUME_HASH_TREE = "6f" * 32


@pytest.fixture
def vprogram() -> VProgramDb:
    return VProgramDb(
        item_hash=VPROGRAM_HASH,
        owner="0xabadbabe",
        allow_amend=False,
        metadata_=None,
        variables=None,
        message_triggers=None,
        environment_reproducible=False,
        environment_internet=True,
        environment_aleph_api=False,
        environment_shared_cache=False,
        resources_vcpus=2,
        resources_memory=2048,
        resources_seconds=30,
        cpu_architecture=None,
        cpu_vendor=None,
        node_owner=None,
        node_address_regex=None,
        replaces=None,
        created=pytz.utc.localize(dt.datetime(2026, 7, 10)),
        runtime_ref=RUNTIME_REF,
        runtime_comment="snp runtime bundle",
        workload_ref=WORKLOAD_REF,
        workload_hash_tree=WORKLOAD_HASH_TREE,
        workload_roothash="ab" * 32,
        verified_volumes=[
            VProgramVerifiedVolumeDb(
                position=0,
                ref=VOLUME_REF,
                hash_tree=VOLUME_HASH_TREE,
                roothash="cd" * 32,
                comment="model weights",
            )
        ],
    )


def test_vprogram_accessors(session_factory: DbSessionFactory, vprogram: VProgramDb):
    with session_factory() as session:
        session.add(vprogram)
        session.commit()

    with session_factory() as session:
        vprogram_db = get_vprogram(session=session, item_hash=VPROGRAM_HASH)
        assert vprogram_db is not None
        assert vprogram_db.type == VmType.VPROGRAM
        assert vprogram_db.runtime_ref == RUNTIME_REF
        assert vprogram_db.workload_ref == WORKLOAD_REF
        assert len(vprogram_db.verified_volumes) == 1

        # A V-Program is neither an instance nor a program.
        assert get_instance(session=session, item_hash=VPROGRAM_HASH) is None
        assert get_program(session=session, item_hash=VPROGRAM_HASH) is None


@pytest.mark.parametrize(
    "volume_hash",
    [RUNTIME_REF, WORKLOAD_REF, WORKLOAD_HASH_TREE, VOLUME_REF, VOLUME_HASH_TREE],
)
def test_get_vms_dependent_volumes_sees_vprogram_refs(
    session_factory: DbSessionFactory, vprogram: VProgramDb, volume_hash: str
):
    """Every store file referenced by a V-Program (runtime manifest, workload
    image and hash tree, verified volumes and their hash trees) must block
    forgets while the V-Program is alive."""
    with session_factory() as session:
        session.add(vprogram)
        session.commit()

    with session_factory() as session:
        dependent_vm = get_vms_dependent_volumes(
            session=session, volume_hash=volume_hash
        )
        assert dependent_vm is not None
        assert dependent_vm.item_hash == VPROGRAM_HASH

        assert get_vms_dependent_volumes(session=session, volume_hash="00" * 32) is None


def test_get_vms_dependent_volumes_multiple_matches(
    session_factory: DbSessionFactory, vprogram: VProgramDb
):
    """Several rows can reference the same file (here: a second verified
    volume of the same V-Program, and a program runtime); the query must
    return one of them instead of raising MultipleResultsFound."""
    vprogram.verified_volumes.append(
        VProgramVerifiedVolumeDb(
            position=1,
            ref=VOLUME_REF,
            hash_tree="7a" * 32,
            roothash="ef" * 32,
            comment="same dataset, second mount",
        )
    )
    program = ProgramDb(
        item_hash="8b" * 32,
        owner="0xabadbabe",
        program_type=MachineType.vm_function,
        allow_amend=False,
        metadata_=None,
        variables=None,
        http_trigger=True,
        message_triggers=None,
        persistent=False,
        environment_reproducible=False,
        environment_internet=True,
        environment_aleph_api=True,
        environment_shared_cache=False,
        resources_vcpus=1,
        resources_memory=128,
        resources_seconds=30,
        cpu_architecture=None,
        cpu_vendor=None,
        node_owner=None,
        node_address_regex=None,
        replaces=None,
        created=pytz.utc.localize(dt.datetime(2026, 7, 10)),
        runtime=RuntimeDb(ref=VOLUME_REF, use_latest=False, comment="shared file"),
    )
    with session_factory() as session:
        session.add(vprogram)
        session.add(program)
        session.commit()

    with session_factory() as session:
        dependent_vm = get_vms_dependent_volumes(
            session=session, volume_hash=VOLUME_REF
        )
        assert dependent_vm is not None
        assert dependent_vm.item_hash in (VPROGRAM_HASH, "8b" * 32)


def test_delete_vprogram_cascades_verified_volumes(
    session_factory: DbSessionFactory, vprogram: VProgramDb
):
    with session_factory() as session:
        session.add(vprogram)
        session.commit()

    with session_factory() as session:
        delete_vm(session=session, vm_hash=VPROGRAM_HASH)
        session.commit()

        assert get_vprogram(session=session, item_hash=VPROGRAM_HASH) is None
        remaining_volumes = session.execute(
            select(func.count()).select_from(VProgramVerifiedVolumeDb)
        ).scalar_one()
        assert remaining_volumes == 0
