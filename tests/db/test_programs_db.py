import copy
import datetime as dt
from typing import Optional

import pytest
import pytz
from aleph_message.models import ItemHash
from aleph_message.models.execution import MachineType, Encoding
from aleph_message.models.execution.volume import VolumePersistence
from sqlalchemy import select

from aleph.db.accessors.vms import (
    get_program,
    is_vm_amend_allowed,
    refresh_vm_version,
    delete_vm,
)
from aleph.db.models import (
    VmBaseDb,
    CodeVolumeDb,
    RuntimeDb,
    VmVersionDb,
    DataVolumeDb,
    ExportVolumeDb,
    ImmutableVolumeDb,
    EphemeralVolumeDb,
    PersistentVolumeDb, ProgramDb,
)
from aleph.types.db_session import DbSessionFactory
from aleph.types.vms import VmVersion


@pytest.fixture
def original_program() -> ProgramDb:
    program_hash = ItemHash(
        "9e33e41ef7136823adfd26f2c50217826c98eb78cf6ddb5d8a470b63858bfb92"
    )
    code_volume = CodeVolumeDb(
        encoding=Encoding.zip,
        ref="681c03b10bb698aa2462874e999e11cd49c94b04cf3ba2d6e3f45a634749e607",
        use_latest=False,
        entrypoint="archetype:app",
    )
    runtime = RuntimeDb(
        ref="7162a3b9f8ca870fc06bafb3e9b14553304327bc78c7f53a4cee9445879e4fab",
        use_latest=True,
        comment="My runtime",
    )

    program = ProgramDb(
        item_hash=program_hash,
        owner="0xabadbabe",
        program_type=MachineType.vm_function,
        allow_amend=True,
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
        created=pytz.utc.localize(dt.datetime(2022, 11, 11, 11, 11, 11)),
        runtime=runtime,
        code_volume=code_volume,
    )
    return program


@pytest.fixture
def program_update(original_program: ProgramDb) -> ProgramDb:
    update = copy.deepcopy(original_program)
    update.item_hash = (
        "0e183850b805cf4c63e8783cf1428c3d846c9486fac94db9d59fe0120845407e"
    )
    update.allow_amend = False
    update.replaces = original_program.item_hash
    update.created = pytz.utc.localize(dt.datetime(2023, 1, 1))
    return update


@pytest.fixture
def program_with_many_volumes(original_program: ProgramDb) -> ProgramDb:
    code_volume = CodeVolumeDb(
        encoding=Encoding.squashfs,
        ref="681c03b10bb698aa2462874e999e11cd49c94b04cf3ba2d6e3f45a634749e607",
        use_latest=False,
        entrypoint="archetype:app",
    )
    data_volume = DataVolumeDb(
        encoding=Encoding.plain,
        ref="86136e0bb7764c34aacb76d2cbd469d32738da5655c2d4b6ddc220be09fca9d9",
        use_latest=True,
        mount="/data",
    )
    export_volume = ExportVolumeDb(encoding=Encoding.squashfs)
    runtime = RuntimeDb(
        ref="7162a3b9f8ca870fc06bafb3e9b14553304327bc78c7f53a4cee9445879e4fab",
        use_latest=True,
        comment="My runtime",
    )
    volumes = [
        ImmutableVolumeDb(
            ref="bb753157521ac3190c6ba88c5ad87cff0dfe053717386c07a40d119cb1a13430",
            use_latest=False,
            mount="/static",
            size_mib=100,
        ),
        EphemeralVolumeDb(mount="/tmp", size_mib=10),
        PersistentVolumeDb(
            mount="/data",
            size_mib=1000,
            persistence=VolumePersistence.store,
            name="data",
        ),
    ]

    program = copy.deepcopy(original_program)
    program.item_hash = (
        "16d547ddb8d8ce33dc9f005d6470abe206a657c27943a9452434d68a9ccf1718"
    )
    program.created = pytz.utc.localize(dt.datetime(2023, 2, 1))
    program.code_volume = code_volume
    program.runtime = runtime
    program.data_volume = data_volume
    program.export_volume = export_volume
    program.volumes = volumes

    return program


def assert_programs_equal(expected: ProgramDb, actual: ProgramDb):
    assert actual.item_hash == expected.item_hash
    assert actual.owner == expected.owner
    assert actual.type == expected.type
    assert actual.allow_amend == expected.allow_amend
    assert actual.metadata_ == expected.metadata_
    assert actual.variables == expected.variables
    assert actual.http_trigger == expected.http_trigger
    assert actual.message_triggers == expected.message_triggers
    assert actual.persistent == expected.persistent
    assert actual.environment_reproducible == expected.environment_reproducible
    assert actual.environment_internet == expected.environment_internet
    assert actual.environment_aleph_api == expected.environment_aleph_api
    assert actual.environment_shared_cache == expected.environment_shared_cache
    assert actual.resources_vcpus == expected.resources_vcpus
    assert actual.resources_memory == expected.resources_memory
    assert actual.resources_seconds == expected.resources_seconds
    assert actual.cpu_architecture == expected.cpu_architecture
    assert actual.cpu_vendor == expected.cpu_vendor
    assert actual.node_owner == expected.node_owner
    assert actual.node_address_regex == expected.node_address_regex
    assert actual.replaces == expected.replaces

    if expected.code_volume:
        assert actual.code_volume is not None
        assert actual.code_volume.encoding == expected.code_volume.encoding
        assert actual.code_volume.ref == expected.code_volume.ref
        assert actual.code_volume.use_latest == expected.code_volume.use_latest
        assert actual.code_volume.entrypoint == expected.code_volume.entrypoint
        assert actual.code_volume.program_hash == expected.code_volume.program_hash

    if expected.runtime:
        assert actual.runtime is not None
        assert actual.runtime.ref == expected.runtime.ref
        assert actual.runtime.use_latest == expected.runtime.use_latest
        assert actual.runtime.program_hash == expected.runtime.program_hash
        assert actual.runtime.comment == expected.runtime.comment


def test_program_accessors(
    session_factory: DbSessionFactory,
    original_program: ProgramDb,
    program_update: ProgramDb,
    program_with_many_volumes: ProgramDb,
):
    with session_factory() as session:
        session.add(original_program)
        session.add(program_update)
        session.add(
            VmVersionDb(
                vm_hash=original_program.item_hash,
                owner=original_program.owner,
                current_version=VmVersion(program_update.item_hash),
                last_updated=program_update.created,
            )
        )
        session.add(program_with_many_volumes)
        session.add(
            VmVersionDb(
                vm_hash=program_with_many_volumes.item_hash,
                owner=program_with_many_volumes.owner,
                current_version=VmVersion(program_with_many_volumes.item_hash),
                last_updated=program_with_many_volumes.created,
            )
        )
        session.commit()

    with session_factory() as session:
        original_program_db = get_program(
            session=session, item_hash=original_program.item_hash
        )
        assert original_program_db is not None
        assert_programs_equal(expected=original_program, actual=original_program_db)

        program_update_db = get_program(
            session=session, item_hash=program_update.item_hash
        )
        assert program_update_db is not None
        assert_programs_equal(expected=program_update_db, actual=program_update)

        program_with_many_volumes_db = get_program(
            session=session, item_hash=program_with_many_volumes.item_hash
        )
        assert program_with_many_volumes_db is not None
        assert_programs_equal(
            expected=program_with_many_volumes_db, actual=program_with_many_volumes
        )

        is_amend_allowed = is_vm_amend_allowed(
            session=session, vm_hash=original_program.item_hash
        )
        assert is_amend_allowed is False

        is_amend_allowed = is_vm_amend_allowed(
            session=session, vm_hash=program_with_many_volumes.item_hash
        )
        assert is_amend_allowed is True


def test_refresh_program(
    session_factory: DbSessionFactory,
    original_program: ProgramDb,
    program_update: ProgramDb,
):
    program_hash = original_program.item_hash

    def get_program_version(session) -> Optional[VmVersionDb]:
        return session.execute(
            select(VmVersionDb).where(VmVersionDb.vm_hash == program_hash)
        ).scalar_one_or_none()

    # Insert program version with refresh_program_version
    with session_factory() as session:
        session.add(original_program)
        session.commit()

        refresh_vm_version(session=session, vm_hash=program_hash)
        session.commit()

        program_version_db = get_program_version(session)
        assert program_version_db is not None
        assert program_version_db.current_version == program_hash
        assert program_version_db.last_updated == original_program.created

    # Update the version of the program, program_versions should be updated
    with session_factory() as session:
        session.add(program_update)
        session.commit()

        refresh_vm_version(session=session, vm_hash=program_hash)
        session.commit()

        program_version_db = get_program_version(session)
        assert program_version_db is not None
        assert program_version_db.current_version == program_update.item_hash
        assert program_version_db.last_updated == program_update.created

    # Delete the update, the original should be back in program_versions
    with session_factory() as session:
        delete_vm(session=session, vm_hash=program_update.item_hash)
        session.commit()

        refresh_vm_version(session=session, vm_hash=program_hash)
        session.commit()

        program_version_db = get_program_version(session)
        assert program_version_db is not None
        assert program_version_db.current_version == program_hash
        assert program_version_db.last_updated == original_program.created

    # Delete the original, no entry should be left in program_versions
    with session_factory() as session:
        delete_vm(session=session, vm_hash=original_program.item_hash)
        session.commit()

        refresh_vm_version(session=session, vm_hash=program_hash)
        session.commit()

        program_version_db = get_program_version(session)
        assert program_version_db is None
