import datetime as dt
from typing import Iterable, Optional

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import selectinload, with_polymorphic

from aleph.db.models.vms import (
    CodeVolumeDb,
    DataVolumeDb,
    ImmutableVolumeDb,
    MachineVolumeBaseDb,
    ProgramDb,
    RootfsVolumeDb,
    RuntimeDb,
    VmBaseDb,
    VmInstanceDb,
    VmVersionDb,
)
from aleph.types.db_session import AsyncDbSession
from aleph.types.vms import VmVersion

VolumeWithSubtypes = with_polymorphic(MachineVolumeBaseDb, "*")


async def get_instance(
    session: AsyncDbSession, item_hash: str
) -> Optional[VmInstanceDb]:
    stmt = (
        select(VmInstanceDb)
        .options(
            selectinload(VmInstanceDb.volumes.of_type(VolumeWithSubtypes)),
            selectinload(VmInstanceDb.rootfs),
        )
        .where(VmInstanceDb.item_hash == item_hash)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_program(session: AsyncDbSession, item_hash: str) -> Optional[ProgramDb]:
    stmt = (
        select(ProgramDb)
        .options(
            selectinload(ProgramDb.code_volume),
            selectinload(ProgramDb.runtime),
            selectinload(ProgramDb.data_volume),
            selectinload(ProgramDb.volumes.of_type(VolumeWithSubtypes)),
        )
        .where(ProgramDb.item_hash == item_hash)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def is_vm_amend_allowed(session: AsyncDbSession, vm_hash: str) -> Optional[bool]:
    select_stmt = (
        select(VmBaseDb.allow_amend)
        .select_from(VmVersionDb)
        .join(VmBaseDb, VmVersionDb.current_version == VmBaseDb.item_hash)
        .where(VmVersionDb.vm_hash == vm_hash)
    )
    return (await session.execute(select_stmt)).scalar_one_or_none()


async def _delete_vm(session: AsyncDbSession, where) -> Iterable[str]:
    # Deletion of volumes is managed automatically by the DB
    # using an "on delete cascade" foreign key.
    return (
        await session.execute(
            delete(VmBaseDb).where(where).returning(VmBaseDb.item_hash)
        )
    ).scalars()


async def delete_vm(session: AsyncDbSession, vm_hash: str) -> None:
    _ = await _delete_vm(session=session, where=VmBaseDb.item_hash == vm_hash)


async def delete_vm_updates(session: AsyncDbSession, vm_hash: str) -> Iterable[str]:
    return await _delete_vm(session=session, where=VmBaseDb.replaces == vm_hash)


async def get_vm_version(
    session: AsyncDbSession, vm_hash: str
) -> Optional[VmVersionDb]:
    return (
        await session.execute(select(VmVersionDb).where(VmVersionDb.vm_hash == vm_hash))
    ).scalar_one_or_none()


async def get_vms_dependent_volumes(
    session: AsyncDbSession, volume_hash: str
) -> Optional[VmBaseDb]:
    statement = (
        select(VmBaseDb)
        .join(
            ImmutableVolumeDb,
            ImmutableVolumeDb.vm_hash == VmBaseDb.item_hash,
            isouter=True,
        )
        .join(
            CodeVolumeDb, CodeVolumeDb.program_hash == VmBaseDb.item_hash, isouter=True
        )
        .join(
            DataVolumeDb, DataVolumeDb.program_hash == VmBaseDb.item_hash, isouter=True
        )
        .join(RuntimeDb, RuntimeDb.program_hash == VmBaseDb.item_hash, isouter=True)
        .join(
            RootfsVolumeDb,
            RootfsVolumeDb.instance_hash == VmBaseDb.item_hash,
            isouter=True,
        )
        .where(
            or_(
                ImmutableVolumeDb.ref == volume_hash,
                CodeVolumeDb.ref == volume_hash,
                DataVolumeDb.ref == volume_hash,
                RuntimeDb.ref == volume_hash,
                RootfsVolumeDb.parent_ref == volume_hash,
            )
        )
    )
    return (await session.execute(statement)).scalar_one_or_none()


async def upsert_vm_version(
    session: AsyncDbSession,
    vm_hash: str,
    owner: str,
    current_version: VmVersion,
    last_updated: dt.datetime,
) -> None:
    insert_stmt = insert(VmVersionDb).values(
        vm_hash=vm_hash,
        owner=owner,
        current_version=current_version,
        last_updated=last_updated,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="program_versions_pkey",
        set_={"current_version": current_version, "last_updated": last_updated},
        where=VmVersionDb.last_updated < last_updated,
    )
    await session.execute(upsert_stmt)


async def refresh_vm_version(session: AsyncDbSession, vm_hash: str) -> None:
    coalesced_ref = func.coalesce(VmBaseDb.replaces, VmBaseDb.item_hash)
    select_latest_revision_stmt = (
        select(
            coalesced_ref.label("replaces"),
            func.max(VmBaseDb.created).label("created"),
        ).group_by(coalesced_ref)
    ).subquery()
    select_latest_program_version_stmt = (
        select(
            coalesced_ref,
            VmBaseDb.owner,
            VmBaseDb.item_hash,
            VmBaseDb.created,
        )
        .join(
            select_latest_revision_stmt,
            (coalesced_ref == select_latest_revision_stmt.c.replaces)
            & (VmBaseDb.created == select_latest_revision_stmt.c.created),
        )
        .where(coalesced_ref == vm_hash)
    )

    insert_stmt = insert(VmVersionDb).from_select(
        ["vm_hash", "owner", "current_version", "last_updated"],
        select_latest_program_version_stmt,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="program_versions_pkey",
        set_={
            "current_version": insert_stmt.excluded.current_version,
            "last_updated": insert_stmt.excluded.last_updated,
        },
    )
    await session.execute(delete(VmVersionDb).where(VmVersionDb.vm_hash == vm_hash))
    await session.execute(upsert_stmt)
