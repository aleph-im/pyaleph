import datetime as dt
from typing import Iterable, Optional

from sqlalchemy import delete, func, select, union_all
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models.vms import (
    CodeVolumeDb,
    DataVolumeDb,
    ImmutableVolumeDb,
    ProgramDb,
    RootfsVolumeDb,
    RuntimeDb,
    VmBaseDb,
    VmInstanceDb,
    VmVersionDb,
    VProgramDb,
    VProgramVerifiedVolumeDb,
)
from aleph.types.db_session import DbSession
from aleph.types.vms import VmVersion


def get_instance(session: DbSession, item_hash: str) -> Optional[VmInstanceDb]:
    select_stmt = select(VmInstanceDb).where(VmInstanceDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def get_program(session: DbSession, item_hash: str) -> Optional[ProgramDb]:
    select_stmt = select(ProgramDb).where(ProgramDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def get_vprogram(session: DbSession, item_hash: str) -> Optional[VProgramDb]:
    select_stmt = select(VProgramDb).where(VProgramDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def is_vm_amend_allowed(session: DbSession, vm_hash: str) -> Optional[bool]:
    select_stmt = (
        select(VmBaseDb.allow_amend)
        .select_from(VmVersionDb)
        .join(VmBaseDb, VmVersionDb.current_version == VmBaseDb.item_hash)
        .where(VmVersionDb.vm_hash == vm_hash)
    )
    return session.execute(select_stmt).scalar_one_or_none()


def _delete_vm(session: DbSession, where) -> Iterable[str]:
    # Deletion of volumes is managed automatically by the DB
    # using an "on delete cascade" foreign key.
    return session.execute(
        delete(VmBaseDb).where(where).returning(VmBaseDb.item_hash)
    ).scalars()


def delete_vm(session: DbSession, vm_hash: str) -> None:
    _ = _delete_vm(session=session, where=VmBaseDb.item_hash == vm_hash)


def delete_vm_updates(session: DbSession, vm_hash: str) -> Iterable[str]:
    return _delete_vm(session=session, where=VmBaseDb.replaces == vm_hash)


def get_vm_version(session: DbSession, vm_hash: str) -> Optional[VmVersionDb]:
    return session.execute(
        select(VmVersionDb).where(VmVersionDb.vm_hash == vm_hash)
    ).scalar_one_or_none()


def get_vms_dependent_volumes(
    session: DbSession, volume_hash: str
) -> Optional[VmBaseDb]:
    # Each leg is a point lookup on an indexed column, one per table that
    # can reference a store file. A UNION ALL of point lookups lets the
    # planner use an index scan per leg instead of the LEFT JOIN + OR this
    # used to be, which forced a scan of every VM to check every volume
    # table. Several rows can match (e.g. two VMs sharing a runtime, or two
    # volumes of one VM referencing the same file); any one of them is
    # enough to block the forget, hence the LIMIT 1 over the union.
    legs = union_all(
        select(ImmutableVolumeDb.vm_hash.label("vm_hash")).where(
            ImmutableVolumeDb.ref == volume_hash
        ),
        select(CodeVolumeDb.program_hash.label("vm_hash")).where(
            CodeVolumeDb.ref == volume_hash
        ),
        select(DataVolumeDb.program_hash.label("vm_hash")).where(
            DataVolumeDb.ref == volume_hash
        ),
        select(RuntimeDb.program_hash.label("vm_hash")).where(
            RuntimeDb.ref == volume_hash
        ),
        select(RootfsVolumeDb.instance_hash.label("vm_hash")).where(
            RootfsVolumeDb.parent_ref == volume_hash
        ),
        # V-Program refs: the runtime manifest, runtime bundle and workload
        # live in columns on the vms table itself (single-table
        # inheritance), so these legs query VProgramDb directly. SQLAlchemy
        # adds the `type = 'v-program'` polymorphic filter automatically,
        # matching the partial indexes on these columns.
        select(VProgramDb.item_hash.label("vm_hash")).where(
            VProgramDb.runtime_ref == volume_hash
        ),
        select(VProgramDb.item_hash.label("vm_hash")).where(
            VProgramDb.runtime_bundle_ref == volume_hash
        ),
        select(VProgramDb.item_hash.label("vm_hash")).where(
            VProgramDb.workload_ref == volume_hash
        ),
        select(VProgramDb.item_hash.label("vm_hash")).where(
            VProgramDb.workload_hash_tree == volume_hash
        ),
        select(VProgramVerifiedVolumeDb.vm_hash.label("vm_hash")).where(
            VProgramVerifiedVolumeDb.ref == volume_hash
        ),
        select(VProgramVerifiedVolumeDb.vm_hash.label("vm_hash")).where(
            VProgramVerifiedVolumeDb.hash_tree == volume_hash
        ),
    ).limit(1)
    vm_hash = session.execute(legs).scalar()
    if vm_hash is None:
        return None
    return session.get(VmBaseDb, vm_hash)


def upsert_vm_version(
    session: DbSession,
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
    session.execute(upsert_stmt)


def refresh_vm_version(session: DbSession, vm_hash: str) -> None:
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
    session.execute(delete(VmVersionDb).where(VmVersionDb.vm_hash == vm_hash))
    session.execute(upsert_stmt)
