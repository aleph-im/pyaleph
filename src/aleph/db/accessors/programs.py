import datetime as dt
from typing import Optional, Iterable

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models.programs import (
    CodeVolumeDb,
    DataVolumeDb,
    ExportVolumeDb,
    ProgramDb,
    RuntimeDb,
    MachineVolumeBaseDb,
    ProgramVersionDb,
)
from aleph.types.db_session import DbSession
from aleph.types.vms import ProgramVersion


def program_exists(session: DbSession, item_hash: str) -> bool:
    return ProgramDb.exists(session=session, where=ProgramDb.item_hash == item_hash)


def get_program(session: DbSession, item_hash: str) -> Optional[ProgramDb]:
    select_stmt = select(ProgramDb).where(ProgramDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def is_program_amend_allowed(session: DbSession, program_hash: str) -> Optional[bool]:
    select_stmt = (
        select(ProgramDb.allow_amend)
        .select_from(ProgramVersionDb)
        .join(ProgramDb, ProgramVersionDb.current_version == ProgramDb.item_hash)
        .where(ProgramVersionDb.program_hash == program_hash)
    )
    return session.execute(select_stmt).scalar_one_or_none()


def _delete_program(session: DbSession, where) -> Iterable[str]:
    # Deletion of volumes is managed automatically by the DB
    # using an "on delete cascade" foreign key.
    return session.execute(
        delete(ProgramDb).where(where).returning(ProgramDb.item_hash)
    ).scalars()


def delete_program(session: DbSession, item_hash: str) -> None:
    _ = _delete_program(session=session, where=ProgramDb.item_hash == item_hash)


def delete_program_updates(session: DbSession, program_hash: str) -> Iterable[str]:
    return _delete_program(session=session, where=ProgramDb.replaces == program_hash)


def upsert_program_version(
    session: DbSession,
    program_hash: str,
    owner: str,
    current_version: ProgramVersion,
    last_updated: dt.datetime,
) -> None:
    insert_stmt = insert(ProgramVersionDb).values(
        program_hash=program_hash,
        owner=owner,
        current_version=current_version,
        last_updated=last_updated,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="program_versions_pkey",
        set_={"current_version": current_version, "last_updated": last_updated},
        where=ProgramVersionDb.last_updated < last_updated,
    )
    session.execute(upsert_stmt)


def refresh_program_version(session: DbSession, program_hash: str) -> None:
    coalesced_ref = func.coalesce(ProgramDb.replaces, ProgramDb.item_hash)
    select_latest_revision_stmt = (
        select(
            coalesced_ref.label("replaces"),
            func.max(ProgramDb.created).label("created"),
        ).group_by(coalesced_ref)
    ).subquery()
    select_latest_program_version_stmt = (
        select(
            coalesced_ref,
            ProgramDb.owner,
            ProgramDb.item_hash,
            ProgramDb.created,
        )
        .join(
            select_latest_revision_stmt,
            (coalesced_ref == select_latest_revision_stmt.c.replaces)
            & (ProgramDb.created == select_latest_revision_stmt.c.created),
        )
        .where(coalesced_ref == program_hash)
    )

    insert_stmt = insert(ProgramVersionDb).from_select(
        ["program_hash", "owner", "current_version", "last_updated"],
        select_latest_program_version_stmt,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="program_versions_pkey",
        set_={
            "current_version": insert_stmt.excluded.current_version,
            "last_updated": insert_stmt.excluded.last_updated,
        },
    )
    session.execute(
        delete(ProgramVersionDb).where(ProgramVersionDb.program_hash == program_hash)
    )
    session.execute(upsert_stmt)
