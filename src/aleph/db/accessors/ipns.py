import datetime as dt
from typing import Iterable, Optional

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from aleph.db.models.ipns import IpnsRecordDb
from aleph.types.db_session import DbSession
from aleph.types.ipns import IpnsStatus


def upsert_ipns_record(
    session: DbSession,
    name: str,
    owner: str,
    item_hash: str,
    record: Optional[bytes],
    record_sequence: Optional[int],
    record_validity: Optional[dt.datetime],
    max_size_mib: int,
    resolved_cid: Optional[str],
    last_resolved: Optional[dt.datetime],
    status: IpnsStatus,
    created: dt.datetime,
) -> None:
    insert_stmt = insert(IpnsRecordDb).values(
        name=name,
        owner=owner,
        item_hash=item_hash,
        record=record,
        record_sequence=record_sequence,
        record_validity=record_validity,
        max_size_mib=max_size_mib,
        resolved_cid=resolved_cid,
        last_resolved=last_resolved,
        status=status.value,
        created=created,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["name", "owner"],
        set_={
            "item_hash": item_hash,
            "record": record,
            "record_sequence": record_sequence,
            "record_validity": record_validity,
            "max_size_mib": max_size_mib,
            "resolved_cid": resolved_cid,
            "last_resolved": last_resolved,
            "status": status.value,
        },
    )
    session.execute(upsert_stmt)


def get_ipns_record(
    session: DbSession, name: str, owner: str
) -> Optional[IpnsRecordDb]:
    select_stmt = select(IpnsRecordDb).where(
        (IpnsRecordDb.name == name) & (IpnsRecordDb.owner == owner)
    )
    return session.execute(select_stmt).scalar_one_or_none()


def get_ipns_records_by_name(session: DbSession, name: str) -> Iterable[IpnsRecordDb]:
    select_stmt = select(IpnsRecordDb).where(IpnsRecordDb.name == name)
    return session.execute(select_stmt).scalars().all()


def get_ipns_records_by_owner(session: DbSession, owner: str) -> Iterable[IpnsRecordDb]:
    select_stmt = select(IpnsRecordDb).where(IpnsRecordDb.owner == owner)
    return session.execute(select_stmt).scalars().all()


def get_all_ipns_records(session: DbSession) -> Iterable[IpnsRecordDb]:
    return session.execute(select(IpnsRecordDb)).scalars().all()


def delete_ipns_record(session: DbSession, name: str, owner: str) -> None:
    delete_stmt = delete(IpnsRecordDb).where(
        (IpnsRecordDb.name == name) & (IpnsRecordDb.owner == owner)
    )
    session.execute(delete_stmt)
