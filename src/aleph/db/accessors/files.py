import datetime as dt
from typing import Optional, Iterable, Collection, Tuple

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Row

from aleph.types.db_session import DbSession
from aleph.types.files import FileTag, FileType
from aleph.types.sort_order import SortOrder
from ..models.files import (
    FilePinDb,
    FileTagDb,
    StoredFileDb,
    TxFilePinDb,
    MessageFilePinDb,
    FilePinType,
    ContentFilePinDb,
    GracePeriodFilePinDb,
)


def is_pinned_file(session: DbSession, file_hash: str) -> bool:
    return FilePinDb.exists(session=session, where=FilePinDb.file_hash == file_hash)


def get_unpinned_files(session: DbSession) -> Iterable[StoredFileDb]:
    """
    Returns the list of files that are not pinned by a message or an on-chain transaction.
    """
    select_pins = select(FilePinDb).where(FilePinDb.file_hash == StoredFileDb.hash)
    select_stmt = select(StoredFileDb).where(~select_pins.exists())
    return session.execute(select_stmt).scalars()


def upsert_tx_file_pin(
    session: DbSession, file_hash: str, tx_hash: str, created: dt.datetime
) -> None:
    upsert_stmt = (
        insert(TxFilePinDb)
        .values(
            file_hash=file_hash, tx_hash=tx_hash, type=FilePinType.TX, created=created
        )
        .on_conflict_do_nothing()
    )
    session.execute(upsert_stmt)


def insert_content_file_pin(
    session: DbSession,
    file_hash: str,
    owner: str,
    item_hash: str,
    created: dt.datetime,
) -> None:
    insert_stmt = insert(ContentFilePinDb).values(
        file_hash=file_hash,
        owner=owner,
        item_hash=item_hash,
        type=FilePinType.CONTENT,
        created=created,
    )
    session.execute(insert_stmt)


def insert_message_file_pin(
    session: DbSession,
    file_hash: str,
    owner: str,
    item_hash: str,
    ref: Optional[str],
    created: dt.datetime,
) -> None:
    insert_stmt = insert(MessageFilePinDb).values(
        file_hash=file_hash,
        owner=owner,
        item_hash=item_hash,
        type=FilePinType.MESSAGE,
        ref=ref,
        created=created,
    )
    session.execute(insert_stmt)


def count_file_pins(session: DbSession, file_hash: str) -> int:
    select_count_stmt = select(func.count()).select_from(
        select(FilePinDb).where(FilePinDb.file_hash == file_hash).subquery()
    )
    return session.execute(select_count_stmt).scalar_one()


def find_file_pins(session: DbSession, item_hashes: Collection[str]) -> Iterable[str]:
    select_stmt = select(MessageFilePinDb.item_hash).where(
        MessageFilePinDb.item_hash.in_(item_hashes)
    )
    return session.execute(select_stmt).scalars()


def delete_file_pin(session: DbSession, item_hash: str) -> None:
    delete_stmt = delete(MessageFilePinDb).where(
        MessageFilePinDb.item_hash == item_hash
    )
    session.execute(delete_stmt)


def insert_grace_period_file_pin(
    session: DbSession,
    file_hash: str,
    created: dt.datetime,
    delete_by: dt.datetime,
) -> None:
    insert_stmt = insert(GracePeriodFilePinDb).values(
        file_hash=file_hash,
        created=created,
        type=FilePinType.GRACE_PERIOD,
        delete_by=delete_by,
    )
    session.execute(insert_stmt)


def delete_grace_period_file_pins(session: DbSession, datetime: dt.datetime) -> None:
    delete_stmt = delete(GracePeriodFilePinDb).where(
        GracePeriodFilePinDb.delete_by < datetime
    )
    session.execute(delete_stmt)


def get_message_file_pin(
    session: DbSession, item_hash: str
) -> Optional[MessageFilePinDb]:
    return session.execute(
        select(MessageFilePinDb).where(MessageFilePinDb.item_hash == item_hash)
    ).scalar_one_or_none()


def get_address_files_stats(session: DbSession, owner: str) -> Tuple[int, int]:
    select_stmt = (
        select(
            func.count().label("nb_files"),
            func.sum(StoredFileDb.size).label("total_size"),
        )
        .select_from(MessageFilePinDb)
        .join(StoredFileDb, MessageFilePinDb.file_hash == StoredFileDb.hash)
        .where(MessageFilePinDb.owner == owner)
    )
    result = session.execute(select_stmt).one()
    return result.nb_files, result.total_size


def get_address_files_for_api(
    session: DbSession,
    owner: str,
    pagination: int = 0,
    page: int = 1,
    sort_order: SortOrder = SortOrder.DESCENDING,
) -> Iterable[Row]:
    select_stmt = (
        select(
            MessageFilePinDb.file_hash,
            MessageFilePinDb.created,
            MessageFilePinDb.item_hash,
            StoredFileDb.size,
            StoredFileDb.type,
        )
        .join(StoredFileDb, MessageFilePinDb.file_hash == StoredFileDb.hash)
        .where(MessageFilePinDb.owner == owner)
    )

    if pagination:
        select_stmt = select_stmt.limit(pagination).offset((page - 1) * pagination)

    order_by = (
        MessageFilePinDb.created.desc()
        if sort_order == SortOrder.DESCENDING
        else MessageFilePinDb.created.asc()
    )
    select_stmt = select_stmt.order_by(order_by)

    return session.execute(select_stmt).all()


def upsert_file(session: DbSession, file_hash: str, size: int, file_type: FileType):
    upsert_file_stmt = (
        insert(StoredFileDb)
        .values(hash=file_hash, size=size, type=file_type)
        .on_conflict_do_nothing(constraint="files_pkey")
    )
    session.execute(upsert_file_stmt)


def get_file(session: DbSession, file_hash: str) -> Optional[StoredFileDb]:
    select_stmt = select(StoredFileDb).where(StoredFileDb.hash == file_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def delete_file(session: DbSession, file_hash: str) -> None:
    delete_stmt = delete(StoredFileDb).where(StoredFileDb.hash == file_hash)
    session.execute(delete_stmt)


def get_file_tag(session: DbSession, tag: FileTag) -> Optional[FileTagDb]:
    select_stmt = select(FileTagDb).where(FileTagDb.tag == tag)
    return session.execute(select_stmt).scalar()


def file_tag_exists(session: DbSession, tag: FileTag) -> bool:
    return FileTagDb.exists(session=session, where=FileTagDb.tag == tag)


def find_file_tags(session: DbSession, tags: Collection[FileTag]) -> Iterable[FileTag]:
    select_stmt = select(FileTagDb.tag).where(FileTagDb.tag.in_(tags))
    return session.execute(select_stmt).scalars()


def upsert_file_tag(
    session: DbSession,
    tag: FileTag,
    owner: str,
    file_hash: str,
    last_updated: dt.datetime,
) -> None:
    insert_stmt = insert(FileTagDb).values(
        tag=tag, owner=owner, file_hash=file_hash, last_updated=last_updated
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="file_tags_pkey",
        set_={"file_hash": file_hash, "last_updated": last_updated},
        where=FileTagDb.last_updated < last_updated,
    )
    session.execute(upsert_stmt)


def refresh_file_tag(session: DbSession, tag: FileTag) -> None:
    coalesced_ref = func.coalesce(MessageFilePinDb.ref, MessageFilePinDb.item_hash)
    select_latest_file_pin_stmt = (
        select(
            coalesced_ref.label("computed_ref"),
            func.max(MessageFilePinDb.created).label("created"),
        )
        .group_by(coalesced_ref)
        .where(coalesced_ref == tag)
    ).subquery()
    select_file_tag_stmt = select(
        coalesced_ref.label("computed_ref"),
        MessageFilePinDb.owner,
        MessageFilePinDb.file_hash,
        MessageFilePinDb.created,
    ).join(
        select_latest_file_pin_stmt,
        (coalesced_ref == select_latest_file_pin_stmt.c.computed_ref)
        & (MessageFilePinDb.created == select_latest_file_pin_stmt.c.created),
    )

    insert_stmt = insert(FileTagDb).from_select(
        ["tag", "owner", "file_hash", "last_updated"], select_file_tag_stmt
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="file_tags_pkey",
        set_={
            "file_hash": insert_stmt.excluded.file_hash,
            "last_updated": insert_stmt.excluded.last_updated,
        },
    )
    session.execute(delete(FileTagDb).where(FileTagDb.tag == tag))
    session.execute(upsert_stmt)
