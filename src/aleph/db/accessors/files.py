import datetime as dt
from typing import Collection, Iterable, Optional, Tuple, Union

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Row
from sqlalchemy.orm import selectinload

from aleph.types.db_session import AsyncDbSession
from aleph.types.files import FileTag, FileType
from aleph.types.sort_order import SortOrder
from aleph.utils import make_file_tag

from ..models.files import (
    ContentFilePinDb,
    FilePinDb,
    FilePinType,
    FileTagDb,
    GracePeriodFilePinDb,
    MessageFilePinDb,
    StoredFileDb,
    TxFilePinDb,
)


async def is_pinned_file(session: AsyncDbSession, file_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.file_hash == file_hash
    )


async def get_unpinned_files(session: AsyncDbSession) -> Iterable[StoredFileDb]:
    """
    Returns the list of files that are not pinned by a message or an on-chain transaction.
    """
    select_stmt = (
        select(StoredFileDb)
        .join(FilePinDb, StoredFileDb.hash == FilePinDb.file_hash, isouter=True)
        .where(FilePinDb.id.is_(None))
    )
    return (await session.execute(select_stmt)).scalars()


async def upsert_tx_file_pin(
    session: AsyncDbSession, file_hash: str, tx_hash: str, created: dt.datetime
) -> None:
    upsert_stmt = (
        insert(TxFilePinDb)
        .values(
            file_hash=file_hash, tx_hash=tx_hash, type=FilePinType.TX, created=created
        )
        .on_conflict_do_nothing()
    )
    await session.execute(upsert_stmt)


async def insert_content_file_pin(
    session: AsyncDbSession,
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
    await session.execute(insert_stmt)


async def insert_message_file_pin(
    session: AsyncDbSession,
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
    await session.execute(insert_stmt)


async def count_file_pins(session: AsyncDbSession, file_hash: str) -> int:
    select_count_stmt = select(func.count()).select_from(
        select(FilePinDb).where(FilePinDb.file_hash == file_hash).subquery()
    )
    return (await session.execute(select_count_stmt)).scalar_one()


async def find_file_pins(
    session: AsyncDbSession, item_hashes: Collection[str]
) -> Iterable[str]:
    select_stmt = select(MessageFilePinDb.item_hash).where(
        MessageFilePinDb.item_hash.in_(item_hashes)
    )
    return (await session.execute(select_stmt)).scalars()


async def delete_file_pin(session: AsyncDbSession, item_hash: str) -> None:
    delete_stmt = delete(MessageFilePinDb).where(
        MessageFilePinDb.item_hash == item_hash
    )
    await session.execute(delete_stmt)


async def insert_grace_period_file_pin(
    session: AsyncDbSession,
    file_hash: str,
    created: dt.datetime,
    delete_by: dt.datetime,
    item_hash: Optional[str] = None,
    owner: Optional[str] = None,
    ref: Optional[str] = None,
) -> None:
    insert_stmt = insert(GracePeriodFilePinDb).values(
        item_hash=item_hash,
        file_hash=file_hash,
        owner=owner,
        ref=ref,
        created=created,
        type=FilePinType.GRACE_PERIOD,
        delete_by=delete_by,
    )
    await session.execute(insert_stmt)


# TODO: Improve performance
async def update_file_pin_grace_period(
    session: AsyncDbSession,
    item_hash: str,
    delete_by: Union[dt.datetime, None],
) -> None:
    if delete_by is None:
        delete_stmt = (
            delete(GracePeriodFilePinDb)
            .where(GracePeriodFilePinDb.item_hash == item_hash)
            .returning(
                GracePeriodFilePinDb.file_hash,
                GracePeriodFilePinDb.owner,
                GracePeriodFilePinDb.ref,
                GracePeriodFilePinDb.created,
            )
        )

        grace_period = (await session.execute(delete_stmt)).first()
        if grace_period is None:
            return

        file_hash, owner, ref, created = grace_period

        await insert_message_file_pin(
            session=session,
            item_hash=item_hash,
            file_hash=file_hash,
            owner=owner,
            ref=ref,
            created=created,
        )
    else:
        delete_stmt = (
            delete(MessageFilePinDb)
            .where(MessageFilePinDb.item_hash == item_hash)
            .returning(
                MessageFilePinDb.file_hash,
                MessageFilePinDb.owner,
                MessageFilePinDb.ref,
                MessageFilePinDb.created,
            )
        )

        message_pin = (await session.execute(delete_stmt)).first()
        if message_pin is None:
            return

        file_hash, owner, ref, created = message_pin

        await insert_grace_period_file_pin(
            session=session,
            item_hash=item_hash,
            file_hash=file_hash,
            owner=owner,
            ref=ref,
            created=created,
            delete_by=delete_by,
        )

    await refresh_file_tag(
        session=session,
        tag=make_file_tag(
            owner=owner,
            ref=ref,
            item_hash=item_hash,
        ),
    )


async def delete_grace_period_file_pins(
    session: AsyncDbSession, datetime: dt.datetime
) -> None:
    delete_stmt = delete(GracePeriodFilePinDb).where(
        GracePeriodFilePinDb.delete_by < datetime
    )
    await session.execute(delete_stmt)


async def get_message_file_pin(
    session: AsyncDbSession, item_hash: str
) -> Optional[MessageFilePinDb]:
    stmt = (
        select(MessageFilePinDb)
        .options(
            selectinload(MessageFilePinDb.file)
        )  # select in load to avoid lazy loads / implicit IO
        .where(MessageFilePinDb.item_hash == item_hash)
    )

    return (await session.execute(stmt)).scalar_one_or_none()


async def get_address_files_stats(
    session: AsyncDbSession, owner: str
) -> Tuple[int, int]:
    select_stmt = (
        select(
            func.count().label("nb_files"),
            func.sum(StoredFileDb.size).label("total_size"),
        )
        .select_from(MessageFilePinDb)
        .join(StoredFileDb, MessageFilePinDb.file_hash == StoredFileDb.hash)
        .where(MessageFilePinDb.owner == owner)
    )
    result = (await session.execute(select_stmt)).one()
    return result.nb_files, result.total_size


async def get_address_files_for_api(
    session: AsyncDbSession,
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

    if sort_order == SortOrder.DESCENDING:
        order_by_columns = (
            MessageFilePinDb.created.desc(),
            MessageFilePinDb.item_hash.asc(),
        )
    else:  # ASCENDING
        order_by_columns = (
            MessageFilePinDb.item_hash.asc(),
            MessageFilePinDb.item_hash.asc(),
        )

    select_stmt = select_stmt.order_by(*order_by_columns)

    return (await session.execute(select_stmt)).all()


async def upsert_file(
    session: AsyncDbSession, file_hash: str, size: int, file_type: FileType
):
    upsert_file_stmt = (
        insert(StoredFileDb)
        .values(hash=file_hash, size=size, type=file_type)
        .on_conflict_do_nothing(constraint="files_pkey")
    )
    await session.execute(upsert_file_stmt)


async def get_file(session: AsyncDbSession, file_hash: str) -> Optional[StoredFileDb]:
    select_stmt = (
        select(StoredFileDb)
        .options(
            selectinload(StoredFileDb.pins),
            selectinload(StoredFileDb.tags),
        )
        .where(StoredFileDb.hash == file_hash)
    )
    return (await session.execute(select_stmt)).scalar_one_or_none()


async def delete_file(session: AsyncDbSession, file_hash: str) -> None:
    delete_stmt = delete(StoredFileDb).where(StoredFileDb.hash == file_hash)
    await session.execute(delete_stmt)


async def get_file_tag(session: AsyncDbSession, tag: FileTag) -> Optional[FileTagDb]:
    select_stmt = (
        select(FileTagDb)
        .options(
            selectinload(FileTagDb.file)
        )  # Avoid lazy load / Implicit IO https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
        .where(FileTagDb.tag == tag)
    )
    return (await session.execute(select_stmt)).scalar_one_or_none()


async def file_pin_exists(session: AsyncDbSession, item_hash: str) -> bool:
    return await FilePinDb.exists(
        session=session, where=FilePinDb.item_hash == item_hash
    )


async def file_tag_exists(session: AsyncDbSession, tag: FileTag) -> bool:
    return await FileTagDb.exists(session=session, where=FileTagDb.tag == tag)


async def find_file_tags(
    session: AsyncDbSession, tags: Collection[FileTag]
) -> Iterable[FileTag]:
    select_stmt = select(FileTagDb.tag).where(FileTagDb.tag.in_(tags))
    return (await session.execute(select_stmt)).scalars()


async def upsert_file_tag(
    session: AsyncDbSession,
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
    await session.execute(upsert_stmt)


async def refresh_file_tag(session: AsyncDbSession, tag: FileTag) -> None:
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
    await session.execute(delete(FileTagDb).where(FileTagDb.tag == tag))
    await session.execute(upsert_stmt)
