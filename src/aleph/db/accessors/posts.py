import datetime as dt
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
)

from aleph_message.models import Chain, ItemHash, ItemType
from sqlalchemy import TIMESTAMP, Float, String, case
from sqlalchemy import cast as sqla_cast
from sqlalchemy import (
    delete,
    extract,
    func,
    literal_column,
    nullsfirst,
    nullslast,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, array
from sqlalchemy.orm import aliased
from sqlalchemy.sql import Select

from aleph.db.models import ChainTxDb, MessageDb, message_confirmations
from aleph.db.models.posts import PostDb
from aleph.toolkit.timestamp import coerce_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortBy, SortOrder


class MergedPost(Protocol):
    item_hash: str
    content: Dict[str, Any]
    original_item_hash: str
    original_type: str
    owner: str
    ref: Optional[str]
    channel: Optional[Channel]
    last_updated: dt.datetime
    created: dt.datetime


class MergedPostV0(Protocol):
    chain: Chain
    item_hash: str
    content: Dict[str, Any]
    type: str
    item_type: ItemType
    item_content: Optional[str]
    original_item_hash: str
    original_type: str
    owner: str
    ref: Optional[str]
    channel: Optional[Channel]
    signature: str
    original_signature: str
    time: float
    size: int
    last_updated: dt.datetime


Amend = aliased(PostDb)
Original = aliased(PostDb)

OriginalMessage = aliased(MessageDb)
AmendMessage = aliased(MessageDb)


def make_select_merged_post_stmt() -> Select:
    """
    Combines posts and their latest amends according to the v1 /posts/ endpoint spec.
    """

    select_merged_post_stmt = (
        select(
            Original.item_hash.label("original_item_hash"),
            func.coalesce(Amend.item_hash, Original.item_hash).label("item_hash"),
            func.coalesce(Amend.content, Original.content).label("content"),
            Original.owner.label("owner"),
            Original.ref.label("ref"),
            func.coalesce(Amend.creation_datetime, Original.creation_datetime).label(
                "last_updated"
            ),
            Original.channel.label("channel"),
            Original.creation_datetime.label("created"),
            Original.type.label("original_type"),
            func.coalesce(Amend.tags, Original.tags).label("tags"),
        ).join(
            Amend,
            Original.latest_amend == Amend.item_hash,
            isouter=True,
        )
    ).where(Original.amends.is_(None))

    return select_merged_post_stmt


def _make_select_merged_post_v0_base_stmt() -> Select:
    """
    Originals + their latest amends, projected with the post-side
    columns needed by the v0 /posts/ endpoint.

    Unlike :func:`make_select_merged_post_stmt` the result also exposes
    ``latest_amend`` and the amend-aware ``type``, which lets callers
    defer the ``messages`` joins until *after* applying filters and
    LIMIT instead of paying for them on every candidate row.
    """

    return (
        select(
            Original.item_hash.label("original_item_hash"),
            func.coalesce(Amend.item_hash, Original.item_hash).label("item_hash"),
            Original.latest_amend.label("latest_amend"),
            func.coalesce(Amend.content, Original.content).label("content"),
            Original.owner.label("owner"),
            Original.ref.label("ref"),
            func.coalesce(Amend.creation_datetime, Original.creation_datetime).label(
                "last_updated"
            ),
            Original.channel.label("channel"),
            Original.creation_datetime.label("created"),
            func.coalesce(Amend.type, Original.type).label("type"),
            Original.type.label("original_type"),
            func.coalesce(Amend.tags, Original.tags).label("tags"),
        )
        .join(
            Amend,
            Original.latest_amend == Amend.item_hash,
            isouter=True,
        )
        .where(Original.amends.is_(None))
    )


def get_post(session: DbSession, item_hash: str) -> Optional[MergedPost]:
    select_stmt = make_select_merged_post_stmt()
    select_stmt = select_stmt.where(Original.item_hash == str(item_hash))
    return session.execute(select_stmt).one_or_none()


def get_original_post(session: DbSession, item_hash: str) -> Optional[PostDb]:
    select_stmt = select(PostDb).where(PostDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar()


def refresh_latest_amend(session: DbSession, item_hash: str) -> None:
    select_latest_amend = (
        select(
            PostDb.amends, func.max(PostDb.creation_datetime).label("creation_datetime")
        )
        .where(PostDb.amends == item_hash)
        .group_by(PostDb.amends)
        .subquery()
    )

    select_stmt = select(PostDb.item_hash).join(
        select_latest_amend,
        (PostDb.amends == select_latest_amend.c.amends)
        & (PostDb.creation_datetime == select_latest_amend.c.creation_datetime),
    )

    latest_amend_hash = session.execute(select_stmt).scalar()

    update_stmt = (
        update(PostDb)
        .where(PostDb.item_hash == item_hash)
        .values(latest_amend=latest_amend_hash)
    )

    session.execute(update_stmt)


def filter_post_select_stmt(
    select_stmt: Select,
    hashes: Optional[Sequence[ItemHash]] = None,
    addresses: Optional[Sequence[str]] = None,
    refs: Optional[Sequence[str]] = None,
    post_types: Optional[Sequence[str]] = None,
    tags: Optional[Sequence[str]] = None,
    channels: Optional[Sequence[Channel]] = None,
    start_date: Optional[Union[float, dt.datetime]] = None,
    end_date: Optional[Union[float, dt.datetime]] = None,
    sort_by: Optional[SortBy] = None,
    sort_order: Optional[SortOrder] = None,
    page: int = 0,
    pagination: int = 20,
    after_time: Optional[dt.datetime] = None,
    after_hash: Optional[str] = None,
    cursor_mode: bool = False,
) -> Select:
    select_merged_post_subquery = select_stmt.subquery()
    select_stmt = select(select_merged_post_subquery)

    start_datetime = coerce_to_datetime(start_date)
    end_datetime = coerce_to_datetime(end_date)

    last_updated_column = literal_column("last_updated", TIMESTAMP(timezone=True))

    if hashes:
        select_stmt = select_stmt.where(
            literal_column("original_item_hash", type_=String).in_(hashes)
        )
    if addresses:
        select_stmt = select_stmt.where(literal_column("owner").in_(addresses))
    if refs:
        select_stmt = select_stmt.where(literal_column("ref").in_(refs))
    if post_types:
        select_stmt = select_stmt.where(literal_column("original_type").in_(post_types))
    if tags:
        select_stmt = select_stmt.where(
            literal_column("tags", type_=ARRAY(String)).overlap(array(tags))
        )
    if channels:
        select_stmt = select_stmt.where(literal_column("channel").in_(channels))
    if start_datetime:
        select_stmt = select_stmt.where(last_updated_column >= start_datetime)
    if end_datetime:
        select_stmt = select_stmt.where(last_updated_column < end_datetime)

    order_by_columns: Tuple  # For mypy to leave us alone until SQLA2

    if sort_order:
        if sort_by == SortBy.TX_TIME:
            select_earliest_confirmation = (
                select(
                    message_confirmations.c.item_hash,
                    func.min(ChainTxDb.datetime).label("earliest_confirmation"),
                )
                .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
                .group_by(message_confirmations.c.item_hash)
            ).subquery()

            select_stmt = select_stmt.join(
                select_earliest_confirmation,
                select_merged_post_subquery.c.original_item_hash
                == select_earliest_confirmation.c.item_hash,
                isouter=True,
            )

            if sort_order == SortOrder.DESCENDING:
                order_by_columns = (
                    nullsfirst(
                        select_earliest_confirmation.c.earliest_confirmation.desc()
                    ),
                    select_merged_post_subquery.c.created.desc(),
                    select_merged_post_subquery.c.item_hash.asc(),
                )
            else:  # ASCENDING
                order_by_columns = (
                    nullslast(
                        select_earliest_confirmation.c.earliest_confirmation.asc()
                    ),
                    select_merged_post_subquery.c.created.asc(),
                    select_merged_post_subquery.c.item_hash.asc(),
                )
        else:
            if sort_order == SortOrder.DESCENDING:
                order_by_columns = (
                    last_updated_column.desc(),
                    select_merged_post_subquery.c.original_item_hash.asc(),
                )
            else:  # ASCENDING
                order_by_columns = (
                    last_updated_column.asc(),
                    select_merged_post_subquery.c.original_item_hash.asc(),
                )
        select_stmt = select_stmt.order_by(*order_by_columns)

    # Cursor filtering
    if after_time is not None and sort_by == SortBy.TIME:
        if sort_order == SortOrder.DESCENDING:
            select_stmt = select_stmt.where(
                (last_updated_column < after_time)
                | (
                    (last_updated_column == after_time)
                    & (select_merged_post_subquery.c.original_item_hash > after_hash)
                )
            )
        else:
            select_stmt = select_stmt.where(
                (last_updated_column > after_time)
                | (
                    (last_updated_column == after_time)
                    & (select_merged_post_subquery.c.original_item_hash > after_hash)
                )
            )
    elif page > 1:
        select_stmt = select_stmt.offset((page - 1) * pagination)

    if pagination:
        select_stmt = select_stmt.limit(
            pagination + 1 if after_time is not None or cursor_mode else pagination
        )

    return select_stmt


def count_matching_posts(
    session: DbSession,
    page: int = 1,
    pagination: int = 0,
    sort_by: SortBy = SortBy.TIME,
    sort_order: SortOrder = SortOrder.DESCENDING,
    start_date: float = 0,
    end_date: float = 0,
    **kwargs,
) -> int:
    # Note that we deliberately ignore the pagination parameters so that users can pass
    # the same parameters as get_matching_posts and get the total number of posts,
    # not just the number on a page.
    if kwargs:
        select_stmt = make_select_merged_post_stmt()
        select_stmt = filter_post_select_stmt(
            select_stmt=select_stmt,
            **kwargs,
            page=1,
            pagination=0,
            start_date=start_date,
            end_date=end_date,
        ).subquery()
    else:
        # Without filters, counting the number of original posts is faster.
        select_stmt = select(PostDb).where(PostDb.amends.is_(None)).subquery()

    select_count_stmt = select(func.count()).select_from(select_stmt)
    return session.execute(select_count_stmt).scalar_one()


def get_matching_posts_legacy(
    session: DbSession,
    # Same as make_matching_posts_query
    **kwargs,
) -> List[MergedPostV0]:
    """
    v0 /posts/ list query. Filters and LIMITs on ``posts`` first, then
    joins ``messages`` on the bounded result so we never fetch message
    fields for rows that get discarded by pagination.
    """
    base_stmt = _make_select_merged_post_v0_base_stmt()
    limited_stmt = filter_post_select_stmt(base_stmt, **kwargs)
    limited = limited_stmt.subquery()

    final_stmt = (
        select(
            limited.c.original_item_hash,
            limited.c.item_hash,
            OriginalMessage.chain.label("chain"),
            limited.c.content,
            case(
                (AmendMessage.item_type.is_(None), OriginalMessage.item_content),
                else_=AmendMessage.item_content,
            ).label("item_content"),
            func.coalesce(AmendMessage.item_type, OriginalMessage.item_type).label(
                "item_type"
            ),
            limited.c.owner,
            limited.c.ref,
            limited.c.last_updated,
            limited.c.channel,
            limited.c.created,
            limited.c.type,
            limited.c.original_type,
            func.coalesce(AmendMessage.signature, OriginalMessage.signature).label(
                "signature"
            ),
            OriginalMessage.signature.label("original_signature"),
            func.coalesce(AmendMessage.size, OriginalMessage.size).label("size"),
            sqla_cast(
                extract(
                    "epoch", func.coalesce(AmendMessage.time, OriginalMessage.time)
                ),
                Float,
            ).label("time"),
            limited.c.tags,
        )
        .select_from(limited)
        .join(
            OriginalMessage,
            OriginalMessage.item_hash == limited.c.original_item_hash,
        )
        .join(
            AmendMessage,
            AmendMessage.item_hash == limited.c.latest_amend,
            isouter=True,
        )
    )

    # The inner subquery applies the LIMIT, but wrapping it discards row
    # order, so re-apply ORDER BY on the bounded set (cheap, at most a
    # few hundred rows).
    sort_by: Optional[SortBy] = kwargs.get("sort_by")
    sort_order: Optional[SortOrder] = kwargs.get("sort_order")
    if sort_order is not None:
        if sort_by == SortBy.TX_TIME:
            select_earliest_confirmation = (
                select(
                    message_confirmations.c.item_hash,
                    func.min(ChainTxDb.datetime).label("earliest_confirmation"),
                )
                .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
                .group_by(message_confirmations.c.item_hash)
            ).subquery()
            final_stmt = final_stmt.join(
                select_earliest_confirmation,
                select_earliest_confirmation.c.item_hash
                == limited.c.original_item_hash,
                isouter=True,
            )
            if sort_order == SortOrder.DESCENDING:
                final_stmt = final_stmt.order_by(
                    nullsfirst(
                        select_earliest_confirmation.c.earliest_confirmation.desc()
                    ),
                    limited.c.created.desc(),
                    limited.c.item_hash.asc(),
                )
            else:
                final_stmt = final_stmt.order_by(
                    nullslast(
                        select_earliest_confirmation.c.earliest_confirmation.asc()
                    ),
                    limited.c.created.asc(),
                    limited.c.item_hash.asc(),
                )
        else:
            if sort_order == SortOrder.DESCENDING:
                final_stmt = final_stmt.order_by(
                    limited.c.last_updated.desc(),
                    limited.c.original_item_hash.asc(),
                )
            else:
                final_stmt = final_stmt.order_by(
                    limited.c.last_updated.asc(),
                    limited.c.original_item_hash.asc(),
                )

    return cast(List[MergedPostV0], session.execute(final_stmt).all())


def get_matching_posts(
    session: DbSession,
    # Same as make_matching_posts_query
    **kwargs,
) -> List[MergedPost]:
    select_stmt = make_select_merged_post_stmt()
    filtered_select_stmt = filter_post_select_stmt(select_stmt, **kwargs)
    return cast(List[MergedPost], session.execute(filtered_select_stmt).all())


def delete_amends(session: DbSession, item_hash: str) -> Iterable[str]:
    return session.execute(
        delete(PostDb).where(PostDb.amends == item_hash).returning(PostDb.item_hash)
    ).scalars()


def delete_post(session: DbSession, item_hash: str) -> None:
    session.execute(delete(PostDb).where(PostDb.item_hash == item_hash))
