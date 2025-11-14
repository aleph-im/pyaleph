import datetime as dt
import traceback
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Union, overload

from aleph_message.models import Chain, ItemHash, MessageType
from sqlalchemy import delete, func, nullsfirst, nullslast, select, text, update
from sqlalchemy.dialects.postgresql import array, insert
from sqlalchemy.orm import contains_eager, load_only, selectinload
from sqlalchemy.sql import Insert, Select
from sqlalchemy.sql.elements import literal

from aleph.db.accessors.cost import delete_costs_for_message
from aleph.toolkit.timestamp import coerce_to_datetime, utc_now
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    ErrorCode,
    MessageProcessingException,
    MessageStatus,
)
from aleph.types.sort_order import SortBy, SortOrder

from ..models.chains import ChainTxDb
from ..models.messages import (
    ForgottenMessageDb,
    MessageDb,
    MessageStatusDb,
    RejectedMessageDb,
    message_confirmations,
)
from ..models.pending_messages import PendingMessageDb
from .pending_messages import delete_pending_message


def get_message_by_item_hash(
    session: DbSession, item_hash: ItemHash
) -> Optional[MessageDb]:
    select_stmt = (
        select(MessageDb)
        .where(MessageDb.item_hash == item_hash)
        .options(selectinload(MessageDb.confirmations))
    )
    return (session.execute(select_stmt)).scalar()


def message_exists(session: DbSession, item_hash: str) -> bool:
    return MessageDb.exists(
        session=session,
        where=MessageDb.item_hash == item_hash,
    )


def get_one_message_by_item_hash(
    session: DbSession, item_hash: str
) -> Optional[RejectedMessageDb]:
    select_stmt = select(MessageDb).where(MessageDb.item_hash == item_hash)
    return session.execute(select_stmt).scalar_one_or_none()


def make_matching_messages_query(
    hashes: Optional[Sequence[ItemHash]] = None,
    addresses: Optional[Sequence[str]] = None,
    refs: Optional[Sequence[str]] = None,
    chains: Optional[Sequence[Chain]] = None,
    message_type: Optional[MessageType] = None,
    message_types: Optional[Sequence[MessageType]] = None,
    message_statuses: Optional[Sequence[MessageStatus]] = None,
    start_date: Optional[Union[float, dt.datetime]] = None,
    end_date: Optional[Union[float, dt.datetime]] = None,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    content_hashes: Optional[Sequence[ItemHash]] = None,
    content_types: Optional[Sequence[str]] = None,
    tags: Optional[Sequence[str]] = None,
    channels: Optional[Sequence[str]] = None,
    sort_by: SortBy = SortBy.TIME,
    sort_order: SortOrder = SortOrder.DESCENDING,
    page: int = 1,
    pagination: int = 20,
    include_confirmations: bool = False,
    # TODO: remove once all filters are supported
    **kwargs,
) -> Select:
    select_stmt = select(MessageDb)

    if message_statuses:
        select_stmt = (
            select_stmt.join(
                MessageStatusDb, MessageDb.item_hash == MessageStatusDb.item_hash
            )
            .where(MessageStatusDb.status.in_(message_statuses))
            .options(
                contains_eager(MessageDb.status).options(
                    load_only(MessageStatusDb.status)
                )
            )
        )

    if include_confirmations:
        # Note: we assume this is only used for the API, so we only load the fields
        # returned by the API. If additional fields are required, add them here to
        # avoid additional queries.
        select_stmt = select_stmt.options(
            selectinload(MessageDb.confirmations).options(
                load_only(ChainTxDb.hash, ChainTxDb.chain, ChainTxDb.height)
            )
        )

    start_datetime = coerce_to_datetime(start_date)
    end_datetime = coerce_to_datetime(end_date)

    if hashes:
        select_stmt = select_stmt.where(MessageDb.item_hash.in_(hashes))
    if addresses:
        select_stmt = select_stmt.where(MessageDb.sender.in_(addresses))
    if chains:
        select_stmt = select_stmt.where(MessageDb.chain.in_(chains))
    if message_types:
        select_stmt = select_stmt.where(MessageDb.type.in_(message_types))
    if message_type:
        select_stmt = select_stmt.where(MessageDb.type == message_type)
    if start_datetime:
        select_stmt = select_stmt.where(MessageDb.time >= start_datetime)
    if end_datetime:
        select_stmt = select_stmt.where(MessageDb.time < end_datetime)
    if refs:
        select_stmt = select_stmt.where(MessageDb.content["ref"].astext.in_(refs))
    if content_hashes:
        select_stmt = select_stmt.where(
            MessageDb.content["item_hash"].astext.in_(content_hashes)
        )
    if content_types:
        select_stmt = select_stmt.where(MessageDb.content_type.in_(content_types))
    if tags:
        select_stmt = select_stmt.where(
            MessageDb.content["content"]["tags"].has_any(array(tags))
        )
    if channels:
        select_stmt = select_stmt.where(MessageDb.channel.in_(channels))

    order_by_columns: Tuple = ()  # For mypy to leave us alone until SQLA2

    if sort_by == SortBy.TX_TIME or start_block or end_block:
        select_earliest_confirmation = (
            select(
                message_confirmations.c.item_hash,
                func.min(ChainTxDb.datetime).label("earliest_confirmation"),
                ChainTxDb.height,
            )
            .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
            .group_by(message_confirmations.c.item_hash, ChainTxDb.height)
        ).subquery()
        select_stmt = select_stmt.join(
            select_earliest_confirmation,
            MessageDb.item_hash == select_earliest_confirmation.c.item_hash,
            isouter=True,
        )
        if start_block:
            select_stmt = select_stmt.where(
                select_earliest_confirmation.c.height.is_(None)
                | (select_earliest_confirmation.c.height >= start_block)
            )
        if end_block:
            select_stmt = select_stmt.where(
                select_earliest_confirmation.c.height < end_block
            )

        sort_by = SortBy.TX_TIME
        if sort_by == SortBy.TX_TIME:
            order_by_columns = (
                (
                    nullsfirst(
                        select_earliest_confirmation.c.earliest_confirmation.desc()
                    ),
                    MessageDb.time.desc(),
                    MessageDb.item_hash.asc(),
                )
                if sort_order == SortOrder.DESCENDING
                else (
                    nullslast(
                        select_earliest_confirmation.c.earliest_confirmation.asc()
                    ),
                    MessageDb.time.asc(),
                    MessageDb.item_hash.asc(),
                )
            )
    else:
        if sort_order == SortOrder.DESCENDING:
            order_by_columns = (
                MessageDb.time.desc(),
                MessageDb.item_hash.asc(),
            )
        else:  # ASCENDING
            order_by_columns = (
                MessageDb.time.asc(),
                MessageDb.item_hash.asc(),
            )

    select_stmt = select_stmt.order_by(*order_by_columns).offset(
        (page - 1) * pagination
    )

    # If pagination == 0, return all matching results
    if pagination:
        select_stmt = select_stmt.limit(pagination)

    return select_stmt


def count_matching_messages(
    session: DbSession,
    start_date: float = 0.0,
    end_date: float = 0.0,
    start_block: int = 0,
    end_block: int = 0,
    sort_by: SortBy = SortBy.TIME,
    sort_order: SortOrder = SortOrder.DESCENDING,
    page: int = 1,
    pagination: int = 0,
    **kwargs,
) -> int:
    # Note that we deliberately ignore the pagination parameters so that users can pass
    # the same parameters as get_matching_messages and get the total number of messages,
    # not just the number on a page.
    if kwargs or start_date or end_date or start_block or end_block:
        select_stmt = make_matching_messages_query(
            **kwargs,
            start_date=start_date,
            end_date=end_date,
            start_block=start_block,
            end_block=end_block,
            include_confirmations=False,
            page=1,
            pagination=0,
        ).subquery()
        select_count_stmt = select(func.count()).select_from(select_stmt)
        return session.execute(select_count_stmt).scalar_one()

    return MessageDb.fast_count(session=session)


def get_matching_messages(
    session: DbSession,
    **kwargs,  # Same as make_matching_messages_query
) -> Iterable[MessageDb]:
    """
    Applies the specified filters on the message table and returns matching entries.
    """
    select_stmt = make_matching_messages_query(**kwargs)
    return (session.execute(select_stmt)).scalars()


def get_message_stats_by_address(
    session: DbSession,
    addresses: Optional[Sequence[str]] = None,
):
    """
    Get message stats for user addresses.

    :param session: DB session object.
    :param addresses: If specified, restricts the list of results to these addresses.
                      otherwise, results for all addresses are returned.
    :return: A list of (sender, message_type, count) tuples.
    """
    parameters = {}
    select_stmt = "select address, type, nb_messages from address_stats_mat_view"

    if addresses:
        # Tuples are supported as array parameters by SQLAlchemy
        addresses_tuple = (
            addresses if isinstance(addresses, tuple) else tuple(addresses)
        )

        select_stmt += " where address in :addresses"
        parameters = {"addresses": addresses_tuple}

    return session.execute(text(select_stmt), parameters).all()


def refresh_address_stats_mat_view(session: DbSession) -> None:
    session.execute(
        text("refresh materialized view concurrently address_stats_mat_view")
    )


# TODO: declare a type that will match the result (something like UnconfirmedMessageDb)
#       and translate the time field to epoch.
def get_unconfirmed_messages(
    session: DbSession, limit: int = 100, chain: Optional[Chain] = None
) -> Iterable[MessageDb]:

    if chain is None:
        select_message_confirmations = select(message_confirmations.c.item_hash).where(
            message_confirmations.c.item_hash == MessageDb.item_hash
        )
    else:
        select_message_confirmations = (
            select(message_confirmations.c.item_hash)
            .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
            .where(
                (message_confirmations.c.item_hash == MessageDb.item_hash)
                & (ChainTxDb.chain == chain)
            )
        )

    select_stmt = (
        select(MessageDb)
        .join(MessageStatusDb, MessageStatusDb.item_hash == MessageDb.item_hash)
        .where(
            MessageDb.signature.isnot(None) & (~select_message_confirmations.exists())
        )
        .order_by(MessageStatusDb.reception_time.asc())
    )

    return (session.execute(select_stmt.limit(limit))).scalars()


def make_message_upsert_query(message: MessageDb) -> Insert:
    return (
        insert(MessageDb)
        .values(message.to_dict(exclude={"content_type"}))
        .on_conflict_do_update(
            constraint="messages_pkey",
            set_={"time": func.least(MessageDb.time, message.time)},
        )
    )


def make_confirmation_upsert_query(item_hash: str, tx_hash: str) -> Insert:
    return (
        insert(message_confirmations)
        .values(item_hash=item_hash, tx_hash=tx_hash)
        .on_conflict_do_nothing()
    )


def get_message_status(
    session: DbSession, item_hash: ItemHash
) -> Optional[MessageStatusDb]:
    return (
        session.execute(
            select(MessageStatusDb).where(MessageStatusDb.item_hash == str(item_hash))
        )
    ).scalar()


def get_rejected_message(
    session: DbSession, item_hash: str
) -> Optional[RejectedMessageDb]:
    select_stmt = select(RejectedMessageDb).where(
        RejectedMessageDb.item_hash == item_hash
    )
    return session.execute(select_stmt).scalar()


# TODO typing: Find a correct type for `where`
def make_message_status_upsert_query(
    item_hash: str, new_status: MessageStatus, reception_time: dt.datetime, where
) -> Insert:
    return (
        insert(MessageStatusDb)
        .values(item_hash=item_hash, status=new_status, reception_time=reception_time)
        .on_conflict_do_update(
            constraint="message_status_pkey",
            set_={
                "status": new_status,
                "reception_time": func.least(
                    MessageStatusDb.reception_time, reception_time
                ),
            },
            where=where,
        )
    )


def get_distinct_channels(session: DbSession) -> Iterable[Channel]:
    select_stmt = select(MessageDb.channel).distinct().order_by(MessageDb.channel)
    return session.execute(select_stmt).scalars()


def get_forgotten_message(
    session: DbSession, item_hash: str
) -> Optional[ForgottenMessageDb]:
    return session.execute(
        select(ForgottenMessageDb).where(ForgottenMessageDb.item_hash == item_hash)
    ).scalar()


def forget_message(
    session: DbSession, item_hash: str, forget_message_hash: str
) -> None:
    """
    Marks a processed message as forgotten.

    Expects the caller to perform checks to determine whether the message is
    in the proper state.

    :param session: DB session.
    :param item_hash: Hash of the message to forget.
    :param forget_message_hash: Hash of the forget message.
    """

    copy_row_stmt = insert(ForgottenMessageDb).from_select(
        [
            "item_hash",
            "type",
            "chain",
            "sender",
            "signature",
            "item_type",
            "time",
            "channel",
            "forgotten_by",
        ],
        select(
            MessageDb.item_hash,
            MessageDb.type,
            MessageDb.chain,
            MessageDb.sender,
            MessageDb.signature,
            MessageDb.item_type,
            MessageDb.time,
            MessageDb.channel,
            literal(f"{{{forget_message_hash}}}"),
        ).where(MessageDb.item_hash == item_hash),
    )
    session.execute(copy_row_stmt)
    session.execute(
        update(MessageStatusDb)
        .values(status=MessageStatus.FORGOTTEN)
        .where(MessageStatusDb.item_hash == item_hash)
    )
    session.execute(
        delete(message_confirmations).where(
            message_confirmations.c.item_hash == item_hash
        )
    )
    session.execute(delete(MessageDb).where(MessageDb.item_hash == item_hash))

    delete_costs_for_message(
        session=session,
        item_hash=item_hash,
    )


def append_to_forgotten_by(
    session: DbSession, forgotten_message_hash: str, forget_message_hash: str
) -> None:
    update_stmt = (
        update(ForgottenMessageDb)
        .where(ForgottenMessageDb.item_hash == forgotten_message_hash)
        .values(
            forgotten_by=text(
                f"array_append({ForgottenMessageDb.forgotten_by.name}, :forget_hash)"
            )
        )
    )
    session.execute(update_stmt, {"forget_hash": forget_message_hash})


def make_upsert_rejected_message_statement(
    item_hash: str,
    pending_message_dict: Mapping[str, Any],
    error_code: int,
    details: Optional[Mapping[str, Any]] = None,
    exc_traceback: Optional[str] = None,
    tx_hash: Optional[str] = None,
) -> Insert:
    # Convert details to a dictionary that is JSON serializable
    serializable_details = None
    if details is not None:
        try:
            # First try a simple dict conversion
            serializable_details = dict(details)

            # Now recursively ensure all values within the dictionary are JSON serializable
            def ensure_serializable(obj):
                if isinstance(obj, dict):
                    return {k: ensure_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [ensure_serializable(item) for item in obj]
                elif isinstance(obj, (str, int, float, bool, type(None))):
                    return obj
                else:
                    # For non-serializable types, convert to string
                    return str(obj)

            serializable_details = ensure_serializable(serializable_details)

        except Exception:
            # If any conversion fails, create a new dict with a message
            serializable_details = {
                "error": "Details contained non-serializable values"
            }

    insert_rejected_message_stmt = insert(RejectedMessageDb).values(
        item_hash=item_hash,
        message=pending_message_dict,
        error_code=error_code,
        details=serializable_details,
        traceback=exc_traceback,
        tx_hash=tx_hash,
    )
    upsert_rejected_message_stmt = insert_rejected_message_stmt.on_conflict_do_update(
        constraint="rejected_messages_pkey",
        set_={
            "error_code": insert_rejected_message_stmt.excluded.error_code,
            "details": serializable_details,
            "traceback": insert_rejected_message_stmt.excluded.traceback,
            "tx_hash": tx_hash,
        },
    )
    return upsert_rejected_message_stmt


def mark_pending_message_as_rejected(
    session: DbSession,
    item_hash: str,
    pending_message_dict: Mapping[str, Any],
    exception: BaseException,
    tx_hash: Optional[str],
) -> RejectedMessageDb:
    if isinstance(exception, MessageProcessingException):
        error_code = exception.error_code
        details = exception.details()
        exc_traceback = None

        # Fix for ValueError in details - ensure all values are JSON serializable
        if details and "errors" in details:
            for error in details["errors"]:
                if (
                    isinstance(error, dict)
                    and "ctx" in error
                    and isinstance(error["ctx"], dict)
                ):
                    for key, value in list(error["ctx"].items()):
                        # Convert any ValueError or other exceptions to strings
                        if isinstance(value, Exception):
                            error["ctx"][key] = str(value)
    else:
        error_code = ErrorCode.INTERNAL_ERROR
        details = None
        exc_traceback = "\n".join(
            traceback.format_exception(
                type(exception), exception, exception.__traceback__
            )
        )

    upsert_status_stmt = make_message_status_upsert_query(
        item_hash=item_hash,
        new_status=MessageStatus.REJECTED,
        reception_time=utc_now(),
        where=MessageStatusDb.status == MessageStatus.PENDING,
    )

    upsert_rejected_message_stmt = make_upsert_rejected_message_statement(
        item_hash=item_hash,
        pending_message_dict=pending_message_dict,
        details=details,
        error_code=error_code,
        exc_traceback=exc_traceback,
        tx_hash=tx_hash,
    )

    session.execute(upsert_status_stmt)
    session.execute(upsert_rejected_message_stmt)

    return RejectedMessageDb(
        item_hash=item_hash,
        message=pending_message_dict,
        traceback=exc_traceback,
        error_code=error_code,
        details=details,
        tx_hash=tx_hash,
    )


@overload
def reject_new_pending_message(
    session: DbSession,
    pending_message: Mapping[str, Any],
    exception: BaseException,
    tx_hash: Optional[str],
) -> None: ...


@overload
def reject_new_pending_message(
    session: DbSession,
    pending_message: PendingMessageDb,
    exception: BaseException,
    tx_hash: Optional[str],
) -> None: ...


def reject_new_pending_message(
    session: DbSession,
    pending_message: Union[Mapping[str, Any], PendingMessageDb],
    exception: BaseException,
    tx_hash: Optional[str],
) -> Optional[RejectedMessageDb]:
    """
    Reject a pending message that is not yet in the DB.
    """

    pending_message_dict: Mapping[str, Any]

    if isinstance(pending_message, PendingMessageDb):
        pending_message_dict = pending_message.to_dict(
            exclude={
                "id",
                "check_message",
                "retries",
                "fetched",
                "tx_hash",
                "reception_time",
            }
        )
    else:
        pending_message_dict = pending_message

    # If the message does not even have an item hash, we just drop it silently.
    try:
        item_hash = pending_message_dict["item_hash"]
    except KeyError:
        return None

    # The message may already be processed and someone is sending invalid copies.
    # Just do nothing if that is the case. We just consider the case where a previous
    # message with the same item hash was already sent to replace the error message
    # (ex: someone is retrying a message after fixing an error).
    message_status = get_message_status(session=session, item_hash=item_hash)
    if message_status:
        if message_status.status != MessageStatus.REJECTED:
            return None

    return mark_pending_message_as_rejected(
        session=session,
        item_hash=item_hash,
        pending_message_dict=pending_message_dict,
        exception=exception,
        tx_hash=tx_hash,
    )


def reject_existing_pending_message(
    session: DbSession,
    pending_message: PendingMessageDb,
    exception: BaseException,
) -> Optional[RejectedMessageDb]:
    item_hash = pending_message.item_hash

    # The message may already be processed and someone is sending invalid copies.
    # Just drop the pending message.
    message_status = get_message_status(session=session, item_hash=ItemHash(item_hash))
    if message_status:
        if message_status.status not in (MessageStatus.PENDING, MessageStatus.REJECTED):
            delete_pending_message(session=session, pending_message=pending_message)
            return None

    # TODO: use Pydantic schema
    pending_message_dict = pending_message.to_dict(
        exclude={
            "id",
            "check_message",
            "retries",
            "next_attempt",
            "fetched",
            "tx_hash",
            "reception_time",
        }
    )
    pending_message_dict["time"] = pending_message_dict["time"].timestamp()

    rejected_message = mark_pending_message_as_rejected(
        session=session,
        item_hash=item_hash,
        pending_message_dict=pending_message_dict,
        exception=exception,
        tx_hash=pending_message.tx_hash,
    )
    delete_pending_message(session=session, pending_message=pending_message)
    return rejected_message


def get_programs_triggered_by_messages(session: DbSession, sort_order: SortOrder):
    time_column = MessageDb.time
    order_by_column = (
        time_column.desc() if sort_order == SortOrder.DESCENDING else time_column.asc()
    )

    message_selector_expr = MessageDb.content["on", "message"]

    select_stmt = (
        select(
            MessageDb.item_hash,
            message_selector_expr.label("message_subscriptions"),
        )
        .where(
            (MessageDb.type == MessageType.program)
            & (message_selector_expr.is_not(None))
        )
        .order_by(order_by_column)
    )

    return session.execute(select_stmt).all()


def make_matching_hashes_query(
    start_date: Optional[Union[float, dt.datetime]] = None,
    end_date: Optional[Union[float, dt.datetime]] = None,
    status: Optional[MessageStatus] = None,
    sort_order: SortOrder = SortOrder.DESCENDING,
    page: int = 1,
    pagination: int = 20,
    hash_only: bool = True,
) -> Select:
    select_stmt = select(MessageStatusDb.item_hash if hash_only else MessageStatusDb)

    start_datetime = coerce_to_datetime(start_date)
    end_datetime = coerce_to_datetime(end_date)

    if start_datetime:
        select_stmt = select_stmt.where(
            MessageStatusDb.reception_time >= start_datetime
        )
    if end_datetime:
        select_stmt = select_stmt.where(MessageStatusDb.reception_time < end_datetime)
    if status:
        select_stmt = select_stmt.where(MessageStatusDb.status == status)

    order_by_columns: Tuple = ()

    if sort_order == SortOrder.DESCENDING:
        order_by_columns = (
            MessageStatusDb.reception_time.desc(),
            MessageStatusDb.item_hash.asc(),
        )
    else:  # ASCENDING
        order_by_columns = (
            MessageStatusDb.reception_time.asc(),
            MessageStatusDb.item_hash.asc(),
        )

    select_stmt = select_stmt.order_by(*order_by_columns)

    select_stmt = select_stmt.offset((page - 1) * pagination)

    # If pagination == 0, return all matching results
    if pagination:
        select_stmt = select_stmt.limit(pagination)

    return select_stmt


def get_matching_hashes(
    session: DbSession,
    **kwargs,  # Same as make_matching_hashes_query
):
    select_stmt = make_matching_hashes_query(**kwargs)
    return (session.execute(select_stmt)).scalars()


def count_matching_hashes(
    session: DbSession,
    pagination: int = 0,
    **kwargs,
) -> Select:
    select_stmt = make_matching_hashes_query(pagination=0, **kwargs).subquery()
    select_count_stmt = select(func.count()).select_from(select_stmt)
    return session.execute(select_count_stmt).scalar_one()
