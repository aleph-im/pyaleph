from decimal import Decimal
from typing import Iterable, List, Optional

from aleph_message.models import PaymentType
from sqlalchemy import asc, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import Insert

from aleph.db.models import ChainTxDb, message_confirmations
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.messages import MessageStatusDb
from aleph.toolkit.costs import format_cost, format_cost_str
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus


def get_total_cost_for_address(
    session: DbSession,
    address: str,
    payment_type: Optional[PaymentType] = PaymentType.hold,
) -> Decimal:
    """Get total cost for an address filtered by payment type.

    Uses get_costs_summary internally but returns a single Decimal value
    for the specified payment type.
    """
    summary = get_costs_summary(
        session=session,
        address=address,
        payment_type=payment_type,
    )

    if payment_type == PaymentType.superfluid:
        return format_cost(Decimal(summary["total_cost_stream"]))
    elif payment_type == PaymentType.credit:
        return format_cost(Decimal(summary["total_cost_credit"]))
    else:
        return format_cost(Decimal(summary["total_cost_hold"]))


def get_total_costs_for_address_grouped_by_message(
    session: DbSession,
    address: str,
    payment_type: Optional[PaymentType] = PaymentType.hold,
):
    if payment_type == PaymentType.hold:
        total_prop = AccountCostsDb.cost_hold
    elif payment_type == PaymentType.superfluid:
        total_prop = AccountCostsDb.cost_stream
    elif payment_type == PaymentType.credit:
        total_prop = AccountCostsDb.cost_credit
    else:
        total_prop = AccountCostsDb.cost_hold

    id_field = func.min(AccountCostsDb.id)

    select_stmt = (
        select(
            AccountCostsDb.item_hash, ChainTxDb.height, func.sum(total_prop), id_field
        )
        .select_from(AccountCostsDb)
        .join(
            message_confirmations,
            message_confirmations.c.item_hash == AccountCostsDb.item_hash,
        )
        .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
        .where(
            (AccountCostsDb.owner == address)
            & (AccountCostsDb.payment_type == payment_type)
        )
        .group_by(AccountCostsDb.item_hash, ChainTxDb.height)
        .order_by(asc(id_field))
    )

    return (session.execute(select_stmt)).all()


def get_message_costs(session: DbSession, item_hash: str) -> Iterable[AccountCostsDb]:
    select_stmt = select(AccountCostsDb).where(AccountCostsDb.item_hash == item_hash)
    return (session.execute(select_stmt)).scalars().all()


def make_costs_upsert_query(costs: List[AccountCostsDb]) -> Insert:
    costs_dict = [
        cost.to_dict(
            exclude={
                "id",
            }
        )
        for cost in costs
    ]

    upsert_stmt = insert(AccountCostsDb).values(costs_dict)

    return upsert_stmt.on_conflict_do_update(
        constraint="account_costs_owner_item_hash_type_name_key",
        set_={
            "cost_hold": upsert_stmt.excluded.cost_hold,
            "cost_stream": upsert_stmt.excluded.cost_stream,
            "cost_credit": upsert_stmt.excluded.cost_credit,
        },
    )


def delete_costs_for_message(session: DbSession, item_hash: str) -> None:
    delete_stmt = delete(AccountCostsDb).where(AccountCostsDb.item_hash == item_hash)
    session.execute(delete_stmt)


def delete_costs_for_forgotten_and_deleted_messages(session: DbSession) -> None:
    delete_stmt = (
        delete(AccountCostsDb)
        .where(AccountCostsDb.item_hash == MessageStatusDb.item_hash)
        .where(
            (MessageStatusDb.status == MessageStatus.FORGOTTEN)
            | (MessageStatusDb.status == MessageStatus.REMOVED)
        )
        .execution_options(synchronize_session=False)
    )
    session.execute(delete_stmt)


def get_costs_summary(
    session: DbSession,
    address: Optional[str] = None,
    item_hash: Optional[str] = None,
    payment_type: Optional[PaymentType] = None,
) -> dict:
    """
    Get aggregated cost summary with optional filtering.

    Args:
        session: Database session
        address: Optional filter by owner address
        item_hash: Optional filter by specific resource
        payment_type: Optional filter by payment type (hold, superfluid, credit)

    Returns:
        Dictionary with total_cost_hold, total_cost_stream, total_cost_credit, resource_count
    """
    select_stmt = select(
        func.sum(AccountCostsDb.cost_hold).label("total_cost_hold"),
        func.sum(AccountCostsDb.cost_stream).label("total_cost_stream"),
        func.sum(AccountCostsDb.cost_credit).label("total_cost_credit"),
        func.count(func.distinct(AccountCostsDb.item_hash)).label("resource_count"),
    ).select_from(AccountCostsDb)

    if address:
        select_stmt = select_stmt.where(AccountCostsDb.owner == address)
    if item_hash:
        select_stmt = select_stmt.where(AccountCostsDb.item_hash == item_hash)
    if payment_type:
        select_stmt = select_stmt.where(AccountCostsDb.payment_type == payment_type)

    result = session.execute(select_stmt).one()

    return {
        "total_cost_hold": format_cost_str(Decimal(result.total_cost_hold or 0)),
        "total_cost_stream": format_cost_str(Decimal(result.total_cost_stream or 0)),
        "total_cost_credit": format_cost_str(Decimal(result.total_cost_credit or 0)),
        "resource_count": result.resource_count or 0,
    }


def get_resources_with_costs(
    session: DbSession,
    address: Optional[str] = None,
    item_hash: Optional[str] = None,
    payment_type: Optional[PaymentType] = None,
    page: int = 1,
    pagination: int = 100,
) -> list:
    """
    Get list of resources with their aggregated costs.

    Returns a list of resources with item_hash, owner, payment_type,
    and aggregated costs (cost_hold, cost_stream, cost_credit).
    """
    select_stmt = (
        select(
            AccountCostsDb.item_hash,
            AccountCostsDb.owner,
            AccountCostsDb.payment_type,
            func.sum(AccountCostsDb.cost_hold).label("cost_hold"),
            func.sum(AccountCostsDb.cost_stream).label("cost_stream"),
            func.sum(AccountCostsDb.cost_credit).label("cost_credit"),
        )
        .select_from(AccountCostsDb)
        .group_by(
            AccountCostsDb.item_hash,
            AccountCostsDb.owner,
            AccountCostsDb.payment_type,
        )
        .order_by(AccountCostsDb.item_hash)
    )

    if address:
        select_stmt = select_stmt.where(AccountCostsDb.owner == address)
    if item_hash:
        select_stmt = select_stmt.where(AccountCostsDb.item_hash == item_hash)
    if payment_type:
        select_stmt = select_stmt.where(AccountCostsDb.payment_type == payment_type)

    select_stmt = select_stmt.offset((page - 1) * pagination)
    if pagination:
        select_stmt = select_stmt.limit(pagination)

    return list(session.execute(select_stmt).all())


def count_resources_with_costs(
    session: DbSession,
    address: Optional[str] = None,
    item_hash: Optional[str] = None,
    payment_type: Optional[PaymentType] = None,
) -> int:
    """
    Count distinct resources matching the given filters.
    """
    select_stmt = select(
        func.count(func.distinct(AccountCostsDb.item_hash))
    ).select_from(AccountCostsDb)

    if address:
        select_stmt = select_stmt.where(AccountCostsDb.owner == address)
    if item_hash:
        select_stmt = select_stmt.where(AccountCostsDb.item_hash == item_hash)
    if payment_type:
        select_stmt = select_stmt.where(AccountCostsDb.payment_type == payment_type)

    return session.execute(select_stmt).scalar() or 0
