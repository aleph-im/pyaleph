from decimal import Decimal
from typing import Iterable, List, Optional

from aleph_message.models import PaymentType
from sqlalchemy import asc, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import Insert

from aleph.db.models import ChainTxDb, message_confirmations
from aleph.db.models.account_costs import AccountCostsDb
from aleph.toolkit.costs import format_cost
from aleph.types.db_session import DbSession


def get_total_cost_for_address(
    session: DbSession,
    address: str,
    payment_type: Optional[PaymentType] = PaymentType.hold,
) -> Decimal:
    total_prop = (
        AccountCostsDb.cost_hold
        if payment_type == PaymentType.hold
        else AccountCostsDb.cost_stream
    )

    select_stmt = (
        select(func.sum(total_prop))
        .select_from(AccountCostsDb)
        .where(
            (AccountCostsDb.owner == address)
            & (AccountCostsDb.payment_type == payment_type)
        )
    )

    total_cost = session.execute(select_stmt).scalar()
    return format_cost(Decimal(total_cost or 0))


def get_total_costs_for_address_grouped_by_message(
    session: DbSession,
    address: str,
    payment_type: Optional[PaymentType] = PaymentType.hold,
):
    total_prop = (
        AccountCostsDb.cost_hold
        if payment_type == PaymentType.hold
        else AccountCostsDb.cost_stream
    )

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
        },
    )
