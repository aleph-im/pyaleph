from decimal import Decimal

from sqlalchemy import func, select, text

from aleph.types.db_session import DbSession


def get_total_cost_for_address(session: DbSession, address: str) -> Decimal:
    select_stmt = (
        select(func.sum(text("total_cost")))
        .select_from(text("public.costs_view"))
        .where(text("address = :address"))
    ).params(address=address)

    total_cost = session.execute(select_stmt).scalar()
    return Decimal(total_cost) if total_cost is not None else Decimal(0)
