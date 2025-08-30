import datetime as dt
from decimal import Decimal
from typing import Dict, Mapping, Optional, Sequence

from aleph_message.models import Chain
from sqlalchemy import func, select
from sqlalchemy.sql import Select

from aleph.db.models import AlephBalanceDb
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import AsyncDbSession


async def get_balance_by_chain(
    session: AsyncDbSession, address: str, chain: Chain, dapp: Optional[str] = None
) -> Optional[Decimal]:
    query = select(AlephBalanceDb.balance).where(
        AlephBalanceDb.address == address, AlephBalanceDb.chain == chain
    )

    if dapp:
        # For some reason asyncpg don't handle it if dapp is None
        query = query.where(AlephBalanceDb.dapp == dapp)

    balance = (await session.execute(query)).scalar_one_or_none()
    return balance


def make_balances_by_chain_query(
    chains: Optional[Sequence[Chain]] = None,
    page: int = 1,
    pagination: int = 100,
    min_balance: int = 0,
) -> Select:
    query = select(AlephBalanceDb.address, AlephBalanceDb.balance, AlephBalanceDb.chain)

    if chains:
        query = query.where(AlephBalanceDb.chain.in_(chains))

    if min_balance > 0:
        query = query.filter(AlephBalanceDb.balance >= min_balance)

    query = query.offset((page - 1) * pagination)

    # If pagination == 0, return all matching results
    if pagination:
        query = query.limit(pagination)

    return query


async def get_balances_by_chain(session: AsyncDbSession, **kwargs):
    select_stmt = make_balances_by_chain_query(**kwargs)
    return (await session.execute(select_stmt)).all()


async def count_balances_by_chain(
    session: AsyncDbSession, pagination: int = 0, **kwargs
) -> Decimal:
    select_stmt = make_balances_by_chain_query(pagination=0, **kwargs).subquery()
    select_count_stmt = select(func.count()).select_from(select_stmt)
    return (await session.execute(select_count_stmt)).scalar_one()


async def get_total_balance(
    session: AsyncDbSession, address: str, include_dapps: bool = False
) -> Decimal:
    where_clause = AlephBalanceDb.address == address
    if not include_dapps:
        where_clause = where_clause & AlephBalanceDb.dapp.is_(None)
    select_stmt = (
        select(
            AlephBalanceDb.address, func.sum(AlephBalanceDb.balance).label("balance")
        )
        .where(where_clause)
        .group_by(AlephBalanceDb.address)
    )

    result = (await session.execute(select_stmt)).one_or_none()
    return Decimal(0) if result is None else result.balance or Decimal(0)


async def get_total_detailed_balance(
    session: AsyncDbSession,
    address: str,
    chain: Optional[str] = None,
    include_dapps: bool = False,
) -> tuple[Decimal, Dict[str, Decimal]]:
    if chain is not None:
        query = (
            select(func.sum(AlephBalanceDb.balance))
            .where(
                (AlephBalanceDb.address == address)
                & (AlephBalanceDb.chain == chain)
                & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
            )
            .group_by(AlephBalanceDb.address)
        )

        result = (await session.execute(query)).first()
        return result[0] if result is not None else Decimal(0), {}

    query = (
        select(AlephBalanceDb.chain, func.sum(AlephBalanceDb.balance).label("balance"))
        .where(
            (AlephBalanceDb.address == address)
            & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
        )
        .group_by(AlephBalanceDb.chain)
    )

    balances_by_chain = {
        row.chain: row.balance or Decimal(0)
        for row in (await session.execute(query)).fetchall()
    }

    query = (
        select(func.sum(AlephBalanceDb.balance))
        .where(
            (AlephBalanceDb.address == address)
            & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
        )
        .group_by(AlephBalanceDb.address)
    )

    result = (await session.execute(query)).first()
    return result[0] if result is not None else Decimal(0), balances_by_chain


async def update_balances(
    session: AsyncDbSession,
    chain: Chain,
    dapp: Optional[str],
    eth_height: int,
    balances: Mapping[str, float],
) -> None:
    """
    Updates multiple balances at the same time, efficiently.

    Upserting balances one by one takes too much time if done naively.
    The alternative, implemented here, is to bulk insert balances in a temporary
    table using the COPY operator and then upserting records into the main balances
    table from the temporary one.
    """

    last_update = utc_now()

    raw_conn = await (await session.connection()).get_raw_connection()
    asyncpg_conn = raw_conn.driver_connection

    # Then create the temporary table
    await asyncpg_conn.execute(
        "CREATE TEMPORARY TABLE temp_balances AS SELECT * FROM balances WITH NO DATA"
    )

    # Convert floats to str to avoid having issue with float to decimal conversion

    records = [
        (address, chain.value, dapp or "", str(balance), eth_height, last_update)
        for address, balance in balances.items()
    ]

    await asyncpg_conn.copy_records_to_table(
        table_name="temp_balances",
        records=records,
        columns=["address", "chain", "dapp", "balance", "eth_height", "last_update"],
    )

    await asyncpg_conn.execute(
        """
        INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update)
            (SELECT address, chain, dapp, balance, eth_height, last_update FROM temp_balances)
            ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex DO UPDATE
            SET balance = excluded.balance, eth_height = excluded.eth_height, last_update = (CASE WHEN excluded.balance <> balances.balance THEN excluded.last_update ELSE balances.last_update END)
            WHERE excluded.eth_height > balances.eth_height
        """
    )

    # Temporary tables are dropped at the same time as the connection, but SQLAlchemy
    # tends to reuse connections. Dropping the table here guarantees it will not be present
    # on the next run.
    await asyncpg_conn.execute("DROP TABLE temp_balances")


async def get_updated_balance_accounts(
    session: AsyncDbSession, last_update: dt.datetime
):
    select_stmt = (
        select(AlephBalanceDb.address)
        .where(AlephBalanceDb.last_update >= last_update)
        .distinct()
    )
    return (await session.execute(select_stmt)).scalars().all()
