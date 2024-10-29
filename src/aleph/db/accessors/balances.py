from decimal import Decimal
from io import StringIO
from typing import Dict, Mapping, Optional

from aleph_message.models import Chain
from sqlalchemy import func, select

from aleph.db.models import AlephBalanceDb
from aleph.types.db_session import DbSession


def get_balance_by_chain(
    session: DbSession, address: str, chain: Chain, dapp: Optional[str] = None
) -> Optional[Decimal]:
    return session.execute(
        select(AlephBalanceDb.balance).where(
            (AlephBalanceDb.address == address)
            & (AlephBalanceDb.chain == chain.value)
            & (AlephBalanceDb.dapp == dapp)
        )
    ).scalar()


def get_total_balance(
    session: DbSession, address: str, include_dapps: bool = False
) -> Optional[Decimal]:
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

    result = session.execute(select_stmt).one_or_none()
    if result is None:
        return None

    return result.balance


def get_total_detailed_balance(
    session: DbSession,
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

        result = session.execute(query).first()
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
        for row in session.execute(query).fetchall()
    }

    query = (
        select(func.sum(AlephBalanceDb.balance))
        .where(
            (AlephBalanceDb.address == address)
            & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
        )
        .group_by(AlephBalanceDb.address)
    )

    result = session.execute(query).first()
    return result[0] if result is not None else Decimal(0), balances_by_chain


def update_balances(
    session: DbSession,
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

    session.execute(
        "CREATE TEMPORARY TABLE temp_balances AS SELECT * FROM balances WITH NO DATA"  # type: ignore[arg-type]
    )

    conn = session.connection().connection
    cursor = conn.cursor()

    # Prepare an in-memory CSV file for use with the COPY operator
    csv_balances = StringIO(
        "\n".join(
            [
                f"{address};{chain.value};{dapp or ''};{balance};{eth_height}"
                for address, balance in balances.items()
            ]
        )
    )
    cursor.copy_expert(
        "COPY temp_balances(address, chain, dapp, balance, eth_height) FROM STDIN WITH CSV DELIMITER ';'",
        csv_balances,
    )
    session.execute(
        """
        INSERT INTO balances(address, chain, dapp, balance, eth_height)
            (SELECT address, chain, dapp, balance, eth_height FROM temp_balances) 
            ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex DO UPDATE 
            SET balance = excluded.balance, eth_height = excluded.eth_height 
            WHERE excluded.eth_height > balances.eth_height
        """  # type: ignore[arg-type]
    )

    # Temporary tables are dropped at the same time as the connection, but SQLAlchemy
    # tends to reuse connections. Dropping the table here guarantees it will not be present
    # on the next run.
    session.execute("DROP TABLE temp_balances")  # type: ignore[arg-type]
