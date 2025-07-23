import datetime as dt
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, Mapping, Optional, Sequence

from aleph_message.models import Chain
from sqlalchemy import func, select
from sqlalchemy.sql import Select

from aleph.db.models import AlephBalanceDb, AlephCreditBalanceDb
from aleph.toolkit.timestamp import utc_now
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


def make_balances_by_chain_query(
    session: DbSession,
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


def get_balances_by_chain(session: DbSession, **kwargs):
    select_stmt = make_balances_by_chain_query(session=session, **kwargs)
    return (session.execute(select_stmt)).all()


def count_balances_by_chain(session: DbSession, pagination: int = 0, **kwargs):
    select_stmt = make_balances_by_chain_query(
        session=session, pagination=0, **kwargs
    ).subquery()
    select_count_stmt = select(func.count()).select_from(select_stmt)
    return session.execute(select_count_stmt).scalar_one()


def get_total_balance(
    session: DbSession, address: str, include_dapps: bool = False
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

    result = session.execute(select_stmt).one_or_none()
    return Decimal(0) if result is None else result.balance or Decimal(0)


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

    last_update = utc_now()

    session.execute(
        "CREATE TEMPORARY TABLE temp_balances AS SELECT * FROM balances WITH NO DATA"  # type: ignore[arg-type]
    )

    conn = session.connection().connection
    cursor = conn.cursor()

    # Prepare an in-memory CSV file for use with the COPY operator
    csv_balances = StringIO(
        "\n".join(
            [
                f"{address};{chain.value};{dapp or ''};{balance};{eth_height};{last_update}"
                for address, balance in balances.items()
            ]
        )
    )
    cursor.copy_expert(
        "COPY temp_balances(address, chain, dapp, balance, eth_height, last_update) FROM STDIN WITH CSV DELIMITER ';'",
        csv_balances,
    )
    session.execute(
        """
        INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update)
            (SELECT address, chain, dapp, balance, eth_height, last_update FROM temp_balances)
            ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex DO UPDATE
            SET balance = excluded.balance, eth_height = excluded.eth_height, last_update = (CASE WHEN excluded.balance <> balances.balance THEN excluded.last_update ELSE balances.last_update END)
            WHERE excluded.eth_height > balances.eth_height
        """  # type: ignore[arg-type]
    )

    # Temporary tables are dropped at the same time as the connection, but SQLAlchemy
    # tends to reuse connections. Dropping the table here guarantees it will not be present
    # on the next run.
    session.execute("DROP TABLE temp_balances")  # type: ignore[arg-type]


def get_updated_balance_accounts(session: DbSession, last_update: dt.datetime):
    select_stmt = (
        select(AlephBalanceDb.address)
        .where(AlephBalanceDb.last_update >= last_update)
        .distinct()
    )
    return (session.execute(select_stmt)).scalars().all()


def get_credit_balance(session: DbSession, address: str) -> Decimal:
    now = utc_now()

    # Sum all non-expired credit balances for the address
    result = session.execute(
        select(func.sum(AlephCreditBalanceDb.amount)).where(
            (AlephCreditBalanceDb.address == address)
            & (
                (AlephCreditBalanceDb.expiration_date.is_(None))
                | (AlephCreditBalanceDb.expiration_date > now)
            )
        )
    ).scalar()

    return result if result is not None else Decimal(0)


def get_credit_balances(
    session: DbSession,
    page: int = 1,
    pagination: int = 100,
    min_balance: int = 0,
    **kwargs,
):
    now = utc_now()

    # Get aggregated non-expired credit balances by address
    subquery = (
        select(
            AlephCreditBalanceDb.address,
            func.sum(AlephCreditBalanceDb.amount).label("credits"),
        )
        .where(
            (AlephCreditBalanceDb.expiration_date.is_(None))
            | (AlephCreditBalanceDb.expiration_date > now)
        )
        .group_by(AlephCreditBalanceDb.address)
    ).subquery()

    query = select(subquery.c.address, subquery.c.credits)

    if min_balance > 0:
        query = query.filter(subquery.c.credits >= min_balance)

    query = query.offset((page - 1) * pagination)

    if pagination:
        query = query.limit(pagination)

    return session.execute(query).all()


def count_credit_balances(session: DbSession, min_balance: int = 0, **kwargs):
    from aleph.toolkit.timestamp import utc_now

    now = utc_now()

    # Count unique addresses with non-expired credit balances
    subquery = (
        select(AlephCreditBalanceDb.address)
        .where(AlephCreditBalanceDb.expiration_date > now)
        .group_by(AlephCreditBalanceDb.address)
    )

    if min_balance > 0:
        subquery = subquery.having(func.sum(AlephCreditBalanceDb.amount) >= min_balance)

    query = select(func.count()).select_from(subquery.subquery())

    return session.execute(query).scalar_one()


def update_credit_balances(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    token: str,
    chain: str,
) -> None:
    """
    Updates multiple credit balances at the same time, efficiently.

    Similar to update_balances, this uses a temporary table and bulk operations
    for better performance.
    """

    last_update = utc_now()

    session.execute(
        "CREATE TEMPORARY TABLE temp_credit_balances AS SELECT * FROM credit_balances WITH NO DATA"  # type: ignore[arg-type]
    )

    conn = session.connection().connection
    cursor = conn.cursor()

    # Prepare an in-memory CSV file for use with the COPY operator
    csv_rows = []
    for credit_entry in credits_list:
        address = credit_entry["address"]
        amount = Decimal(credit_entry["amount"])  # Cast from string to Decimal
        ratio = Decimal(credit_entry["ratio"])
        tx_hash = credit_entry["tx_hash"]
        provider = credit_entry["provider"]

        # Extract optional fields from each credit entry
        expiration_timestamp = credit_entry.get("expiration", "")
        origin = credit_entry.get("origin", "")
        ref = credit_entry.get("ref", "")
        payment_method = credit_entry.get("payment_method", "")

        # Convert expiration timestamp to datetime

        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp != ""
            else None
        )

        csv_rows.append(
            f"{address};{amount};{ratio};{tx_hash};{expiration_date or ''};{token};{chain};{origin};{provider};{ref};{payment_method};{last_update}"
        )

    csv_credit_balances = StringIO("\n".join(csv_rows))
    cursor.copy_expert(
        "COPY temp_credit_balances(address, amount, ratio, tx_hash, expiration_date, token, chain, origin, provider, ref, payment_method, last_update) FROM STDIN WITH CSV DELIMITER ';'",
        csv_credit_balances,
    )
    session.execute(
        """
        INSERT INTO credit_balances(address, amount, ratio, tx_hash, expiration_date, token, chain, origin, provider, ref, payment_method, last_update)
            (SELECT address, amount, ratio, tx_hash, expiration_date, token, chain, 
             NULLIF(origin, ''), provider, NULLIF(ref, ''), NULLIF(payment_method, ''), last_update FROM temp_credit_balances)
            ON CONFLICT ON CONSTRAINT credit_balances_tx_hash_uindex DO NOTHING
        """  # type: ignore[arg-type]
    )

    # Drop the temporary table
    session.execute("DROP TABLE temp_credit_balances")  # type: ignore[arg-type]
