import datetime as dt
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, Mapping, Optional, Sequence
import time
import random

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


def get_credit_balance(session: DbSession, address: str) -> int:
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

    return result if result is not None else 0


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


def _bulk_insert_credit_balances(
    session: DbSession,
    csv_rows: Sequence[str],
) -> None:
    """
    Generic function to bulk insert credit balance rows using temporary table.
    
    Args:
        session: Database session
        csv_rows: List of CSV-formatted strings with credit balance data
    """
    
    # Generate unique table name with timestamp and random suffix to avoid race conditions
    timestamp = int(time.time() * 1000000)  # microseconds
    random_suffix = random.randint(1000, 9999)
    temp_table_name = f"temp_credit_balances_{timestamp}_{random_suffix}"
    
    # Drop the temporary table if it exists from a previous operation
    session.execute(f"DROP TABLE IF EXISTS {temp_table_name}")  # type: ignore[arg-type]
    
    session.execute(
        f"CREATE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM credit_balances WITH NO DATA"  # type: ignore[arg-type]
    )

    conn = session.connection().connection
    cursor = conn.cursor()

    # Common column specification for all credit balance types
    # Common fields first: address, amount, credit_ref, credit_index, last_update
    # Optional fields last: ratio, tx_hash, expiration_date, token, chain, origin, provider, origin_ref, payment_method
    copy_columns = "address, amount, credit_ref, credit_index, last_update, ratio, tx_hash, expiration_date, token, chain, origin, provider, origin_ref, payment_method"
    
    csv_credit_balances = StringIO("\n".join(csv_rows))
    cursor.copy_expert(
        f"COPY {temp_table_name}({copy_columns}) FROM STDIN WITH CSV DELIMITER ';'",
        csv_credit_balances,
    )

    
    # Common insert query that handles all message types with proper type casting
    insert_query = f"""
        INSERT INTO credit_balances({copy_columns})
            (SELECT {copy_columns} FROM {temp_table_name})
            ON CONFLICT ON CONSTRAINT credit_balances_pkey DO NOTHING
        """
    
    session.execute(insert_query)  # type: ignore[arg-type]

    # Drop the temporary table
    session.execute(f"DROP TABLE {temp_table_name}")  # type: ignore[arg-type]



def get_updated_credit_balance_accounts(session: DbSession, last_update: dt.datetime):
    """
    Get addresses that have had their credit balances updated since the given timestamp.
    """
    select_stmt = (
        select(AlephCreditBalanceDb.address)
        .where(AlephCreditBalanceDb.last_update >= last_update)
        .distinct()
    )
    return session.execute(select_stmt).scalars().all()



def update_credit_balances_distribution(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    token: str,
    chain: str,
    message_hash: str,
) -> None:
    """
    Updates credit balances for distribution messages (aleph_credit_distribution).
    
    Distribution messages include all fields like ratio, tx_hash, provider, 
    payment_method, token, chain, and expiration_date.
    """

    last_update = utc_now()
    csv_rows = []

    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        amount = abs(int(credit_entry["amount"]))
        ratio = Decimal(credit_entry["ratio"])
        tx_hash = credit_entry["tx_hash"]
        provider = credit_entry["provider"]

        # Extract optional fields from each credit entry
        expiration_timestamp = credit_entry.get("expiration", "")
        origin = credit_entry.get("origin", "")
        origin_ref = credit_entry.get("ref", "")
        payment_method = credit_entry.get("payment_method", "")

        # Convert expiration timestamp to datetime

        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp != ""
            else None
        )

        csv_rows.append(
            f"{address};{amount};{message_hash};{index};{last_update};{ratio};{tx_hash};{expiration_date or ''};{token};{chain};{origin};{provider};{origin_ref};{payment_method}"
        )

    _bulk_insert_credit_balances(session, csv_rows)

def update_credit_balances_expense(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    message_hash: str,
) -> None:
    """
    Updates credit balances for expense messages (aleph_credit_expense).
    
    Expense messages have negative amounts and only include origin_ref field.
    Other fields like ratio, tx_hash, provider, payment_method, token, chain, 
    origin, and expiration_date are not present.
    """
    
    last_update = utc_now()
    csv_rows = []
    
    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        amount = -abs(int(credit_entry["amount"]))
        origin_ref = credit_entry.get("ref", "")

        csv_rows.append(
            f"{address};{amount};{message_hash};{index};{last_update};;;;;;;ALEPH;{origin_ref};credit_expense"
        )

    _bulk_insert_credit_balances(session, csv_rows)


def update_credit_balances_transfer(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    sender_address: str,
    whitelisted_addresses: Sequence[str],
    message_hash: str,
) -> None:
    """
    Updates credit balances for transfer messages (aleph_credit_transfer).
    
    Transfer messages involve two entries per transfer:
    - One negative entry for the sender (subtracting credits)
    - One positive entry for the recipient (adding credits)
    
    Special case: If sender is in the whitelisted addresses, only add credits to recipient.
    """
    
    last_update = utc_now()
    csv_rows = []
    index = 0
    
    for credit_entry in credits_list:
        recipient_address = credit_entry["address"]
        amount = abs(int(credit_entry["amount"]))
        expiration_timestamp = credit_entry.get("expiration", "")
        
        # Convert expiration timestamp to datetime
        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp != ""
            else None
        )

        # Add positive entry for recipient (origin = sender, provider = ALEPH, payment_method = credit_transfer)
        csv_rows.append(
            f"{recipient_address};{amount};{message_hash};{index};{last_update};;;{expiration_date or ''};;;{sender_address};ALEPH;;credit_transfer"
        )
        index += 1

        # Add negative entry for sender (unless sender is in whitelisted addresses)
        # (origin = recipient, provider = ALEPH, payment_method = credit_transfer)
        if sender_address not in whitelisted_addresses:
            csv_rows.append(
                f"{sender_address};{-amount};{message_hash};{index};{last_update};;;;;;{recipient_address};ALEPH;;credit_transfer"
            )
            index += 1

    _bulk_insert_credit_balances(session, csv_rows)


def validate_credit_transfer_balance(
    session: DbSession,
    sender_address: str,
    total_transfer_amount: int,
) -> bool:
    """
    Validates if the sender has enough credit balance to process a transfer.
    
    Args:
        session: Database session
        sender_address: Address of the sender
        total_transfer_amount: Total amount to be transferred
        
    Returns:
        True if sender has sufficient balance, False otherwise
    """
    current_balance = get_credit_balance(session, sender_address)
    return current_balance >= total_transfer_amount
