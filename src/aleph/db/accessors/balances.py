import datetime as dt
import random
import time
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, Mapping, Optional, Sequence

from aleph_message.models import Chain
from sqlalchemy import func, select, text
from sqlalchemy.sql import Select

from aleph.db.models import AlephBalanceDb, AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.toolkit.constants import (
    CREDIT_PRECISION_CUTOFF_TIMESTAMP,
    CREDIT_PRECISION_MULTIPLIER,
)
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.db_session import DbSession


def _apply_credit_precision_multiplier(
    amount: int, message_timestamp: dt.datetime
) -> int:
    """
    Apply the credit precision multiplier for messages before the cutoff.

    Old messages (before cutoff) have amounts in old format (100 credits = 1 USD)
    and need to be multiplied by 10,000 to match new format (1,000,000 credits = 1 USD).
    """
    cutoff_datetime = timestamp_to_datetime(CREDIT_PRECISION_CUTOFF_TIMESTAMP)
    if message_timestamp < cutoff_datetime:
        return amount * CREDIT_PRECISION_MULTIPLIER
    return amount


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
        balance_on_chain_query = (
            select(func.sum(AlephBalanceDb.balance))
            .where(
                (AlephBalanceDb.address == address)
                & (AlephBalanceDb.chain == chain)
                & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
            )
            .group_by(AlephBalanceDb.address)
        )

        result = session.execute(balance_on_chain_query).first()
        return result[0] if result is not None else Decimal(0), {}

    balance_by_chain_query = (
        select(AlephBalanceDb.chain, func.sum(AlephBalanceDb.balance).label("balance"))
        .where(
            (AlephBalanceDb.address == address)
            & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
        )
        .group_by(AlephBalanceDb.chain)
    )

    balances_by_chain = {
        row.chain: row.balance or Decimal(0)
        for row in session.execute(balance_by_chain_query).fetchall()
    }

    total_balance_query = (
        select(func.sum(AlephBalanceDb.balance))
        .where(
            (AlephBalanceDb.address == address)
            & ((AlephBalanceDb.dapp.is_(None)) if not include_dapps else True)
        )
        .group_by(AlephBalanceDb.address)
    )

    result = session.execute(total_balance_query).first()
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
        text(
            "CREATE TEMPORARY TABLE temp_balances AS SELECT * FROM balances WITH NO DATA"
        )
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
        text(
            """
        INSERT INTO balances(address, chain, dapp, balance, eth_height, last_update)
            (SELECT address, chain, dapp, balance, eth_height, last_update FROM temp_balances)
            ON CONFLICT ON CONSTRAINT balances_address_chain_dapp_uindex DO UPDATE
            SET balance = excluded.balance, eth_height = excluded.eth_height, last_update = (CASE WHEN excluded.balance <> balances.balance THEN excluded.last_update ELSE balances.last_update END)
            WHERE excluded.eth_height > balances.eth_height
        """
        )
    )

    # Temporary tables are dropped at the same time as the connection, but SQLAlchemy
    # tends to reuse connections. Dropping the table here guarantees it will not be present
    # on the next run.
    session.execute(text("DROP TABLE temp_balances"))


def get_updated_balance_accounts(session: DbSession, last_update: dt.datetime):
    select_stmt = (
        select(AlephBalanceDb.address)
        .where(AlephBalanceDb.last_update >= last_update)
        .distinct()
    )
    return (session.execute(select_stmt)).scalars().all()


@dataclass
class PositiveCredit:
    amount: int
    expiration_date: Optional[dt.datetime]
    timestamp: dt.datetime
    remaining: int


@dataclass
class NegativeAmount:
    amount: int
    timestamp: dt.datetime


def _calculate_credit_balance_fifo(
    session: DbSession, address: str, now: Optional[dt.datetime] = None
) -> int:
    """
    Calculate credit balance using FIFO consumption strategy.

    This function implements the core FIFO logic:
    1. Get all positive credits (ordered by message_timestamp)
    2. Get all negative amounts (expenses/transfers)
    3. Apply negative amounts to oldest credits first, but only if the expense
       occurred before the credit's expiration date
    4. Return remaining balance considering current expiration status
    """

    now = now if now is not None else utc_now()

    # Get all credit history for this address, ordered by message timestamp
    records = (
        session.execute(
            select(AlephCreditHistoryDb)
            .where(AlephCreditHistoryDb.address == address)
            .order_by(AlephCreditHistoryDb.message_timestamp.asc())
        )
        .scalars()
        .all()
    )

    # Separate positive credits and negative amounts
    positive_credits = []
    negative_amounts = []

    for record in records:
        if record.amount > 0:
            positive_credits.append(
                PositiveCredit(
                    amount=record.amount,
                    expiration_date=record.expiration_date,
                    timestamp=record.message_timestamp,
                    remaining=record.amount,
                )
            )
        else:
            negative_amounts.append(
                NegativeAmount(
                    amount=abs(record.amount), timestamp=record.message_timestamp
                )
            )

    # Apply negative amounts using FIFO strategy
    for expense in negative_amounts:
        remaining_expense = expense.amount

        # Consume from oldest credits first
        for credit in positive_credits:
            if remaining_expense <= 0:
                break

            # Can only consume if expense happened before credit expiration
            # If credit has no expiration (None), expense can always consume
            # If credit has expiration, expense must have occurred before expiration
            expense_valid = (
                credit.expiration_date is None
                or expense.timestamp < credit.expiration_date
            )

            if expense_valid and credit.remaining > 0:
                consumed = min(credit.remaining, remaining_expense)
                credit.remaining -= consumed
                remaining_expense -= consumed

    # Sum remaining balances from currently non-expired credits
    total_balance = 0
    for credit in positive_credits:
        if credit.expiration_date is None or credit.expiration_date > now:
            total_balance += credit.remaining

    return max(0, total_balance)


def get_credit_balance(
    session: DbSession, address: str, now: Optional[dt.datetime] = None
) -> int:
    """
    Get credit balance using lazy recalculation strategy.

    1. Check if cached balance exists in credit_balances table
    2. Check if credit_history has newer entries than cached balance
    3. Check if any credits have expiration dates that occurred after the cache's last update
    4. If recalculation is needed, recalculate using FIFO and update cache
    5. Return cached balance
    """

    now = now if now is not None else utc_now()

    # Get the timestamp of the most recent credit history entry for this address
    latest_history_timestamp = session.execute(
        select(func.max(AlephCreditHistoryDb.last_update)).where(
            AlephCreditHistoryDb.address == address
        )
    ).scalar()

    # If no history exists, balance is 0
    if latest_history_timestamp is None:
        return 0

    # Get cached balance if it exists
    cached_balance = session.execute(
        select(AlephCreditBalanceDb).where(AlephCreditBalanceDb.address == address)
    ).scalar_one_or_none()

    # Check if recalculation is needed
    needs_recalculation = (
        cached_balance is None or cached_balance.last_update < latest_history_timestamp
    )

    # Also check if any credits have expiration dates that occurred after the cache's last update
    # This handles the case where credits expired since the last cache update
    if not needs_recalculation and cached_balance is not None:
        # Check for any credits with expiration dates between cache last_update and now
        earliest_expiration_after_cache = session.execute(
            select(func.min(AlephCreditHistoryDb.expiration_date)).where(
                (AlephCreditHistoryDb.address == address)
                & (AlephCreditHistoryDb.expiration_date.isnot(None))
                & (AlephCreditHistoryDb.expiration_date > cached_balance.last_update)
                & (AlephCreditHistoryDb.expiration_date <= now)
            )
        ).scalar()

        needs_recalculation = earliest_expiration_after_cache is not None

    if needs_recalculation:
        # Recalculate balance using FIFO
        new_balance = _calculate_credit_balance_fifo(session, address, now)

        if cached_balance is None:
            # Create new cache entry
            session.add(
                AlephCreditBalanceDb(
                    address=address, balance=new_balance, last_update=now
                )
            )
        else:
            # Update existing cache entry
            cached_balance.balance = new_balance
            cached_balance.last_update = now

        session.flush()
        return new_balance

    return cached_balance.balance if cached_balance else 0


def get_credit_balances(
    session: DbSession,
    page: int = 1,
    pagination: int = 100,
    min_balance: int = 0,
) -> list[tuple[str, int]]:
    """
    Get paginated credit balances for all addresses.
    Uses the cached balances from the credit_balances table.
    """
    query = select(AlephCreditBalanceDb.address, AlephCreditBalanceDb.balance)

    if min_balance > 0:
        query = query.filter(AlephCreditBalanceDb.balance >= min_balance)

    query = query.offset((page - 1) * pagination)

    if pagination:
        query = query.limit(pagination)

    # Return results in the expected format (address, credits)
    results = session.execute(query).all()
    return [(row.address, row.balance) for row in results]


def count_credit_balances(session: DbSession, min_balance: int = 0) -> int:
    """
    Count addresses with credit balances.
    Uses the cached balances from the credit_balances table.
    """
    query = select(func.count(AlephCreditBalanceDb.address))

    if min_balance > 0:
        query = query.filter(AlephCreditBalanceDb.balance >= min_balance)

    return session.execute(query).scalar_one()


def _bulk_insert_credit_history(
    session: DbSession,
    csv_rows: Sequence[str],
) -> None:
    """
    Generic function to bulk insert credit history rows using temporary table.

    Args:
        session: Database session
        csv_rows: List of CSV-formatted strings with credit history data
    """

    # Generate unique table name with timestamp and random suffix to avoid race conditions
    timestamp = int(time.time() * 1000000)  # microseconds
    random_suffix = random.randint(1000, 9999)
    temp_table_name = f"temp_credit_history_{timestamp}_{random_suffix}"

    # Drop the temporary table if it exists from a previous operation
    session.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))

    session.execute(
        text(
            f"CREATE TEMPORARY TABLE {temp_table_name} AS SELECT * FROM credit_history WITH NO DATA"
        )
    )

    conn = session.connection().connection
    cursor = conn.cursor()

    # Column specification for credit history
    # Include the new message_timestamp and bonus_amount fields
    copy_columns = "address, amount, credit_ref, credit_index, message_timestamp, last_update, price, bonus_amount, tx_hash, expiration_date, token, chain, origin, provider, origin_ref, payment_method"

    csv_credit_history = StringIO("\n".join(csv_rows))
    cursor.copy_expert(
        f"COPY {temp_table_name}({copy_columns}) FROM STDIN WITH CSV DELIMITER ';'",
        csv_credit_history,
    )

    # Insert query for credit history
    insert_query = text(
        f"""
        INSERT INTO credit_history({copy_columns})
            (SELECT {copy_columns} FROM {temp_table_name})
            ON CONFLICT ON CONSTRAINT credit_history_pkey DO NOTHING
        """
    )

    session.execute(insert_query)

    # Drop the temporary table
    session.execute(text(f"DROP TABLE {temp_table_name}"))


def get_updated_credit_balance_accounts(session: DbSession, last_update: dt.datetime):
    """
    Get addresses that have had their credit history updated since the given timestamp.
    """
    select_stmt = (
        select(AlephCreditHistoryDb.address)
        .where(AlephCreditHistoryDb.last_update >= last_update)
        .distinct()
    )
    return session.execute(select_stmt).scalars().all()


def update_credit_balances_distribution(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    token: str,
    chain: str,
    message_hash: str,
    message_timestamp: dt.datetime,
) -> None:
    """
    Updates credit balances for distribution messages (aleph_credit_distribution).

    Distribution messages include all fields like price, bonus_amount, tx_hash, provider,
    payment_method, token, chain, and expiration_date.
    """

    last_update = utc_now()
    csv_rows = []

    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        raw_amount = abs(int(credit_entry["amount"]))
        amount = _apply_credit_precision_multiplier(raw_amount, message_timestamp)
        price = Decimal(credit_entry["price"])
        tx_hash = credit_entry["tx_hash"]
        provider = credit_entry["provider"]

        # Extract optional fields from each credit entry
        expiration_timestamp = credit_entry.get("expiration", "")
        origin = credit_entry.get("origin", "")
        origin_ref = credit_entry.get("ref", "")
        payment_method = credit_entry.get("payment_method", "")
        bonus_amount = credit_entry.get("bonus_amount", "")

        # Convert expiration timestamp to datetime

        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp != ""
            else None
        )

        csv_rows.append(
            f"{address};{amount};{message_hash};{index};{message_timestamp};{last_update};{price};{bonus_amount or ''};{tx_hash};{expiration_date or ''};{token};{chain};{origin};{provider};{origin_ref};{payment_method}"
        )

    _bulk_insert_credit_history(session, csv_rows)


def update_credit_balances_expense(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    message_hash: str,
    message_timestamp: dt.datetime,
) -> None:
    """
    Updates credit balances for expense messages (aleph_credit_expense).

    Expense messages have negative amounts and can include:
    - execution_id (mapped to origin)
    - node_id (mapped to tx_hash)
    - price (mapped to price)
    - time (skipped for now)
    - ref (mapped to origin_ref)
    """

    last_update = utc_now()
    csv_rows = []

    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        raw_amount = abs(int(credit_entry["amount"]))
        amount = -_apply_credit_precision_multiplier(raw_amount, message_timestamp)
        origin_ref = credit_entry.get("ref", "")

        # Map new fields
        origin = credit_entry.get("execution_id", "")
        tx_hash = credit_entry.get("node_id", "")
        price = credit_entry.get("price", "")
        # Skip time field for now

        csv_rows.append(
            f"{address};{amount};{message_hash};{index};{message_timestamp};{last_update};{price};;{tx_hash};;;;{origin};ALEPH;{origin_ref};credit_expense"
        )

    _bulk_insert_credit_history(session, csv_rows)


def update_credit_balances_transfer(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    sender_address: str,
    whitelisted_addresses: Sequence[str],
    message_hash: str,
    message_timestamp: dt.datetime,
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
        raw_amount = abs(int(credit_entry["amount"]))
        amount = _apply_credit_precision_multiplier(raw_amount, message_timestamp)
        expiration_timestamp = credit_entry.get("expiration", "")

        # Convert expiration timestamp to datetime
        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp != ""
            else None
        )

        # Add positive entry for recipient (origin = sender, provider = ALEPH, payment_method = credit_transfer)
        csv_rows.append(
            f"{recipient_address};{amount};{message_hash};{index};{message_timestamp};{last_update};;;;{expiration_date or ''};;;{sender_address};ALEPH;;credit_transfer"
        )
        index += 1

        # Add negative entry for sender (unless sender is in whitelisted addresses)
        # (origin = recipient, provider = ALEPH, payment_method = credit_transfer)
        if sender_address not in whitelisted_addresses:
            csv_rows.append(
                f"{sender_address};{-amount};{message_hash};{index};{message_timestamp};{last_update};;;;;;;{recipient_address};ALEPH;;credit_transfer"
            )
            index += 1

    _bulk_insert_credit_history(session, csv_rows)


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


def get_address_credit_history(
    session: DbSession,
    address: str,
    page: int = 1,
    pagination: int = 0,
    tx_hash: Optional[str] = None,
    token: Optional[str] = None,
    chain: Optional[str] = None,
    provider: Optional[str] = None,
    origin: Optional[str] = None,
    origin_ref: Optional[str] = None,
    payment_method: Optional[str] = None,
) -> Sequence[AlephCreditHistoryDb]:
    """
    Get paginated credit history entries for a specific address, ordered from newest to oldest.

    Args:
        session: Database session
        address: Address to get credit history for
        page: Page number (starts at 1)
        pagination: Number of entries per page (0 for all entries)
        tx_hash: Filter by transaction hash
        token: Filter by token
        chain: Filter by chain
        provider: Filter by provider
        origin: Filter by origin
        origin_ref: Filter by origin reference
        payment_method: Filter by payment method

    Returns:
        List of credit history entries ordered by message_timestamp desc
    """
    query = (
        select(AlephCreditHistoryDb)
        .where(AlephCreditHistoryDb.address == address)
        .order_by(AlephCreditHistoryDb.message_timestamp.desc())
    )

    # Apply filters
    if tx_hash is not None:
        query = query.where(AlephCreditHistoryDb.tx_hash == tx_hash)
    if token is not None:
        query = query.where(AlephCreditHistoryDb.token == token)
    if chain is not None:
        query = query.where(AlephCreditHistoryDb.chain == chain)
    if provider is not None:
        query = query.where(AlephCreditHistoryDb.provider == provider)
    if origin is not None:
        query = query.where(AlephCreditHistoryDb.origin == origin)
    if origin_ref is not None:
        query = query.where(AlephCreditHistoryDb.origin_ref == origin_ref)
    if payment_method is not None:
        query = query.where(AlephCreditHistoryDb.payment_method == payment_method)

    if pagination > 0:
        query = query.offset((page - 1) * pagination).limit(pagination)

    return session.execute(query).scalars().all()


def count_address_credit_history(
    session: DbSession,
    address: str,
    tx_hash: Optional[str] = None,
    token: Optional[str] = None,
    chain: Optional[str] = None,
    provider: Optional[str] = None,
    origin: Optional[str] = None,
    origin_ref: Optional[str] = None,
    payment_method: Optional[str] = None,
) -> int:
    """
    Count total credit history entries for a specific address with optional filters.

    Args:
        session: Database session
        address: Address to count credit history for
        tx_hash: Filter by transaction hash
        token: Filter by token
        chain: Filter by chain
        provider: Filter by provider
        origin: Filter by origin
        origin_ref: Filter by origin reference
        payment_method: Filter by payment method

    Returns:
        Total number of credit history entries for the address matching the filters
    """
    query = select(func.count(AlephCreditHistoryDb.credit_ref)).where(
        AlephCreditHistoryDb.address == address
    )

    # Apply filters
    if tx_hash is not None:
        query = query.where(AlephCreditHistoryDb.tx_hash == tx_hash)
    if token is not None:
        query = query.where(AlephCreditHistoryDb.token == token)
    if chain is not None:
        query = query.where(AlephCreditHistoryDb.chain == chain)
    if provider is not None:
        query = query.where(AlephCreditHistoryDb.provider == provider)
    if origin is not None:
        query = query.where(AlephCreditHistoryDb.origin == origin)
    if origin_ref is not None:
        query = query.where(AlephCreditHistoryDb.origin_ref == origin_ref)
    if payment_method is not None:
        query = query.where(AlephCreditHistoryDb.payment_method == payment_method)

    return session.execute(query).scalar_one()


def get_resource_consumed_credits(
    session: DbSession,
    item_hash: str,
) -> int:
    """
    Calculate the total credits consumed by a specific resource.

    Aggregates all credit_history entries where:
    - payment_method = 'credit_expense'
    - origin = item_hash (the resource identifier)

    Args:
        session: Database session
        item_hash: The item hash of the resource (message hash)

    Returns:
        Total credits consumed by the resource
    """
    query = select(func.sum(func.abs(AlephCreditHistoryDb.amount))).where(
        (AlephCreditHistoryDb.payment_method == "credit_expense")
        & (AlephCreditHistoryDb.origin == item_hash)
    )

    result = session.execute(query).scalar()
    return result or 0
