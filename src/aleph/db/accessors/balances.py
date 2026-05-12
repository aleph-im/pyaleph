import csv
import datetime as dt
import random
import time
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from aleph_message.models import Chain
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import Select

from aleph.db.models import AlephBalanceDb, AlephCreditBalanceDb, AlephCreditHistoryDb
from aleph.toolkit.constants import (
    CREDIT_PRECISION_CUTOFF_TIMESTAMP,
    CREDIT_PRECISION_MULTIPLIER,
)
from aleph.toolkit.infinity import INFINITY
from aleph.toolkit.timestamp import timestamp_to_datetime, utc_now
from aleph.types.db_session import DbSession
from aleph.types.sort_order import SortByCreditHistory, SortOrder


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
    after_address: Optional[str] = None,
    cursor_mode: bool = False,
) -> Select:
    query = select(AlephBalanceDb.address, AlephBalanceDb.balance, AlephBalanceDb.chain)

    if chains:
        query = query.where(AlephBalanceDb.chain.in_(chains))

    if min_balance > 0:
        query = query.filter(AlephBalanceDb.balance >= min_balance)

    query = query.order_by(AlephBalanceDb.address.asc())

    if after_address is not None:
        query = query.where(AlephBalanceDb.address > after_address)

    if after_address is not None or cursor_mode:
        if pagination:
            query = query.limit(pagination + 1)
    else:
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
class CreditBalanceDetail:
    expiration_date: Optional[dt.datetime]
    amount: int


def _bucket_expiration(expiration_date: Optional[dt.datetime]) -> dt.datetime:
    """Map an optional API-level expiration to the DB sentinel for the bucket PK."""
    return expiration_date if expiration_date is not None else INFINITY


def _api_expiration(expiration_date: dt.datetime) -> Optional[dt.datetime]:
    """Inverse of _bucket_expiration: convert the sentinel back to None for callers."""
    return None if expiration_date == INFINITY else expiration_date


def _apply_grant_bucket(
    session: DbSession,
    address: str,
    amount: int,
    expiration_date: Optional[dt.datetime],
) -> None:
    """Add ``amount`` (positive) to the bucket for ``(address, expiration_date)``.

    Inserts the bucket if missing, increments it otherwise. ``amount`` may be
    negative to undo a previous grant, but consumption of expenses/transfers
    should go through ``_consume_address_credits`` so the soonest-expiring
    bucket is drained first.
    """
    bucket_exp = _bucket_expiration(expiration_date)
    stmt = pg_insert(AlephCreditBalanceDb).values(
        address=address,
        expiration_date=bucket_exp,
        amount=amount,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            AlephCreditBalanceDb.address,
            AlephCreditBalanceDb.expiration_date,
        ],
        set_={
            "amount": AlephCreditBalanceDb.amount + stmt.excluded.amount,
            "last_update": func.now(),
        },
    )
    session.execute(stmt)


def _consume_address_credits(
    session: DbSession,
    address: str,
    amount: int,
    now: Optional[dt.datetime] = None,
) -> List[Tuple[int, dt.datetime]]:
    """Drain ``amount`` credits from the address's still-valid buckets, soonest
    expiry first. Returns a list of ``(consumed_amount, source_bucket_expiration)``
    in consumption order. The bucket expiration is the DB-stored value (with
    sentinel for "no expiration"); callers wanting the API-shaped value should
    pass it through ``_api_expiration``.

    ``now`` is the validity cutoff: only buckets whose ``expiration_date`` is
    strictly after ``now`` are eligible to be drained. Production callers pass
    the expense / transfer ``message_timestamp`` here so a bucket that was
    valid at the moment the message was signed remains spendable even if it
    has since expired in wall-clock terms. This also keeps the eager-write
    path consistent with the repair replay, which uses the historical
    ``message_timestamp`` as the cutoff. ``now`` defaults to ``func.now()``
    (evaluated server-side) for callers that genuinely mean "right now".

    Bucket rows are locked ``FOR UPDATE`` to serialise concurrent writers for
    the same address. Buckets are not deleted when they reach zero; the
    expiration task and the next read both treat zero-amount buckets as inert.

    If ``amount`` exceeds the available credit, the leftover is silently
    dropped (matches the prior FIFO behaviour). Going negative is prevented.
    """
    if amount <= 0:
        return []

    cutoff = now if now is not None else func.now()
    buckets = (
        session.execute(
            select(AlephCreditBalanceDb)
            .where(
                AlephCreditBalanceDb.address == address,
                AlephCreditBalanceDb.expiration_date > cutoff,
                AlephCreditBalanceDb.amount > 0,
            )
            .order_by(AlephCreditBalanceDb.expiration_date.asc())
            .with_for_update()
        )
        .scalars()
        .all()
    )

    consumed_log: List[Tuple[int, dt.datetime]] = []
    remaining = amount
    for bucket in buckets:
        if remaining <= 0:
            break
        take = min(bucket.amount, remaining)
        bucket.amount -= take
        remaining -= take
        consumed_log.append((take, bucket.expiration_date))
    session.flush()
    return consumed_log


def _compute_transfer_entries_by_expiration(
    consumed_buckets: List[Tuple[int, dt.datetime]],
    requested_expiration: Optional[dt.datetime],
) -> List[Tuple[int, Optional[dt.datetime]]]:
    """For each consumed source bucket, produce the matching recipient entry,
    capping the recipient's expiration at the more-restrictive of source vs.
    requested. Adjacent entries with the same effective expiration are merged
    so a recipient never sees more granularity than necessary.

    The cap rule (``min(source, requested)``) prevents an attacker re-transferring
    expiring credits with a longer requested expiration to bypass the original
    expiry. Whitelisted senders skip this path entirely (see caller).
    """
    result: List[Tuple[int, Optional[dt.datetime]]] = []
    for consumed, source_exp in consumed_buckets:
        api_source_exp = _api_expiration(source_exp)

        if api_source_exp is None:
            effective_exp: Optional[dt.datetime] = requested_expiration
        elif requested_expiration is None:
            effective_exp = api_source_exp
        else:
            effective_exp = min(api_source_exp, requested_expiration)

        if result and result[-1][1] == effective_exp:
            result[-1] = (result[-1][0] + consumed, effective_exp)
        else:
            result.append((consumed, effective_exp))
    return result


def _credit_balance_amount_expr():
    """Reusable SQL expression: per-address sum of non-expired bucket amounts.

    Uses ``func.now()`` so the cutoff is evaluated server-side at statement
    execution time rather than at expression-construction time in Python.
    """
    return func.coalesce(
        func.sum(AlephCreditBalanceDb.amount).filter(
            AlephCreditBalanceDb.expiration_date > func.now()
        ),
        0,
    )


def get_credit_balance(
    session: DbSession, address: str, now: Optional[dt.datetime] = None
) -> int:
    """Sum of non-expired bucket amounts for ``address``, floored at zero.

    Pure read: no FIFO walk, no write-back. The bucket cache is maintained by
    the credit_history writers and by the expiration task.
    """
    cutoff = now if now is not None else func.now()
    result = session.execute(
        select(func.coalesce(func.sum(AlephCreditBalanceDb.amount), 0)).where(
            AlephCreditBalanceDb.address == address,
            AlephCreditBalanceDb.expiration_date > cutoff,
        )
    ).scalar()
    return max(0, int(result or 0))


def get_credit_balance_with_details(
    session: DbSession, address: str, now: Optional[dt.datetime] = None
) -> Tuple[int, List[CreditBalanceDetail]]:
    """Per-expiration-date breakdown of an address's non-expired credits.

    Returns ``(total, details)`` where details are sorted with the
    non-expiring bucket first (sentinel mapped back to ``None``), then by
    expiration ascending. Zero-amount buckets are filtered out.
    """
    cutoff = now if now is not None else func.now()
    rows = session.execute(
        select(AlephCreditBalanceDb.expiration_date, AlephCreditBalanceDb.amount).where(
            AlephCreditBalanceDb.address == address,
            AlephCreditBalanceDb.expiration_date > cutoff,
            AlephCreditBalanceDb.amount > 0,
        )
    ).all()

    pairs = [(row.expiration_date, int(row.amount)) for row in rows]
    total = max(0, sum(amount for _, amount in pairs))

    details = [
        CreditBalanceDetail(expiration_date=_api_expiration(exp), amount=amount)
        for exp, amount in sorted(
            pairs,
            # Sentinel (no expiration) first, then by expiration ascending.
            key=lambda x: (x[0] != INFINITY, x[0]),
        )
    ]
    return total, details


def get_credit_balances(
    session: DbSession,
    page: int = 1,
    pagination: int = 100,
    min_balance: int = 0,
    after_address: Optional[str] = None,
    cursor_mode: bool = False,
) -> list[tuple[str, int]]:
    """Paginated list of (address, balance) across all addresses with a
    positive non-expired sum.
    """
    balance_expr = _credit_balance_amount_expr().label("balance")

    query = (
        select(AlephCreditBalanceDb.address, balance_expr)
        .group_by(AlephCreditBalanceDb.address)
        .having(balance_expr >= max(min_balance, 1))
        .order_by(AlephCreditBalanceDb.address.asc())
    )

    if after_address is not None:
        query = query.where(AlephCreditBalanceDb.address > after_address)

    if after_address is not None or cursor_mode:
        if pagination:
            query = query.limit(pagination + 1)
    else:
        query = query.offset((page - 1) * pagination)
        if pagination:
            query = query.limit(pagination)

    return [(row.address, int(row.balance)) for row in session.execute(query).all()]


def count_credit_balances(session: DbSession, min_balance: int = 0) -> int:
    """Count of addresses with a positive non-expired sum (or matching
    ``min_balance``)."""
    balance_expr = _credit_balance_amount_expr().label("balance")
    sub = (
        select(AlephCreditBalanceDb.address)
        .group_by(AlephCreditBalanceDb.address)
        .having(balance_expr >= max(min_balance, 1))
        .subquery()
    )
    return session.execute(select(func.count()).select_from(sub)).scalar_one()


def _format_csv_row(*fields) -> str:
    """Format fields as a properly escaped CSV row with semicolon delimiter."""
    output = StringIO()
    writer = csv.writer(
        output, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
    )
    writer.writerow([str(f) if f is not None else "" for f in fields])
    return output.getvalue().rstrip("\n")


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
    """Apply a distribution message: grant each entry's bucket and append the
    matching credit_history rows.
    """

    last_update = utc_now()
    csv_rows = []

    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        raw_amount = int(credit_entry["amount"])
        amount = _apply_credit_precision_multiplier(raw_amount, message_timestamp)
        price = Decimal(credit_entry["price"])
        tx_hash = credit_entry["tx_hash"]
        provider = credit_entry["provider"]

        expiration_timestamp = credit_entry.get("expiration") or None
        origin = credit_entry.get("origin", "")
        origin_ref = credit_entry.get("ref", "")
        payment_method = credit_entry.get("payment_method", "")
        bonus_amount = credit_entry.get("bonus_amount", "")

        expiration_date = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp is not None
            else None
        )

        _apply_grant_bucket(session, address, amount, expiration_date)

        csv_rows.append(
            _format_csv_row(
                address,
                amount,
                message_hash,
                index,
                message_timestamp,
                last_update,
                price,
                bonus_amount or "",
                tx_hash,
                expiration_date or "",
                token,
                chain,
                origin,
                provider,
                origin_ref,
                payment_method,
            )
        )

    _bulk_insert_credit_history(session, csv_rows)


def update_credit_balances_expense(
    session: DbSession,
    credits_list: Sequence[Dict[str, Any]],
    message_hash: str,
    message_timestamp: dt.datetime,
) -> None:
    """Apply an expense message: drain each entry's amount from the soonest-
    expiring non-expired buckets, then append the matching credit_history rows.

    The history row's negative ``amount`` reflects the message intent. If the
    address is under-funded, fewer credits are actually consumed (buckets cannot
    go negative), matching the prior FIFO behaviour.
    """

    last_update = utc_now()
    csv_rows = []

    for index, credit_entry in enumerate(credits_list):
        address = credit_entry["address"]
        raw_amount = int(credit_entry["amount"])
        amount = _apply_credit_precision_multiplier(raw_amount, message_timestamp)
        origin_ref = credit_entry.get("ref", "")
        origin = credit_entry.get("execution_id", "")
        tx_hash = credit_entry.get("node_id", "")
        price = credit_entry.get("price", "")

        _consume_address_credits(session, address, amount, now=message_timestamp)

        csv_rows.append(
            _format_csv_row(
                address,
                -amount,
                message_hash,
                index,
                message_timestamp,
                last_update,
                price,
                "",
                tx_hash,
                "",
                "",
                "",
                origin,
                "ALEPH",
                origin_ref,
                "credit_expense",
            )
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
    """Apply a transfer message: drain the sender's buckets (soonest-expiring
    first), grant the resulting amounts to the recipient(s) with each portion
    capped at ``min(source_expiration, requested_expiration)``, and append the
    matching credit_history rows.

    Whitelisted senders create credits from nothing: the sender is not debited
    and the recipient is granted ``amount`` with the requested expiration as-is.
    """

    last_update = utc_now()
    csv_rows = []
    index = 0
    is_whitelisted = sender_address in whitelisted_addresses

    for credit_entry in credits_list:
        recipient_address = credit_entry["address"]
        raw_amount = int(credit_entry["amount"])
        amount = _apply_credit_precision_multiplier(raw_amount, message_timestamp)
        expiration_timestamp = credit_entry.get("expiration") or None

        requested_expiration = (
            dt.datetime.fromtimestamp(expiration_timestamp / 1000, tz=dt.timezone.utc)
            if expiration_timestamp is not None
            else None
        )

        if is_whitelisted:
            entries: List[Tuple[int, Optional[dt.datetime]]] = [
                (amount, requested_expiration)
            ]
        else:
            consumed = _consume_address_credits(
                session, sender_address, amount, now=message_timestamp
            )
            entries = _compute_transfer_entries_by_expiration(
                consumed, requested_expiration
            )
            # Production transfers are gated by validate_credit_transfer_balance,
            # so consumed should sum to ``amount``. Fall back to a single
            # ``(amount, requested_expiration)`` entry whenever it doesn't (under-
            # funded test scenarios or zero-amount transfers) so the recipient
            # still receives a history row matching the message intent. The
            # bucket grant below is a no-op for zero-amount entries.
            if not entries:
                entries = [(amount, requested_expiration)]

        for entry_amount, entry_expiration in entries:
            _apply_grant_bucket(
                session, recipient_address, entry_amount, entry_expiration
            )
            csv_rows.append(
                _format_csv_row(
                    recipient_address,
                    entry_amount,
                    message_hash,
                    index,
                    message_timestamp,
                    last_update,
                    "",
                    "",
                    "",
                    entry_expiration or "",
                    "",
                    "",
                    sender_address,
                    "ALEPH",
                    "",
                    "credit_transfer",
                )
            )
            index += 1

        if not is_whitelisted:
            csv_rows.append(
                _format_csv_row(
                    sender_address,
                    -amount,
                    message_hash,
                    index,
                    message_timestamp,
                    last_update,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    recipient_address,
                    "ALEPH",
                    "",
                    "credit_transfer",
                )
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


CREDIT_HISTORY_SORT_COLUMN_MAP = {
    SortByCreditHistory.MESSAGE_TIMESTAMP: AlephCreditHistoryDb.message_timestamp,
    SortByCreditHistory.EXPIRATION_DATE: AlephCreditHistoryDb.expiration_date,
    SortByCreditHistory.PAYMENT_METHOD: AlephCreditHistoryDb.payment_method,
    SortByCreditHistory.AMOUNT: AlephCreditHistoryDb.amount,
    SortByCreditHistory.ORIGIN: AlephCreditHistoryDb.origin,
    SortByCreditHistory.TX_HASH: AlephCreditHistoryDb.tx_hash,
    SortByCreditHistory.PROVIDER: AlephCreditHistoryDb.provider,
}

# Columns that are nullable and need NULLS LAST handling
_NULLABLE_SORT_COLUMNS = {
    SortByCreditHistory.EXPIRATION_DATE,
    SortByCreditHistory.ORIGIN,
    SortByCreditHistory.TX_HASH,
    SortByCreditHistory.PROVIDER,
}


def _apply_credit_history_filters(
    query: Select,
    tx_hash: Optional[str] = None,
    token: Optional[str] = None,
    chain: Optional[str] = None,
    provider: Optional[str] = None,
    origin: Optional[str] = None,
    origin_ref: Optional[str] = None,
    payment_method: Optional[str] = None,
    has_expiration: Optional[bool] = None,
    exclude_payment_method: Optional[List[str]] = None,
) -> Select:
    """Apply common filters to a credit history query."""
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
    if has_expiration is True:
        query = query.where(AlephCreditHistoryDb.expiration_date.isnot(None))
    elif has_expiration is False:
        query = query.where(AlephCreditHistoryDb.expiration_date.is_(None))
    if exclude_payment_method:
        query = query.where(
            AlephCreditHistoryDb.payment_method.notin_(exclude_payment_method)
        )
    return query


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
    has_expiration: Optional[bool] = None,
    exclude_payment_method: Optional[List[str]] = None,
    sort_by: SortByCreditHistory = SortByCreditHistory.MESSAGE_TIMESTAMP,
    sort_order: SortOrder = SortOrder.DESCENDING,
    after_sort_value: Optional[Any] = None,
    after_credit_ref: Optional[str] = None,
    after_credit_index: Optional[int] = None,
    cursor_mode: bool = False,
) -> Sequence[AlephCreditHistoryDb]:
    """
    Get paginated credit history entries for a specific address.

    Supports dynamic sorting and cursor-based or page-based pagination.
    """
    query = select(AlephCreditHistoryDb).where(AlephCreditHistoryDb.address == address)

    # Dynamic ordering
    primary_col = CREDIT_HISTORY_SORT_COLUMN_MAP[sort_by]
    is_desc = sort_order == SortOrder.DESCENDING

    if sort_by in _NULLABLE_SORT_COLUMNS:
        if is_desc:
            order_primary = primary_col.desc().nullslast()
        else:
            order_primary = primary_col.asc().nullslast()
    else:
        order_primary = primary_col.desc() if is_desc else primary_col.asc()

    # Tiebreakers for stable pagination
    if is_desc:
        query = query.order_by(
            order_primary,
            AlephCreditHistoryDb.credit_ref.desc(),
            AlephCreditHistoryDb.credit_index.desc(),
        )
    else:
        query = query.order_by(
            order_primary,
            AlephCreditHistoryDb.credit_ref.asc(),
            AlephCreditHistoryDb.credit_index.asc(),
        )

    # Apply filters
    query = _apply_credit_history_filters(
        query,
        tx_hash=tx_hash,
        token=token,
        chain=chain,
        provider=provider,
        origin=origin,
        origin_ref=origin_ref,
        payment_method=payment_method,
        has_expiration=has_expiration,
        exclude_payment_method=exclude_payment_method,
    )

    # Cursor-based keyset pagination
    if after_credit_ref is not None:
        if after_sort_value is None and sort_by in _NULLABLE_SORT_COLUMNS:
            # Last entry had NULL sort value — only compare tiebreakers within NULL group
            if is_desc:
                query = query.where(
                    (primary_col.is_(None))
                    & (
                        (AlephCreditHistoryDb.credit_ref < after_credit_ref)
                        | (
                            (AlephCreditHistoryDb.credit_ref == after_credit_ref)
                            & (AlephCreditHistoryDb.credit_index < after_credit_index)
                        )
                    )
                )
            else:
                query = query.where(
                    (primary_col.is_(None))
                    & (
                        (AlephCreditHistoryDb.credit_ref > after_credit_ref)
                        | (
                            (AlephCreditHistoryDb.credit_ref == after_credit_ref)
                            & (AlephCreditHistoryDb.credit_index > after_credit_index)
                        )
                    )
                )
        elif is_desc:
            query = query.where(
                (primary_col < after_sort_value)
                | (
                    (primary_col == after_sort_value)
                    & (
                        (AlephCreditHistoryDb.credit_ref < after_credit_ref)
                        | (
                            (AlephCreditHistoryDb.credit_ref == after_credit_ref)
                            & (AlephCreditHistoryDb.credit_index < after_credit_index)
                        )
                    )
                )
                # NULLs come after all non-NULLs due to NULLS LAST ordering
                | (primary_col.is_(None))
            )
        else:
            query = query.where(
                (primary_col > after_sort_value)
                | (
                    (primary_col == after_sort_value)
                    & (
                        (AlephCreditHistoryDb.credit_ref > after_credit_ref)
                        | (
                            (AlephCreditHistoryDb.credit_ref == after_credit_ref)
                            & (AlephCreditHistoryDb.credit_index > after_credit_index)
                        )
                    )
                )
                # NULLs come after all non-NULLs due to NULLS LAST ordering
                | (primary_col.is_(None))
            )

    if after_credit_ref is not None or cursor_mode:
        if pagination > 0:
            query = query.limit(pagination + 1)
    elif pagination > 0:
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
    has_expiration: Optional[bool] = None,
    exclude_payment_method: Optional[List[str]] = None,
) -> int:
    """
    Count total credit history entries for a specific address with optional filters.
    """
    query = select(func.count(AlephCreditHistoryDb.credit_ref)).where(
        AlephCreditHistoryDb.address == address
    )

    query = _apply_credit_history_filters(
        query,
        tx_hash=tx_hash,
        token=token,
        chain=chain,
        provider=provider,
        origin=origin,
        origin_ref=origin_ref,
        payment_method=payment_method,
        has_expiration=has_expiration,
        exclude_payment_method=exclude_payment_method,
    )

    return session.execute(query).scalar_one()


def get_total_consumed_credits(
    session: DbSession,
    address: Optional[str] = None,
    item_hash: Optional[str] = None,
) -> int:
    """
    Calculate total credits consumed, optionally filtered by address or item_hash.

    Aggregates all credit_history entries where payment_method = 'credit_expense'.

    Args:
        session: Database session
        address: Optional filter by address
        item_hash: Optional filter by resource (origin)

    Returns:
        Total credits consumed
    """
    query = select(func.sum(func.abs(AlephCreditHistoryDb.amount))).where(
        AlephCreditHistoryDb.payment_method == "credit_expense"
    )

    if address:
        query = query.where(AlephCreditHistoryDb.address == address)
    if item_hash:
        effective_origin = func.coalesce(
            func.nullif(AlephCreditHistoryDb.origin, ""),
            AlephCreditHistoryDb.origin_ref,
        )
        query = query.where(effective_origin == item_hash)

    result = session.execute(query).scalar()
    return result or 0


def get_resource_consumed_credits(
    session: DbSession,
    item_hash: str,
) -> int:
    """
    Calculate the total credits consumed by a specific resource.

    This is a convenience wrapper around get_total_consumed_credits
    for filtering by a single item_hash.

    Args:
        session: Database session
        item_hash: The item hash of the resource (message hash)

    Returns:
        Total credits consumed by the resource
    """
    return get_total_consumed_credits(session=session, item_hash=item_hash)


def get_consumed_credits_by_resource(
    session: DbSession,
    item_hashes: Optional[list[str]] = None,
) -> dict[str, int]:
    """
    Get consumed credits grouped by resource (origin).

    Args:
        session: Database session
        item_hashes: List of item hashes to filter by (required for efficiency)

    Returns:
        Dictionary mapping item_hash to consumed_credits
    """
    if not item_hashes:
        return {}

    effective_origin = func.coalesce(
        func.nullif(AlephCreditHistoryDb.origin, ""),
        AlephCreditHistoryDb.origin_ref,
    )

    query = (
        select(
            effective_origin.label("resource_hash"),
            func.sum(func.abs(AlephCreditHistoryDb.amount)).label("consumed_credits"),
        )
        .where(
            (AlephCreditHistoryDb.payment_method == "credit_expense")
            & (effective_origin.in_(item_hashes))
        )
        .group_by(effective_origin)
    )

    results = session.execute(query).all()
    return {row.resource_hash: row.consumed_credits for row in results}
