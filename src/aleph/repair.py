import logging

from aleph_message.models import ItemHash
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aleph.db.accessors.files import upsert_file
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb, StoredFileDb
from aleph.storage import StorageService
from aleph.toolkit.infinity import INFINITY
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)


async def _fix_file_sizes(
    session: DbSession, storage_service: StorageService, store_files: bool
):
    files_with_negative_size = (
        session.execute(select(StoredFileDb).where(StoredFileDb.size < 0))
        .scalars()
        .all()
    )

    LOGGER.info("Found %d files with negative size", len(files_with_negative_size))

    for file in files_with_negative_size:
        file_hash = ItemHash(file.hash)
        LOGGER.info("Fixing file %s", file_hash)

        try:
            file_content = await storage_service.get_hash_content(
                content_hash=file_hash,
                engine=file_hash.item_type,
                use_network=True,
                use_ipfs=True,
                store_value=store_files,
            )
        except Exception:
            LOGGER.exception("Failed to fetch file %s", file_hash)
            continue

        upsert_file(
            session=session,
            file_hash=file_hash,
            file_type=file.type,
            size=len(file_content),
        )


def _rebuild_credit_buckets_for_address(session: DbSession, address: str) -> None:
    """Replay ``credit_history`` chronologically under the sort-by-expiration
    policy and replace this address's buckets with the resulting state.

    Idempotent: clears existing buckets for the address first, then rebuilds.
    Safe to interrupt at address granularity (callers commit per-address).
    """
    session.execute(
        delete(AlephCreditBalanceDb).where(AlephCreditBalanceDb.address == address)
    )

    records = (
        session.execute(
            select(AlephCreditHistoryDb)
            .where(AlephCreditHistoryDb.address == address)
            .order_by(
                AlephCreditHistoryDb.message_timestamp.asc(),
                AlephCreditHistoryDb.credit_ref.asc(),
                AlephCreditHistoryDb.credit_index.asc(),
            )
        )
        .scalars()
        .all()
    )

    # Bucket state in memory: {expiration_date (sentinel for None): amount}.
    buckets: dict = {}

    def _bucket_key(expiration):
        return expiration if expiration is not None else INFINITY

    for record in records:
        if record.amount > 0:
            key = _bucket_key(record.expiration_date)
            buckets[key] = buckets.get(key, 0) + record.amount
        else:
            remaining = -int(record.amount)
            # Soonest-expiring first, ignoring buckets that have already expired
            # at the moment of the historical expense.
            keys_in_order = sorted(buckets.keys())
            for key in keys_in_order:
                if remaining <= 0:
                    break
                if key <= record.message_timestamp:
                    continue
                available = buckets[key]
                if available <= 0:
                    continue
                take = min(available, remaining)
                buckets[key] = available - take
                remaining -= take

    now = utc_now()
    rows = [
        {"address": address, "expiration_date": key, "amount": amount}
        for key, amount in buckets.items()
        if amount > 0 and key > now
    ]
    if rows:
        session.execute(pg_insert(AlephCreditBalanceDb).values(rows))


def _repair_credit_balances(session_factory: DbSessionFactory) -> None:
    """Bootstrap or repair the credit_balances bucket cache from credit_history.

    Rebuilds buckets for every address that has any credit_history rows. This is
    idempotent and runs on every startup; after the initial bootstrap it's a
    bounded full-table scan plus per-address rebuild, which is acceptable given
    typical address counts.
    """
    with session_factory() as session:
        addresses = list(
            session.execute(select(AlephCreditHistoryDb.address).distinct()).scalars()
        )

    LOGGER.info("Repairing credit_balances for %d address(es)", len(addresses))

    for i, address in enumerate(addresses):
        with session_factory() as session:
            _rebuild_credit_buckets_for_address(session, address)
            session.commit()
        if (i + 1) % 500 == 0:
            LOGGER.info("Repaired %d / %d", i + 1, len(addresses))

    LOGGER.info("Credit balances repair complete (%d address(es))", len(addresses))


async def repair_node(
    storage_service: StorageService, session_factory: DbSessionFactory
):
    LOGGER.info("Fixing file sizes")
    with session_factory() as session:
        await _fix_file_sizes(session, storage_service, store_files=True)
        session.commit()

    LOGGER.info("Repairing credit balances")
    _repair_credit_balances(session_factory)
