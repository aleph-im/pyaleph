import logging

from aleph_message.models import ItemHash
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from aleph.db.accessors.files import upsert_file
from aleph.db.models import AlephCreditBalanceDb, AlephCreditHistoryDb, StoredFileDb
from aleph.storage import StorageService
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


def _rebuild_credit_lots_for_address(session: DbSession, address: str) -> None:
    """Replay ``credit_history`` chronologically and replace this address's
    lot rows with the resulting state.

    Idempotent: clears existing lots for the address first, then rebuilds. Safe
    to interrupt at address granularity (callers commit per-address).

    Replays in emission order ``(message_timestamp, credit_ref, credit_index)
    ASC``, the same ordering the eager writers see. Each positive history row
    becomes a lot; each negative row drains lots in emission order, skipping
    any whose expiration is at or before the negative row's
    ``message_timestamp`` (so an expense from the past does not drain a lot
    that had already expired at that moment).
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

    lots: list[dict] = []
    for record in records:
        if record.amount > 0:
            lots.append(
                {
                    "credit_ref": record.credit_ref,
                    "credit_index": record.credit_index,
                    "amount_remaining": int(record.amount),
                    "expiration_date": record.expiration_date,
                    "message_timestamp": record.message_timestamp,
                }
            )
        else:
            remaining = -int(record.amount)
            for lot in lots:
                if remaining <= 0:
                    break
                if lot["amount_remaining"] <= 0:
                    continue
                if (
                    lot["expiration_date"] is not None
                    and lot["expiration_date"] <= record.message_timestamp
                ):
                    continue
                take = min(lot["amount_remaining"], remaining)
                lot["amount_remaining"] -= take
                remaining -= take

    rows = [{"address": address, **lot} for lot in lots if lot["amount_remaining"] > 0]
    if rows:
        session.execute(pg_insert(AlephCreditBalanceDb).values(rows))


def _repair_credit_balances(session_factory: DbSessionFactory) -> None:
    """Bootstrap or repair the credit_balances lot cache from credit_history.

    Rebuilds lots for every address that has any credit_history rows. Idempotent
    and runs on every startup; after the initial bootstrap it is a bounded
    full-table scan plus per-address rebuild, which is acceptable given typical
    address counts.
    """
    with session_factory() as session:
        addresses = list(
            session.execute(select(AlephCreditHistoryDb.address).distinct()).scalars()
        )

    LOGGER.info("Repairing credit_balances for %d address(es)", len(addresses))

    for i, address in enumerate(addresses):
        with session_factory() as session:
            _rebuild_credit_lots_for_address(session, address)
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
