import datetime as dt
import logging
from typing import Any, Dict, Set

from aleph_message.models import ItemHash, MessageType
from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import aliased

from aleph.db.accessors.files import upsert_file
from aleph.db.accessors.messages import (
    make_message_status_upsert_query,
    make_upsert_rejected_message_statement,
)
from aleph.db.accessors.vms import delete_vm, delete_vm_updates
from aleph.db.models import (
    AlephCreditBalanceDb,
    AlephCreditHistoryDb,
    MessageDb,
    MessageStatusDb,
    StoredFileDb,
)
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import ErrorCode, MessageStatus

LOGGER = logging.getLogger(__name__)


def _wire_message_dict(message: MessageDb) -> Dict[str, Any]:
    """Snapshot a ``MessageDb`` row as a JSON-serializable wire-format dict
    suitable for the ``rejected_messages.message`` JSONB column."""
    data = message.to_dict(exclude=set(MessageDb.DENORMALIZED_COLUMNS))

    if data.get("time") is not None:
        data["time"] = data["time"].timestamp()

    for key in ("chain", "type", "item_type"):
        value = data.get(key)
        if value is not None and hasattr(value, "value"):
            data[key] = value.value

    return data


def mark_processed_message_as_rejected(
    session: DbSession,
    message: MessageDb,
    error_code: ErrorCode,
    reason: str,
) -> None:
    """Transition a processed message into the REJECTED state.

    Mirrors ``mark_pending_message_as_rejected`` for messages that already
    cleared the pipeline under permissive rules but are no longer valid under
    current ones (ex: ExecutableContent.metadata used to accept lists, now
    requires a dict). Cleans up type-specific state (vms rows for the
    executable types), snapshots the row into ``rejected_messages``, flips
    ``message_status`` to REJECTED, and deletes the ``messages`` row. The
    trigger keeps ``message_counts`` consistent; FK cascades clean
    ``message_confirmations`` and ``account_costs``.

    Does not commit. Caller is responsible for state checks (in particular,
    that ``message.status_value == MessageStatus.PROCESSED``).
    """
    snapshot = _wire_message_dict(message)

    # The vms rows have no FK to messages: without an explicit delete_vm, a
    # rejected executable would leave an orphaned vms row that keeps blocking
    # forgets of the files it references (get_vms_dependent_volumes).
    if message.type in (
        MessageType.program,
        MessageType.instance,
        MessageType.v_program,
    ):
        delete_vm(session=session, vm_hash=message.item_hash)
        _ = list(delete_vm_updates(session=session, vm_hash=message.item_hash))

    session.execute(
        make_upsert_rejected_message_statement(
            item_hash=message.item_hash,
            pending_message_dict=snapshot,
            error_code=int(error_code),
            details={"errors": [reason]},
            exc_traceback=reason,
            tx_hash=None,
        )
    )

    session.execute(
        make_message_status_upsert_query(
            item_hash=message.item_hash,
            new_status=MessageStatus.REJECTED,
            reception_time=utc_now(),
            where=MessageStatusDb.status != MessageStatus.REJECTED,
        )
    )

    session.execute(delete(MessageDb).where(MessageDb.item_hash == message.item_hash))


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


def _find_structural_violations(session: DbSession) -> Set[str]:
    """Addresses whose cache rows are structurally inconsistent with their
    granting ``credit_history`` row.

    Flags: orphan lots (no history row for the PK), lots pointing at another
    address's or a negative (drain) history row, negative remainders,
    remainders above the granted amount, and mismatched expiration or
    timestamp. Zero-amount grants are legitimate (zero-amount transfer
    fallback rows), hence ``amount < 0`` rather than ``<= 0``.

    Pure read; over-flagging is safe (worst case an unneeded rebuild).
    """
    grant = aliased(AlephCreditHistoryDb)
    stmt = (
        select(AlephCreditBalanceDb.address)
        .outerjoin(
            grant,
            (grant.credit_ref == AlephCreditBalanceDb.credit_ref)
            & (grant.credit_index == AlephCreditBalanceDb.credit_index),
        )
        .where(
            grant.credit_ref.is_(None)
            | (grant.address != AlephCreditBalanceDb.address)
            | (grant.amount < 0)
            | (AlephCreditBalanceDb.amount_remaining < 0)
            | (AlephCreditBalanceDb.amount_remaining > grant.amount)
            | AlephCreditBalanceDb.expiration_date.is_distinct_from(
                grant.expiration_date
            )
            | AlephCreditBalanceDb.message_timestamp.is_distinct_from(
                grant.message_timestamp
            )
        )
        .distinct()
    )
    return set(session.execute(stmt).scalars())


# Per-address totals check. SUM(cache) == SUM(ledger) holds exactly whenever
# every drain fully landed. A drain leaves leftover (cache > ledger) only if
# valid funds were insufficient at its instant, which requires an expiring
# grant (expiry bounce) or a negative running ledger sum (over-draw, silently
# dropped by both the eager writer and the replay). So: cache < ledger is
# always a bug; strict equality is additionally required for addresses with
# no drains, or with no expiring grants and a never-negative running sum
# (computed in replay order). Over-approximating "bounce possible" only
# weakens the check for that address — never a false flag.
_CONSERVATION_SCREEN_SQL = text(
    """
    WITH ledger AS (
        SELECT address,
               SUM(amount) AS ledger_sum,
               bool_or(amount < 0) AS has_drain,
               bool_or(amount > 0 AND expiration_date IS NOT NULL)
                   AS has_expiring_grant,
               MIN(running_sum) AS min_running_sum
        FROM (
            SELECT address, amount, expiration_date,
                   SUM(amount) OVER (
                       PARTITION BY address
                       ORDER BY message_timestamp, credit_ref, credit_index
                   ) AS running_sum
            FROM credit_history
        ) ordered
        GROUP BY address
    ),
    cache AS (
        SELECT address, SUM(amount_remaining) AS cache_sum
        FROM credit_balances
        GROUP BY address
    )
    SELECT ledger.address
    FROM ledger
    LEFT JOIN cache USING (address)
    WHERE COALESCE(cache.cache_sum, 0) < ledger.ledger_sum
       OR (
            (NOT ledger.has_drain
             OR (NOT ledger.has_expiring_grant AND ledger.min_running_sum >= 0))
            AND COALESCE(cache.cache_sum, 0) <> ledger.ledger_sum
          )
    """
)


def _find_conservation_violations(session: DbSession) -> Set[str]:
    """Addresses whose cache total is impossible given their ledger total.

    See the SQL comment for the exact rule. Pure read; over-flagging is safe.
    """
    return set(session.execute(_CONSERVATION_SCREEN_SQL).scalars())


# The eager writers apply rows in arrival order; the replay applies them in
# (message_timestamp, credit_ref, credit_index) order. The two agree unless a
# row was inserted after a row it replay-precedes AND a drain is involved
# (grant/grant order never changes the outcome; drains pick lots by replay
# order, so a late grant can retroactively change what an already-applied
# drain should have consumed, and vice versa). credit_history.id is never
# populated (not in the bulk-insert column list, no sequence), so insertion
# order is proxied by last_update, set once per writer call. Rows at or below
# the watermark were reconciled by a previous repair run.
_INVERSION_SCREEN_SQL = text(
    """
    SELECT DISTINCT late.address
    FROM credit_history late
    JOIN credit_history early
      ON early.address = late.address
     AND early.last_update < late.last_update
     AND (early.message_timestamp, early.credit_ref, early.credit_index)
         > (late.message_timestamp, late.credit_ref, late.credit_index)
     AND (late.amount < 0 OR early.amount < 0)
    WHERE late.last_update > :watermark
    """
)


def _find_order_inversions(session: DbSession, watermark: dt.datetime) -> Set[str]:
    """Addresses with out-of-order credit rows inserted since ``watermark``.

    Pure read; over-flagging is safe (the rebuild replay is the fixpoint).
    """
    return set(
        session.execute(_INVERSION_SCREEN_SQL, {"watermark": watermark}).scalars()
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


_INVALID_METADATA_REASON = (
    "ExecutableContent.metadata must be a dict; legacy rows with a list value "
    "no longer parse and surfaced as 500s at the API."
)


def _reject_invalid_program_metadata(session_factory: DbSessionFactory) -> None:
    """Reject PROGRAM messages whose ``content.metadata`` is a JSON array.

    aleph-message historically accepted ``ExecutableContent.metadata`` as
    either a dict or a list. The current validator requires a dict, so rows
    accepted under the old rules trip ``parsed_content`` access and surface as
    500s on ``GET /api/v0/messages/<hash>``. Moves them to the rejected state
    so the API can render them the same way nodes that rejected them in the
    first place do.

    Per-message commits so a single bad row does not roll back the rest.
    """
    with session_factory() as session:
        select_stmt = (
            select(MessageDb.item_hash)
            .where(MessageDb.type == MessageType.program)
            .where(MessageDb.status_value == MessageStatus.PROCESSED)
            .where(func.jsonb_typeof(MessageDb.content["metadata"]) == "array")
        )
        item_hashes = list(session.execute(select_stmt).scalars())

    if not item_hashes:
        return

    LOGGER.info(
        "Rejecting %d PROGRAM message(s) with non-dict metadata", len(item_hashes)
    )

    rejected = 0
    for item_hash in item_hashes:
        with session_factory() as session:
            try:
                message = session.execute(
                    select(MessageDb).where(MessageDb.item_hash == item_hash)
                ).scalar_one_or_none()
                if message is None or message.status_value != MessageStatus.PROCESSED:
                    continue
                mark_processed_message_as_rejected(
                    session=session,
                    message=message,
                    error_code=ErrorCode.INVALID_FORMAT,
                    reason=_INVALID_METADATA_REASON,
                )
                session.commit()
                rejected += 1
            except Exception:
                LOGGER.exception("Failed to reject program %s", item_hash)
                session.rollback()

    LOGGER.info(
        "Done: rejected %d / %d PROGRAM message(s) with non-dict metadata",
        rejected,
        len(item_hashes),
    )


async def repair_node(
    storage_service: StorageService, session_factory: DbSessionFactory
):
    LOGGER.info("Fixing file sizes")
    with session_factory() as session:
        await _fix_file_sizes(session, storage_service, store_files=True)
        session.commit()

    LOGGER.info("Repairing credit balances")
    _repair_credit_balances(session_factory)

    LOGGER.info("Rejecting PROGRAM messages with invalid metadata")
    _reject_invalid_program_metadata(session_factory)
