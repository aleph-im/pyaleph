"""Remove the 2026-08-05 double-charged credit expenses

Revision ID: b3d7f1a9c4e2
Revises: a7c3e9f2d5b1
Create Date: 2026-08-06

On 2026-08-05 17:00-17:32 UTC an expense-repricing run republished ~53 days
of already-billed hourly aleph_credit_expense posts as NEW posts (type
aleph_credit_expense + ref) instead of amends (type "amend"). The credit
ledger is additive per message, so every address in those posts was charged
the full window a second time. The bad posts were later FORGOTTEN, but
forgetting does not reverse credit_history, and the corrected "amend" run
never touches balances (amend is not a credit_balances post type).

A ledger row is bad iff its credit_ref (the source message hash) now points
to a forgotten_messages entry: legitimate hourly expense messages are still
live and can never match. A 16:55-18:00 UTC window guard is added on top so
expense messages forgotten for unrelated reasons in the future can never be
swept in when this migration runs on a node that upgrades late.

The removed rows are kept in credit_history_removed_20260805 (audit trail
and downgrade source), then the credit_balances lot cache is rebuilt for the
affected addresses by replaying credit_history chronologically - a frozen
copy of aleph.repair._rebuild_credit_lots_for_address, inlined so this
migration's behavior cannot drift with the application code.

Idempotent: on re-run, on manually-repaired nodes, and on fresh nodes
(empty DB at migration time -> no-op; a fresh sync never processes the
forgotten messages in the first place).
"""

import logging

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b3d7f1a9c4e2"
down_revision = "a7c3e9f2d5b1"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

BACKUP_TABLE = "credit_history_removed_20260805"

# Ledger writes of the wrong-type run (message_timestamp = message time,
# identical on every node). Observed burst: 17:00:11 -> 17:32:5x UTC.
WINDOW_START = "2026-08-05T16:55:00+00"
WINDOW_END = "2026-08-05T18:00:00+00"

BAD_ROWS_PREDICATE = f"""
    ch.payment_method = 'credit_expense'
    AND ch.message_timestamp >= '{WINDOW_START}'
    AND ch.message_timestamp <  '{WINDOW_END}'
    AND EXISTS (
        SELECT 1 FROM forgotten_messages fm
        WHERE fm.item_hash = ch.credit_ref
    )
"""


def _rebuild_credit_lots_for_address(conn, address: str) -> None:
    """Frozen copy of aleph.repair._rebuild_credit_lots_for_address (as of
    this migration): replay credit_history in emission order and replace the
    address's credit_balances rows with the resulting lot state."""
    conn.execute(
        sa.text("DELETE FROM credit_balances WHERE address = :address"),
        {"address": address},
    )

    records = conn.execute(
        sa.text("""
            SELECT credit_ref, credit_index, amount, expiration_date,
                   message_timestamp
            FROM credit_history
            WHERE address = :address
            ORDER BY message_timestamp ASC, credit_ref ASC, credit_index ASC
            """),
        {"address": address},
    ).all()

    lots: list = []
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
        conn.execute(
            sa.text("""
                INSERT INTO credit_balances
                    (address, credit_ref, credit_index, amount_remaining,
                     expiration_date, message_timestamp)
                VALUES
                    (:address, :credit_ref, :credit_index, :amount_remaining,
                     :expiration_date, :message_timestamp)
                """),
            rows,
        )


def _rebuild_backup_table_addresses(conn) -> None:
    addresses = [
        row.address
        for row in conn.execute(sa.text(f"SELECT DISTINCT address FROM {BACKUP_TABLE}"))
    ]
    logger.info(
        "Rebuilding credit lots for %d address(es) touched by the "
        "2026-08-05 double charge",
        len(addresses),
    )
    for i, address in enumerate(addresses, 1):
        _rebuild_credit_lots_for_address(conn, address)
        if i % 500 == 0:
            logger.info("Rebuilt %d / %d", i, len(addresses))


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Audit copy of every row about to be removed. CREATE IF NOT EXISTS +
    #    anti-join top-up keeps this idempotent, including on nodes where the
    #    manual repair SQL already ran and created the table.
    conn.execute(sa.text(f"""
            CREATE TABLE IF NOT EXISTS {BACKUP_TABLE}
                (LIKE credit_history INCLUDING DEFAULTS INCLUDING INDEXES)
            """))
    conn.execute(sa.text(f"""
            INSERT INTO {BACKUP_TABLE}
            SELECT ch.*
            FROM credit_history ch
            WHERE {BAD_ROWS_PREDICATE}
              AND NOT EXISTS (
                  SELECT 1 FROM {BACKUP_TABLE} b
                  WHERE b.credit_ref = ch.credit_ref
                    AND b.credit_index = ch.credit_index
              )
            """))

    # 2. Remove the double charges.
    deleted = conn.execute(
        sa.text(f"DELETE FROM credit_history ch WHERE {BAD_ROWS_PREDICATE}")
    ).rowcount
    logger.info(
        "Removed %d double-charged credit_history row(s) (backup in %s)",
        deleted,
        BACKUP_TABLE,
    )

    # 3. Rebuild the lot cache for every address the incident touched, so
    #    balances reflect the repaired ledger. Runs even when the delete was
    #    a no-op (manually-repaired node): the replay is idempotent.
    _rebuild_backup_table_addresses(conn)


def downgrade() -> None:
    conn = op.get_bind()

    table_exists = conn.execute(
        sa.text("SELECT to_regclass(:t) IS NOT NULL"), {"t": BACKUP_TABLE}
    ).scalar()
    if not table_exists:
        logger.info("%s missing, nothing to restore", BACKUP_TABLE)
        return

    conn.execute(sa.text(f"""
            INSERT INTO credit_history
            SELECT * FROM {BACKUP_TABLE}
            ON CONFLICT (credit_ref, credit_index) DO NOTHING
            """))
    _rebuild_backup_table_addresses(conn)
    # The backup table is deliberately kept: it is the incident audit trail.
