"""Add composite index credit_history(address, message_timestamp, credit_ref, credit_index)

Revision ID: a1d4e7b2c9f6
Revises: b9c4f1e6a2d7
Create Date: 2026-06-16

The credit-history endpoint filters by address and orders by
(message_timestamp, credit_ref, credit_index) - the default sort. With only
single-column indexes on address and message_timestamp, the planner either
sorts the full per-address set or scans the whole message_timestamp index
filtering by address, costing ~10s on high-volume addresses.

This composite index serves the filter and the full sort key in one index
range scan; the single btree covers both ASC and DESC because all sort keys
share direction. It is a left-prefix superset of the standalone
ix_credit_history_address index, which is therefore dropped here.
"""

import logging

from alembic import op
from sqlalchemy import text

revision = "a1d4e7b2c9f6"
down_revision = "b9c4f1e6a2d7"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "ix_credit_history_address_timestamp "
                "ON credit_history "
                "(address, message_timestamp, credit_ref, credit_index)"
            )
        )
        # Redundant now: superseded by the composite index above.
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_credit_history_address")
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                "ix_credit_history_address ON credit_history (address)"
            )
        )
        conn.execute(
            text(
                "DROP INDEX CONCURRENTLY IF EXISTS "
                "ix_credit_history_address_timestamp"
            )
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))
