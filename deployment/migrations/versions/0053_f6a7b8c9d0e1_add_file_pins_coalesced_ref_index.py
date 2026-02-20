"""Add expression index on file_pins COALESCE(ref, item_hash)

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-02-20

The refresh_file_tag function self-joins file_pins on COALESCE(ref, item_hash),
which requires a full table scan with no supporting index. This adds a partial
expression index for message-type pins to allow index lookups on the computed key.
"""

import logging

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    connection = op.get_bind()
    engine = connection.engine

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    logger.info("Building ix_file_pins_coalesced_ref...")
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_file_pins_coalesced_ref "
                "ON file_pins (COALESCE(ref, item_hash)) "
                "WHERE type = 'message'"
            )
        )
    logger.info("Done.")

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    connection = op.get_bind()
    engine = connection.engine

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_file_pins_coalesced_ref")
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))
