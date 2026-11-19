"""Add partial index for unconfirmed message keyset pagination

Revision ID: a7d2e9c4b6f1
Revises: c5e1a9b3d724
Create Date: 2026-11-19

The packer walks unconfirmed messages ordered by reception_time and item_hash.
A partial composite index keeps that cursor query from scanning the full
messages table when the unconfirmed backlog grows.
"""

import logging

from alembic import op
from sqlalchemy import text

revision = "a7d2e9c4b6f1"
down_revision = "c5e1a9b3d724"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

INDEX_NAME = "ix_messages_unconfirmed_reception_time_item_hash"


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
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
                "ON messages (reception_time, item_hash) "
                "WHERE signature IS NOT NULL AND first_confirmed_at IS NULL"
            )
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
        conn.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}"))

    if was_in_transaction:
        connection.execute(text("BEGIN"))
