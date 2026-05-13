"""Add composite partial index posts(owner, type, channel) WHERE amends IS NULL

Revision ID: 4f1e8d2a6c3b
Revises: a8c3d9f1b2e4
Create Date: 2026-05-13

Both /posts/ endpoints filter posts by (owner, type, channel) inside a
subquery gated on ``amends IS NULL``. The planner currently has to
``BitmapAnd`` ``ix_posts_owner`` and ``ix_posts_type``, then recheck the
channel and amends predicates from the heap, which on high-volume
owners scans many thousands of unrelated rows.

This partial composite index covers the three equality predicates in
one lookup and excludes amend rows entirely, since the query throws
them away.
"""

import logging

from alembic import op
from sqlalchemy import text

revision = "4f1e8d2a6c3b"
down_revision = "a8c3d9f1b2e4"
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
                "ix_posts_owner_type_channel "
                "ON posts (owner, type, channel) "
                "WHERE amends IS NULL"
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
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_posts_owner_type_channel")
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))
