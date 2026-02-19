"""Rebuild GIN tags index using subscript syntax

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-02-20

The GIN index created in 0049 uses the arrow operator (content->'content'->'tags')
while SQLAlchemy generates queries using the subscript syntax (content['content']['tags']).
PostgreSQL treats these as different expressions for index matching, so the planner
never pushes the ?| condition into the GIN index — it falls back to a full heap filter
on every POST message.

This is a known PostgreSQL limitation: subscript syntax (introduced in PG 14) and
the arrow operator are semantically equivalent but not interchangeable in expression
indexes. See: https://www.postgresql.org/docs/current/datatype-json.html#JSONB-SUBSCRIPTING

Rebuilding the index with subscript syntax makes it match the queries SQLAlchemy emits.
"""

import logging

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    connection = op.get_bind()
    engine = connection.engine

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    # Drop the old arrow-operator index
    logger.info("Dropping ix_messages_content_tags_gin (arrow-operator syntax)...")
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_messages_content_tags_gin")
        )

    # Recreate with subscript syntax to match SQLAlchemy-generated queries
    logger.info("Rebuilding ix_messages_content_tags_gin (subscript syntax)...")
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_tags_gin "
                "ON messages USING GIN ((content['content']['tags'])) "
                "WHERE type = 'POST'"
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

    # Restore arrow-operator syntax
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_messages_content_tags_gin")
        )

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_tags_gin "
                "ON messages USING GIN ((content->'content'->'tags')) "
                "WHERE type = 'POST'"
            )
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))
