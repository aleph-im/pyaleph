"""Denormalize message and post tags into TEXT[] columns

Revision ID: 7e5a630e4b36
Revises: b3c4d5e6f7a8
Create Date: 2026-05-05

Promotes the tag list out of JSONB into native TEXT[] columns on
``messages`` and ``posts`` so that filtering uses a single GIN index
that covers every message type and matches the per-type validation
introduced in aleph-message 1.2.0.dev0.

Tags currently live in three different places depending on the
content type:

* POST + AGGREGATE: ``content -> 'content' -> 'tags'``
* STORE:            ``content -> 'tags'``
* INSTANCE/PROGRAM: ``content -> 'metadata' -> 'tags'``

The previous ``ix_messages_content_tags_gin`` indexed only the POST
path, so tag queries fell back to a full heap scan for every other
type. The new column unifies the three locations under one indexed
predicate.

Backfill is batched with intermediate COMMITs to bound WAL growth
and lock duration on large tables. Indexes are built CONCURRENTLY
on a separate AUTOCOMMIT connection.
"""

import logging

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "7e5a630e4b36"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

BATCH_SIZE = 50000


def _batched_update(connection, label: str, sql: str) -> None:
    """Run an UPDATE in batches, committing between each."""
    total = 0
    while True:
        connection.execute(text("BEGIN"))
        result = connection.execute(text(sql))
        rows = result.rowcount
        total += rows
        connection.execute(text("COMMIT"))
        logger.info(f"  {label}: {total} rows so far (+{rows})")
        if rows < BATCH_SIZE:
            break
    logger.info(f"  {label}: done ({total} rows total)")


# Backfill SQL: each rebuilds the new column from the legacy JSONB
# location for the relevant content types. ``jsonb_array_length > 0``
# keeps empty tag lists out of the column so an absent value and an
# empty array stay distinguishable.

BACKFILL_MESSAGES_SQL = f"""
WITH batch AS (
    SELECT item_hash FROM messages
    WHERE tags IS NULL
      AND (
        (type IN ('POST', 'AGGREGATE')
            AND jsonb_typeof(content->'content'->'tags') = 'array'
            AND jsonb_array_length(content->'content'->'tags') > 0)
        OR (type = 'STORE'
            AND jsonb_typeof(content->'tags') = 'array'
            AND jsonb_array_length(content->'tags') > 0)
        OR (type IN ('INSTANCE', 'PROGRAM')
            AND jsonb_typeof(content->'metadata'->'tags') = 'array'
            AND jsonb_array_length(content->'metadata'->'tags') > 0)
      )
    LIMIT {BATCH_SIZE}
)
UPDATE messages
SET tags = CASE
    WHEN type IN ('POST', 'AGGREGATE')
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'content'->'tags'))
    WHEN type = 'STORE'
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'tags'))
    WHEN type IN ('INSTANCE', 'PROGRAM')
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'metadata'->'tags'))
END
WHERE item_hash IN (SELECT item_hash FROM batch)
"""

BACKFILL_POSTS_SQL = f"""
WITH batch AS (
    SELECT item_hash FROM posts
    WHERE tags IS NULL
      AND jsonb_typeof(content->'tags') = 'array'
      AND jsonb_array_length(content->'tags') > 0
    LIMIT {BATCH_SIZE}
)
UPDATE posts
SET tags = ARRAY(SELECT jsonb_array_elements_text(content->'tags'))
WHERE item_hash IN (SELECT item_hash FROM batch)
"""


def upgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    # Step 1: schema additions, fast metadata-only ALTERs.
    logger.info("Step 1/3: Adding tags columns...")
    connection.execute(text("BEGIN"))
    connection.execute(
        text("ALTER TABLE messages ADD COLUMN IF NOT EXISTS tags TEXT[]")
    )
    connection.execute(text("ALTER TABLE posts ADD COLUMN IF NOT EXISTS tags TEXT[]"))
    connection.execute(text("COMMIT"))

    # Step 2: backfill from legacy JSONB locations.
    logger.info("Step 2/3: Backfilling messages.tags...")
    _batched_update(connection, "messages.tags", BACKFILL_MESSAGES_SQL)

    logger.info("Step 2/3: Backfilling posts.tags...")
    _batched_update(connection, "posts.tags", BACKFILL_POSTS_SQL)

    # Step 3: build GIN indexes CONCURRENTLY, drop the obsolete one.
    logger.info("Step 3/3: Building GIN indexes...")
    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_tags_gin "
                "ON messages USING GIN (tags) WHERE tags IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_posts_tags_gin "
                "ON posts USING GIN (tags) WHERE tags IS NOT NULL"
            )
        )
        conn.execute(
            text("DROP INDEX CONCURRENTLY IF EXISTS ix_messages_content_tags_gin")
        )

    if was_in_transaction:
        connection.execute(text("BEGIN"))


def downgrade() -> None:
    connection = op.get_bind()

    was_in_transaction = connection.in_transaction()
    if was_in_transaction:
        connection.execute(text("COMMIT"))

    # Restore the previous POST-only JSONB GIN index, drop the new ones.
    engine = connection.engine
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("DROP INDEX CONCURRENTLY IF EXISTS ix_messages_tags_gin"))
        conn.execute(text("DROP INDEX CONCURRENTLY IF EXISTS ix_posts_tags_gin"))
        conn.execute(
            text(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_messages_content_tags_gin "
                "ON messages USING GIN ((content['content']['tags'])) "
                "WHERE type = 'POST'"
            )
        )

    connection.execute(text("BEGIN"))
    connection.execute(text("ALTER TABLE messages DROP COLUMN IF EXISTS tags"))
    connection.execute(text("ALTER TABLE posts DROP COLUMN IF EXISTS tags"))
    connection.execute(text("COMMIT"))

    if was_in_transaction:
        connection.execute(text("BEGIN"))
