"""Create message_counts table and trigger

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
Create Date: 2026-02-18

Creates the message_counts counter table and a PostgreSQL trigger on messages
that automatically maintains counts on INSERT, UPDATE (status change), and
DELETE.
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "d6e7f8a9b0c1"
down_revision = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        CREATE TABLE message_counts (
            type            VARCHAR NOT NULL DEFAULT '',
            status          VARCHAR NOT NULL DEFAULT '',
            sender          VARCHAR NOT NULL DEFAULT '',
            owner           VARCHAR NOT NULL DEFAULT '',
            count           BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (type, status, sender, owner)
        );

        -- Helper: increment the standard set of dimension combos for a message.
        CREATE OR REPLACE FUNCTION _increment_message_counts(
            p_type VARCHAR, p_status VARCHAR, p_sender VARCHAR, p_owner VARCHAR
        ) RETURNS VOID AS $$
        BEGIN
            -- Global: by status only
            INSERT INTO message_counts (status, count)
            VALUES (COALESCE(p_status, ''), 1)
            ON CONFLICT (type, status, sender, owner)
            DO UPDATE SET count = message_counts.count + 1;

            -- By (type, status)
            INSERT INTO message_counts (type, status, count)
            VALUES (COALESCE(p_type, ''), COALESCE(p_status, ''), 1)
            ON CONFLICT (type, status, sender, owner)
            DO UPDATE SET count = message_counts.count + 1;

            -- By (sender, status)
            INSERT INTO message_counts (sender, status, count)
            VALUES (COALESCE(p_sender, ''), COALESCE(p_status, ''), 1)
            ON CONFLICT (type, status, sender, owner)
            DO UPDATE SET count = message_counts.count + 1;

            -- By (sender, type, status) — needed for per-address stats
            INSERT INTO message_counts (sender, type, status, count)
            VALUES (COALESCE(p_sender, ''), COALESCE(p_type, ''), COALESCE(p_status, ''), 1)
            ON CONFLICT (type, status, sender, owner)
            DO UPDATE SET count = message_counts.count + 1;

            -- By (owner, status) -- only if owner is set
            IF p_owner IS NOT NULL AND p_owner != '' THEN
                INSERT INTO message_counts (owner, status, count)
                VALUES (p_owner, COALESCE(p_status, ''), 1)
                ON CONFLICT (type, status, sender, owner)
                DO UPDATE SET count = message_counts.count + 1;
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        -- Helper: decrement (mirror of increment)
        CREATE OR REPLACE FUNCTION _decrement_message_counts(
            p_type VARCHAR, p_status VARCHAR, p_sender VARCHAR, p_owner VARCHAR
        ) RETURNS VOID AS $$
        BEGIN
            UPDATE message_counts SET count = count - 1
            WHERE type = '' AND status = COALESCE(p_status, '')
              AND sender = '' AND owner = '';

            UPDATE message_counts SET count = count - 1
            WHERE type = COALESCE(p_type, '') AND status = COALESCE(p_status, '')
              AND sender = '' AND owner = '';

            UPDATE message_counts SET count = count - 1
            WHERE type = '' AND status = COALESCE(p_status, '')
              AND sender = COALESCE(p_sender, '') AND owner = '';

            UPDATE message_counts SET count = count - 1
            WHERE type = COALESCE(p_type, '') AND status = COALESCE(p_status, '')
              AND sender = COALESCE(p_sender, '') AND owner = '';

            IF p_owner IS NOT NULL AND p_owner != '' THEN
                UPDATE message_counts SET count = count - 1
                WHERE type = '' AND status = COALESCE(p_status, '')
                  AND sender = '' AND owner = p_owner;
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        -- Main trigger function
        CREATE OR REPLACE FUNCTION update_message_counts()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM _increment_message_counts(
                    NEW.type, NEW.status, NEW.sender, NEW.owner
                );
                RETURN NEW;
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF OLD.status IS DISTINCT FROM NEW.status THEN
                    PERFORM _decrement_message_counts(
                        OLD.type, OLD.status, OLD.sender, OLD.owner
                    );
                    PERFORM _increment_message_counts(
                        NEW.type, NEW.status, NEW.sender, NEW.owner
                    );
                END IF;
                RETURN NEW;
            END IF;

            IF TG_OP = 'DELETE' THEN
                PERFORM _decrement_message_counts(
                    OLD.type, OLD.status, OLD.sender, OLD.owner
                );
                RETURN OLD;
            END IF;

            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        -- Attach trigger (will be disabled during backfill in 0049)
        CREATE TRIGGER trg_message_counts
            AFTER INSERT OR UPDATE OR DELETE ON messages
            FOR EACH ROW
            EXECUTE FUNCTION update_message_counts();
        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        DROP TRIGGER IF EXISTS trg_message_counts ON messages;
        DROP FUNCTION IF EXISTS update_message_counts();
        DROP FUNCTION IF EXISTS _decrement_message_counts(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        DROP FUNCTION IF EXISTS _increment_message_counts(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        DROP TABLE IF EXISTS message_counts;
        """
        )
    )
