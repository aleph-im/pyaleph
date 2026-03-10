"""Add GIN index on security aggregate authorizations for reverse lookup

Revision ID: b3c4d5e6f7a8
Revises: 0a1b2c3d4e5f
Create Date: 2026-03-10
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7a8"
down_revision = "0a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX ix_aggregates_security_authorizations
        ON aggregates
        USING GIN ((content -> 'authorizations') jsonb_path_ops)
        WHERE key = 'security'
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_aggregates_security_authorizations")
