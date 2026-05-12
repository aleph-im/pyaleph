"""replace credit_balances with lot-cache schema

The previous ``credit_balances`` table was a single integer sum per address,
recomputed lazily on the read path (and written back from inside the read
itself). This change replaces it with a per-lot cache: one row per granting
``credit_history`` entry, with ``amount_remaining`` decremented eagerly by
writers (distribution, expense, transfer). Reads become a simple ``SUM``
over still-valid lots, no FIFO walk and no write-back.

The table is a pure cache derived from ``credit_history``. The matching
``_repair_credit_balances`` startup hook rebuilds it from history, so the
upgrade does not need to backfill data in the migration itself.

Revision ID: a8c3d9f1b2e4
Revises: 7e5a630e4b36
Create Date: 2026-05-12 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import func

revision = "a8c3d9f1b2e4"
down_revision = "7e5a630e4b36"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_credit_balances_address", table_name="credit_balances")
    op.drop_table("credit_balances")

    op.create_table(
        "credit_balances",
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("credit_ref", sa.String(), nullable=False),
        sa.Column("credit_index", sa.Integer(), nullable=False),
        sa.Column("amount_remaining", sa.BigInteger(), nullable=False),
        sa.Column("expiration_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("message_timestamp", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "last_update",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
        sa.PrimaryKeyConstraint(
            "address", "credit_ref", "credit_index", name="credit_balances_pkey"
        ),
    )

    op.create_index(
        "ix_credit_balances_address_order",
        "credit_balances",
        ["address", "message_timestamp", "credit_ref", "credit_index"],
    )
    op.create_index(
        "ix_credit_balances_address_active",
        "credit_balances",
        ["address"],
        postgresql_where=sa.text("amount_remaining > 0"),
    )


def downgrade() -> None:
    op.drop_index("ix_credit_balances_address_active", table_name="credit_balances")
    op.drop_index("ix_credit_balances_address_order", table_name="credit_balances")
    op.drop_table("credit_balances")

    op.create_table(
        "credit_balances",
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("balance", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "last_update",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        ),
        sa.PrimaryKeyConstraint("address", name="credit_balances_pkey"),
    )
    op.create_index(
        "ix_credit_balances_address", "credit_balances", ["address"], unique=False
    )
