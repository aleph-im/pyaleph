"""Replace credit_balances scalar cache with per-expiration buckets

Revision ID: b1c2d3e4f5a6
Revises: 7e5a630e4b36
Create Date: 2026-05-12

The previous credit_balances table held a single FIFO-derived scalar per
address, lazily recomputed on read by an O(N^2) Python walk over
credit_history. This migration replaces it with a bucket cache keyed by
(address, expiration_date), eagerly maintained by the credit_history
writers under a sort-by-expiration policy. The "no expiration" case is
encoded as PG ``'infinity'::timestamptz`` so expiration_date stays NOT NULL
inside the composite primary key, and read queries can use a uniform
``expiration_date > now()`` predicate without IS-NULL branches.

The previous table is dropped rather than migrated; credit_history is the
source of truth and aleph.repair.repair_credit_balances repopulates the
new table from history on the next startup.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b1c2d3e4f5a6"
down_revision = "7e5a630e4b36"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("credit_balances")
    op.create_table(
        "credit_balances",
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("expiration_date", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("amount", sa.BigInteger(), nullable=False),
        sa.Column(
            "last_update",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint(
            "address", "expiration_date", name="credit_balances_pkey"
        ),
    )
    op.create_index(
        "credit_balances_expiration_date_idx",
        "credit_balances",
        ["expiration_date"],
    )


def downgrade() -> None:
    op.drop_table("credit_balances")
    op.create_table(
        "credit_balances",
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("balance", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "last_update",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("address", name="credit_balances_pkey"),
    )
    op.create_index("ix_credit_balances_address", "credit_balances", ["address"])
