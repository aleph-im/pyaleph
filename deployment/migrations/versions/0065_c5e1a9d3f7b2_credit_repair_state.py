"""Add the credit_repair_state table

Revision ID: c5e1a9d3f7b2
Revises: a7c3e9f2d5b1
Create Date: 2026-07-20

Single-row bookkeeping for the boot-time credit balances repair: which drain
policy version the lot cache was last fully rebuilt under, and the
credit_history insertion watermark (max last_update) up to which out-of-order
arrivals have already been reconciled. Lets the repair skip the per-address
delete-and-replay entirely when set-based screening finds no drift.
"""

import sqlalchemy as sa
from alembic import op

revision = "c5e1a9d3f7b2"
down_revision = "a7c3e9f2d5b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "credit_repair_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("policy_version", sa.Integer(), nullable=False),
        sa.Column("history_watermark", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_run", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="credit_repair_state_pkey"),
        sa.CheckConstraint("id = 1", name="credit_repair_state_single_row"),
    )


def downgrade() -> None:
    op.drop_table("credit_repair_state")
