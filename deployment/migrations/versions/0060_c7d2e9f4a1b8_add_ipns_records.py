"""add ipns_records

Revision ID: c7d2e9f4a1b8
Revises: b9c4f1e6a2d7
Create Date: 2026-06-10

"""

import sqlalchemy as sa
from alembic import op

revision = "c7d2e9f4a1b8"
down_revision = "b9c4f1e6a2d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ipns_records",
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("item_hash", sa.String(), nullable=False),
        sa.Column("record", sa.LargeBinary(), nullable=True),
        sa.Column("record_sequence", sa.BigInteger(), nullable=True),
        sa.Column("record_validity", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("max_size_mib", sa.Integer(), nullable=False),
        sa.Column("resolved_cid", sa.String(), nullable=True),
        sa.Column("last_resolved", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_republished", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["resolved_cid"], ["files.hash"]),
        sa.PrimaryKeyConstraint("name", "owner"),
    )
    op.create_index("ix_ipns_records_owner", "ipns_records", ["owner"])


def downgrade() -> None:
    op.drop_index("ix_ipns_records_owner", table_name="ipns_records")
    op.drop_table("ipns_records")
