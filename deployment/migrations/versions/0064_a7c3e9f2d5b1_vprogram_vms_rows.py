"""Add the vms representation of V-PROGRAM messages

Revision ID: a7c3e9f2d5b1
Revises: f4b8a2d7c1e9
Create Date: 2026-07-10

V-Programs get a row in the single-inheritance vms table (type='vprogram').
The runtime manifest and workload are single refs, so they live in nullable
columns on vms (like the program-specific columns); the positional verified
volume list gets its own table, cascading on vms deletion like the other
volume tables.

These rows exist so that get_vms_dependent_volumes sees the STORE files a
V-Program references (runtime manifest, workload image and hash tree,
verified volumes and their hash trees) and the forget handler blocks
forgetting them while the V-Program is alive.

No data backfill: no V-PROGRAM message can have reached PROCESSED before
this migration (the message type ships in the same release).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a7c3e9f2d5b1"
down_revision = "f4b8a2d7c1e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vms", sa.Column("runtime_ref", sa.String(), nullable=True))
    op.add_column("vms", sa.Column("runtime_comment", sa.String(), nullable=True))
    op.add_column("vms", sa.Column("workload_ref", sa.String(), nullable=True))
    op.add_column("vms", sa.Column("workload_hash_tree", sa.String(), nullable=True))
    op.add_column("vms", sa.Column("workload_roothash", sa.String(), nullable=True))
    op.create_table(
        "vprogram_verified_volumes",
        sa.Column("vm_hash", sa.String(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("ref", sa.String(), nullable=False),
        sa.Column("hash_tree", sa.String(), nullable=False),
        sa.Column("roothash", sa.String(), nullable=False),
        sa.Column("comment", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["vm_hash"], ["vms.item_hash"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("vm_hash", "position"),
    )


def downgrade() -> None:
    op.drop_table("vprogram_verified_volumes")
    op.drop_column("vms", "workload_roothash")
    op.drop_column("vms", "workload_hash_tree")
    op.drop_column("vms", "workload_ref")
    op.drop_column("vms", "runtime_comment")
    op.drop_column("vms", "runtime_ref")
