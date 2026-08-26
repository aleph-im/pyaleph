"""Persist the V-PROGRAM runtime bundle ref

Revision ID: d2f4a7c9e1b3
Revises: b45ceae89f65
Create Date: 2026-08-27

The runtime manifest names the bundle tarball the CRN downloads. Resolving
it means reading the manifest STORE, so the result is persisted next to the
other V-PROGRAM refs on the vms table (single-inheritance, like runtime_ref
and workload_ref). Cost recalculation then reads the column instead of
re-reading the manifest, and get_vms_dependent_volumes blocks forgetting the
bundle STORE while the V-Program is alive.

No data backfill: rows written before this migration keep a NULL bundle ref
and fall back to resolving the manifest on demand.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d2f4a7c9e1b3"
down_revision = "b45ceae89f65"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("vms", sa.Column("runtime_bundle_ref", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("vms", "runtime_bundle_ref")
