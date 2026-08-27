"""Persist the V-PROGRAM runtime bundle ref, index the FORGET dependency legs

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

get_vms_dependent_volumes checks, for every FORGET target, whether any VM
still references the target file. It runs as a UNION ALL of point lookups,
one per table that can hold such a reference, so each leg needs an index to
avoid scanning its table (and, for the vms table, scanning every VM). This
migration adds those indexes:

- Partial B-tree indexes on the V-PROGRAM-only columns of vms
  (runtime_ref, runtime_bundle_ref, workload_ref, workload_hash_tree),
  scoped to `type = 'v-program'` since only V-PROGRAM rows populate them.
- Plain B-tree indexes on vprogram_verified_volumes (ref, hash_tree).
- Plain B-tree indexes on the pre-existing dependency legs that had none:
  vm_machine_volumes.ref, program_code_volumes.ref,
  program_data_volumes.ref, program_runtimes.ref, instance_rootfs.parent_ref.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d2f4a7c9e1b3"
down_revision = "b45ceae89f65"
branch_labels = None
depends_on = None

_VPROGRAM_ONLY = "type = 'v-program'"


def upgrade() -> None:
    op.add_column("vms", sa.Column("runtime_bundle_ref", sa.String(), nullable=True))

    op.create_index(
        "ix_vms_runtime_ref",
        "vms",
        ["runtime_ref"],
        postgresql_where=sa.text(_VPROGRAM_ONLY),
    )
    op.create_index(
        "ix_vms_runtime_bundle_ref",
        "vms",
        ["runtime_bundle_ref"],
        postgresql_where=sa.text(_VPROGRAM_ONLY),
    )
    op.create_index(
        "ix_vms_workload_ref",
        "vms",
        ["workload_ref"],
        postgresql_where=sa.text(_VPROGRAM_ONLY),
    )
    op.create_index(
        "ix_vms_workload_hash_tree",
        "vms",
        ["workload_hash_tree"],
        postgresql_where=sa.text(_VPROGRAM_ONLY),
    )

    op.create_index(
        "ix_vprogram_verified_volumes_ref", "vprogram_verified_volumes", ["ref"]
    )
    op.create_index(
        "ix_vprogram_verified_volumes_hash_tree",
        "vprogram_verified_volumes",
        ["hash_tree"],
    )

    op.create_index("ix_vm_machine_volumes_ref", "vm_machine_volumes", ["ref"])
    op.create_index("ix_program_code_volumes_ref", "program_code_volumes", ["ref"])
    op.create_index("ix_program_data_volumes_ref", "program_data_volumes", ["ref"])
    op.create_index("ix_program_runtimes_ref", "program_runtimes", ["ref"])
    op.create_index(
        "ix_instance_rootfs_parent_ref", "instance_rootfs", ["parent_ref"]
    )


def downgrade() -> None:
    op.drop_index("ix_instance_rootfs_parent_ref", table_name="instance_rootfs")
    op.drop_index("ix_program_runtimes_ref", table_name="program_runtimes")
    op.drop_index("ix_program_data_volumes_ref", table_name="program_data_volumes")
    op.drop_index("ix_program_code_volumes_ref", table_name="program_code_volumes")
    op.drop_index("ix_vm_machine_volumes_ref", table_name="vm_machine_volumes")

    op.drop_index(
        "ix_vprogram_verified_volumes_hash_tree",
        table_name="vprogram_verified_volumes",
    )
    op.drop_index(
        "ix_vprogram_verified_volumes_ref", table_name="vprogram_verified_volumes"
    )

    op.drop_index("ix_vms_workload_hash_tree", table_name="vms")
    op.drop_index("ix_vms_workload_ref", table_name="vms")
    op.drop_index("ix_vms_runtime_bundle_ref", table_name="vms")
    op.drop_index("ix_vms_runtime_ref", table_name="vms")

    op.drop_column("vms", "runtime_bundle_ref")
