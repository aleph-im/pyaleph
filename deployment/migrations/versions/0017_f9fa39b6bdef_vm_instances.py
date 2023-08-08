"""vm instances

Revision ID: f9fa39b6bdef
Revises: 77e68941d36c
Create Date: 2023-05-17 11:59:42.783630

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f9fa39b6bdef"
down_revision = "77e68941d36c"
branch_labels = None
depends_on = None


def recreate_cost_views():
    op.execute("DROP VIEW costs_view")
    op.execute("DROP VIEW program_costs_view")
    op.execute("DROP VIEW program_volumes_files_view")

    # Recreate the views using `vm` instead of `program` wherever necessary
    op.execute(
        """
        create view vm_volumes_files_view(vm_hash, ref, use_latest, type, latest, original, volume_to_use) as
        SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'code_volume'       AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_code_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'data_volume'       AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_data_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'runtime'           AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_runtimes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.vm_hash,
               volume.ref,
               volume.use_latest,
               'machine_volume'    AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM vm_machine_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        """
    )
    op.execute(
        """
        create view vm_costs_view as
            SELECT vm_versions.vm_hash,
                   vm_versions.owner,
                   vms.resources_vcpus,
                   vms.resources_memory,
                   file_volumes_size.file_volumes_size,
                   other_volumes_size.other_volumes_size,
                   used_disk.required_disk_space,
                   cu.compute_units_required,
                   bcp.base_compute_unit_price,
                   m.compute_unit_price_multiplier,
                   cpm.compute_unit_price,
                   free_disk.included_disk_space,
                   additional_disk.additional_disk_space,
                   adp.disk_price,
                   tp.total_price
            FROM vm_versions
                     JOIN vms on vm_versions.current_version = vms.item_hash
                     JOIN (SELECT volume.vm_hash,
                                  sum(files.size) AS file_volumes_size
                           FROM vm_volumes_files_view volume
                                    LEFT JOIN files ON volume.volume_to_use = files.hash
                           GROUP BY volume.vm_hash) file_volumes_size
                          ON vm_versions.current_version = file_volumes_size.vm_hash
                     JOIN (SELECT instance_hash, size_mib * 1024 * 1024 rootfs_size FROM instance_rootfs) rootfs_size
                     ON vm_versions.vm_hash = rootfs_size.instance_hash
                     JOIN (SELECT vm_hash, SUM(size_mib) * 1024 * 1024 other_volumes_size
                           FROM vm_machine_volumes
                           GROUP BY vm_hash) other_volumes_size
                          ON vm_versions.current_version = other_volumes_size.vm_hash,
                 LATERAL (SELECT file_volumes_size + other_volumes_size AS required_disk_space) used_disk,
                 LATERAL ( SELECT ceil(GREATEST(ceil(vms.resources_vcpus / 1),
                                                vms.resources_memory / 2000)) AS compute_units_required) cu,
                 LATERAL ( SELECT CASE
                                      WHEN vms.persistent
                                          THEN 20000000000 * cu.compute_units_required
                                      ELSE 2000000000 * cu.compute_units_required
                                      END AS included_disk_space) free_disk,
                 LATERAL ( SELECT GREATEST(file_volumes_size.file_volumes_size +
                                           rootfs_size.rootfs_size + 
                                           other_volumes_size.other_volumes_size -
                                           free_disk.included_disk_space,
                                           0) AS additional_disk_space) additional_disk,
                 LATERAL ( SELECT CASE
                                      WHEN vms.persistent THEN 2000
                                      ELSE 200
                                      END AS base_compute_unit_price) bcp,
                 LATERAL ( SELECT 1 + vms.environment_internet::integer AS compute_unit_price_multiplier) m,
                 LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                                  bcp.base_compute_unit_price::double precision *
                                  m.compute_unit_price_multiplier AS compute_unit_price) cpm,
                 LATERAL ( SELECT additional_disk.additional_disk_space * 20::double precision /
                                  1000000::double precision AS disk_price) adp,
                 LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp
        """
    )
    op.execute(
        """
        create view costs_view as
        SELECT coalesce(vm_prices.owner, storage.owner) address,
               total_vm_cost,
               total_storage_cost,
               total_cost
        FROM (SELECT owner, sum(total_price) total_vm_cost FROM vm_costs_view GROUP BY owner) vm_prices
                 FULL OUTER JOIN (SELECT owner, sum(f.size) storage_size
                                  FROM file_pins
                                           JOIN files f on file_pins.file_hash = f.hash
                                  WHERE owner is not null
                                  GROUP BY owner) storage ON vm_prices.owner = storage.owner,
             LATERAL (SELECT 3 * storage_size / 1000000 total_storage_cost) sc,
             LATERAL (SELECT coalesce(vm_prices.total_vm_cost, 0) +
                             coalesce(total_storage_cost, 0) AS total_cost ) tc;
        """
    )


def upgrade() -> None:
    # Rename all common tables to `vm_*`
    op.rename_table("programs", "vms")
    op.rename_table("program_machine_volumes", "vm_machine_volumes")
    op.rename_table("program_versions", "vm_versions")

    # Rename all common columns to `vm_*`
    op.alter_column("vm_machine_volumes", "program_hash", new_column_name="vm_hash")
    op.alter_column("vm_versions", "program_hash", new_column_name="vm_hash")

    # Rename indexes
    op.execute(
        "ALTER INDEX ix_program_machine_volumes_program_hash RENAME TO ix_vm_machine_volumes_vm_hash"
    )
    op.execute("ALTER INDEX ix_programs_owner RENAME TO ix_vms_owner")

    # Create the instance rootfs table
    op.create_table(
        "instance_rootfs",
        sa.Column("instance_hash", sa.String(), nullable=False),
        sa.Column("parent_ref", sa.String(), nullable=False),
        sa.Column("parent_use_latest", sa.Boolean(), nullable=False),
        sa.Column("size_mib", sa.Integer(), nullable=False),
        sa.Column("persistence", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["instance_hash"],
            ["vms.item_hash"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("instance_hash"),
    )

    # Make program-only columns nullable
    op.alter_column("vms", "http_trigger", existing_type=sa.BOOLEAN(), nullable=True)
    op.alter_column("vms", "persistent", existing_type=sa.BOOLEAN(), nullable=True)

    # Recreate the cost views (some column names must change)
    recreate_cost_views()

    # Add the parent columns for persistent volumes
    op.add_column(
        "vm_machine_volumes", sa.Column("parent_ref", sa.String(), nullable=True)
    )
    op.add_column(
        "vm_machine_volumes",
        sa.Column("parent_use_latest", sa.Boolean(), nullable=True),
    )

    # Add new columns to the vms (ex programs) table
    op.add_column(
        "vms",
        sa.Column(
            "authorized_keys", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )
    op.add_column("vms", sa.Column("program_type", sa.String(), nullable=True))

    # Update error codes
    op.execute(
        "UPDATE error_codes SET description = 'VM reference not found' WHERE code = 300"
    )
    op.execute(
        "UPDATE error_codes SET description = 'VM volume reference(s) not found' WHERE code = 301"
    )
    op.execute(
        "UPDATE error_codes SET description = 'VM update not allowed' WHERE code = 302"
    )
    op.execute(
        "UPDATE error_codes SET description = 'VM update not targeting the original version of the VM' WHERE code = 303"
    )
    op.execute(
        "INSERT INTO error_codes(code, description) VALUES (304, 'VM volume parent is larger than the child volume')"
    )

    # ### end Alembic commands ###


def downgrade_cost_views():
    op.execute("DROP VIEW costs_view")
    op.execute("DROP VIEW vm_costs_view")
    op.execute("DROP VIEW vm_volumes_files_view")

    # Copied from 0007_0bfde82697c8_balance_views.py
    op.execute(
        """
        create view program_volumes_files_view(program_hash, ref, use_latest, type, latest, original, volume_to_use) as
        SELECT volume.program_hash,
               volume.ref,
               volume.use_latest,
               'code_volume'       AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_code_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.program_hash,
               volume.ref,
               volume.use_latest,
               'data_volume'       AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_data_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.program_hash,
               volume.ref,
               volume.use_latest,
               'runtime'           AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_runtimes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        UNION
        SELECT volume.program_hash,
               volume.ref,
               volume.use_latest,
               'machine_volume'    AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_machine_volumes volume
                 LEFT OUTER JOIN file_tags tags ON volume.ref = tags.tag
                 JOIN file_pins originals ON volume.ref = originals.item_hash
        """
    )
    op.execute(
        """
        create view program_costs_view as
            SELECT program_versions.program_hash,
                   program_versions.owner,
                   programs.resources_vcpus,
                   programs.resources_memory,
                   file_volumes_size.file_volumes_size,
                   other_volumes_size.other_volumes_size,
                   used_disk.required_disk_space,
                   cu.compute_units_required,
                   bcp.base_compute_unit_price,
                   m.compute_unit_price_multiplier,
                   cpm.compute_unit_price,
                   free_disk.included_disk_space,
                   additional_disk.additional_disk_space,
                   adp.disk_price,
                   tp.total_price
            FROM program_versions
                     JOIN programs on program_versions.current_version = programs.item_hash
                     JOIN (SELECT volume.program_hash,
                                  sum(files.size) AS file_volumes_size
                           FROM program_volumes_files_view volume
                                    LEFT JOIN files ON volume.volume_to_use = files.hash
                           GROUP BY volume.program_hash) file_volumes_size
                          ON program_versions.current_version = file_volumes_size.program_hash
                     JOIN (SELECT program_hash, SUM(size_mib) * 1024 * 1024 other_volumes_size
                           FROM program_machine_volumes
                           GROUP BY program_hash) other_volumes_size
                          ON program_versions.current_version = other_volumes_size.program_hash,
                 LATERAL (SELECT file_volumes_size + other_volumes_size AS required_disk_space) used_disk,
                 LATERAL ( SELECT ceil(GREATEST(ceil(programs.resources_vcpus / 1),
                                                programs.resources_memory / 2000)) AS compute_units_required) cu,
                 LATERAL ( SELECT CASE
                                      WHEN programs.persistent
                                          THEN 20000000000 * cu.compute_units_required
                                      ELSE 2000000000 * cu.compute_units_required
                                      END AS included_disk_space) free_disk,
                 LATERAL ( SELECT GREATEST(file_volumes_size.file_volumes_size + other_volumes_size.other_volumes_size -
                                           free_disk.included_disk_space,
                                           0) AS additional_disk_space) additional_disk,
                 LATERAL ( SELECT CASE
                                      WHEN programs.persistent THEN 2000
                                      ELSE 200
                                      END AS base_compute_unit_price) bcp,
                 LATERAL ( SELECT 1 + programs.environment_internet::integer AS compute_unit_price_multiplier) m,
                 LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                                  bcp.base_compute_unit_price::double precision *
                                  m.compute_unit_price_multiplier AS compute_unit_price) cpm,
                 LATERAL ( SELECT additional_disk.additional_disk_space * 20::double precision /
                                  1000000::double precision AS disk_price) adp,
                 LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp
        """
    )
    op.execute(
        """
        create view costs_view as
        SELECT coalesce(program_prices.owner, storage.owner) address,
               total_program_cost,
               total_storage_cost,
               total_cost
        FROM (SELECT owner, sum(total_price) total_program_cost FROM program_costs_view GROUP BY owner) program_prices
                 FULL OUTER JOIN (SELECT owner, sum(f.size) storage_size
                                  FROM file_pins
                                           JOIN files f on file_pins.file_hash = f.hash
                                  WHERE owner is not null
                                  GROUP BY owner) storage ON program_prices.owner = storage.owner,
             LATERAL (SELECT 3 * storage_size / 1000000 total_storage_cost) sc,
             LATERAL (SELECT coalesce(program_prices.total_program_cost, 0) +
                             coalesce(total_storage_cost, 0) AS total_cost ) tc;
        """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###

    # Rename tables / columns
    op.rename_table("vms", "programs")
    op.rename_table("vm_machine_volumes", "program_machine_volumes")
    op.rename_table("vm_versions", "program_versions")
    op.alter_column("program_versions", "vm_hash", new_column_name="program_hash")
    op.alter_column(
        "program_machine_volumes", "vm_hash", new_column_name="program_hash"
    )

    # Rename indexes
    op.execute(
        "ALTER INDEX ix_vm_machine_volumes_vm_hash RENAME TO ix_program_machine_volumes_program_hash"
    )
    op.execute("ALTER INDEX ix_vms_owner RENAME TO ix_programs_owner")

    op.drop_column("programs", "program_type")
    op.drop_column('programs', 'authorized_keys')

    # Drop the parent column for persistent VMs
    op.drop_column("program_machine_volumes", "parent")

    # Make program-only columns non-nullable again
    op.alter_column(
        "programs", "persistent", existing_type=sa.BOOLEAN(), nullable=False
    )
    op.alter_column(
        "programs", "http_trigger", existing_type=sa.BOOLEAN(), nullable=False
    )

    # Reset views
    downgrade_cost_views()

    # Drop the rootfs table
    op.drop_table("instance_rootfs")

    # Revert error codes
    op.execute(
        "UPDATE error_codes SET description = 'Program reference not found' WHERE code = 300"
    )
    op.execute(
        "UPDATE error_codes SET description = 'Program volume reference(s) not found' WHERE code = 301"
    )
    op.execute(
        "UPDATE error_codes SET description = 'Program update not allowed' WHERE code = 302"
    )
    op.execute(
        "UPDATE error_codes "
        "SET description = 'Program update not targeting the original version of the program' WHERE code = 303"
    )
    op.execute("DELETE FROM error_codes WHERE code = 304")

    # ### end Alembic commands ###
