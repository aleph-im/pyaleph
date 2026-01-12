"""fix_vm_costs_view

Revision ID: e682fc8f9506
Revises: 08602db6c78f
Create Date: 2023-11-27 18:19:35.782198

"""

from alembic import op
from sqlalchemy import text

revision = "e682fc8f9506"
down_revision = "08602db6c78f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
        create or replace view vm_costs_view as
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
                 JOIN vms ON vm_versions.current_version::text = vms.item_hash::text
                 JOIN (SELECT volume.vm_hash,
                              sum(files.size) AS file_volumes_size
                       FROM vm_volumes_files_view volume
                                LEFT JOIN files ON volume.volume_to_use::text = files.hash::text
                       GROUP BY volume.vm_hash) file_volumes_size
                      ON vm_versions.current_version::text = file_volumes_size.vm_hash::text
                 LEFT JOIN (SELECT instance_rootfs.instance_hash,
                              instance_rootfs.size_mib::bigint * 1024 * 1024 AS rootfs_size
                       FROM instance_rootfs) rootfs_size ON vm_versions.vm_hash::text = rootfs_size.instance_hash::text
                 JOIN (SELECT vm_machine_volumes.vm_hash,
                              sum(vm_machine_volumes.size_mib) * 1024 * 1024 AS other_volumes_size
                       FROM vm_machine_volumes
                       GROUP BY vm_machine_volumes.vm_hash) other_volumes_size
                      ON vm_versions.current_version::text = other_volumes_size.vm_hash::text,
             LATERAL ( SELECT file_volumes_size.file_volumes_size +
                              other_volumes_size.other_volumes_size::numeric AS required_disk_space) used_disk,
             LATERAL ( SELECT ceil(GREATEST(ceil((vms.resources_vcpus / 1)::double precision),
                                            (vms.resources_memory / 2048)::double precision)) AS compute_units_required) cu,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true)
                                      THEN '21474836480'::bigint::double precision * cu.compute_units_required
                                  ELSE 2147483648::double precision * cu.compute_units_required
                                  END AS included_disk_space) free_disk,
             LATERAL ( SELECT GREATEST((file_volumes_size.file_volumes_size + rootfs_size.rootfs_size::numeric +
                                        other_volumes_size.other_volumes_size::numeric)::double precision -
                                       free_disk.included_disk_space,
                                       0::double precision) AS additional_disk_space) additional_disk,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true) THEN 2000
                                  ELSE 200
                                  END AS base_compute_unit_price) bcp,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true) THEN 1 + vms.environment_internet::integer
                                  ELSE 1
                                  END AS compute_unit_price_multiplier) m,
             LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                          bcp.base_compute_unit_price::double precision AS compute_unit_price) cpm,
         LATERAL ( SELECT additional_disk.additional_disk_space / 20::double precision /
                          (1024 * 1024)::double precision AS disk_price) adp,
         LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp


        """
        )
    )


def downgrade() -> None:
    op.execute(
        text(
            """
        create or replace view vm_costs_view as
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
                 JOIN vms ON vm_versions.current_version::text = vms.item_hash::text
                 JOIN (SELECT volume.vm_hash,
                              sum(files.size) AS file_volumes_size
                       FROM vm_volumes_files_view volume
                                LEFT JOIN files ON volume.volume_to_use::text = files.hash::text
                       GROUP BY volume.vm_hash) file_volumes_size
                      ON vm_versions.current_version::text = file_volumes_size.vm_hash::text
                 LEFT JOIN (SELECT instance_rootfs.instance_hash,
                              instance_rootfs.size_mib::bigint * 1024 * 1024 AS rootfs_size
                       FROM instance_rootfs) rootfs_size ON vm_versions.vm_hash::text = rootfs_size.instance_hash::text
                 JOIN (SELECT vm_machine_volumes.vm_hash,
                              sum(vm_machine_volumes.size_mib) * 1024 * 1024 AS other_volumes_size
                       FROM vm_machine_volumes
                       GROUP BY vm_machine_volumes.vm_hash) other_volumes_size
                      ON vm_versions.current_version::text = other_volumes_size.vm_hash::text,
             LATERAL ( SELECT file_volumes_size.file_volumes_size +
                              other_volumes_size.other_volumes_size::numeric AS required_disk_space) used_disk,
             LATERAL ( SELECT ceil(GREATEST(ceil((vms.resources_vcpus / 1)::double precision),
                                            (vms.resources_memory / 2048)::double precision)) AS compute_units_required) cu,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true)
                                      THEN '21474836480'::bigint::double precision * cu.compute_units_required
                                  ELSE 2147483648::double precision * cu.compute_units_required
                                  END AS included_disk_space) free_disk,
             LATERAL ( SELECT GREATEST((file_volumes_size.file_volumes_size + rootfs_size.rootfs_size::numeric +
                                        other_volumes_size.other_volumes_size::numeric)::double precision -
                                       free_disk.included_disk_space,
                                       0::double precision) AS additional_disk_space) additional_disk,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true) THEN 2000
                                  ELSE 200
                                  END AS base_compute_unit_price) bcp,
             LATERAL ( SELECT 1 + vms.environment_internet::integer AS compute_unit_price_multiplier) m,
             LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                          bcp.base_compute_unit_price::double precision AS compute_unit_price) cpm,
         LATERAL ( SELECT additional_disk.additional_disk_space * 20::double precision /
                          (1024 * 1024)::double precision AS disk_price) adp,
         LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp


        """
        )
    )
