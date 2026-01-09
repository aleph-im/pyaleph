"""balance views

Revision ID: 0bfde82697c8
Revises: 7b547d707a2f
Create Date: 2023-01-20 17:18:01.145689

"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0bfde82697c8"
down_revision = "7b547d707a2f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
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
    )
    op.execute(
        text(
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
    )
    op.execute(
        text("""
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
    ))


def downgrade() -> None:
    op.execute(text("drop view costs_view"))
    op.execute(text("drop view program_costs_view"))
    op.execute(text("drop view program_volumes_files_view"))
