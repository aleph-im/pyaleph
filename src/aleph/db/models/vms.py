import datetime as dt
from typing import Any, Dict, List, Optional

from aleph_message.models.execution import Encoding, MachineType
from aleph_message.models.execution.volume import VolumePersistence
from sqlalchemy import TIMESTAMP, Boolean, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship
from sqlalchemy_utils import ChoiceType

from aleph.types.vms import CpuArchitecture, VmType, VmVersion

from .base import Base


class ProgramVolumeMixin:
    @declared_attr
    def program_hash(cls) -> Mapped[str]:
        return mapped_column(
            "program_hash",
            ForeignKey("vms.item_hash", ondelete="CASCADE"),
            primary_key=True,
        )

    encoding: Mapped[Encoding] = mapped_column(ChoiceType(Encoding), nullable=False)


class VolumeWithRefMixin:
    ref: Mapped[str] = mapped_column(String, nullable=True)
    use_latest: Mapped[bool] = mapped_column(Boolean, nullable=True)


class RootfsVolumeDb(Base):
    __tablename__ = "instance_rootfs"
    __table_args__ = (Index("ix_instance_rootfs_parent_ref", "parent_ref"),)

    instance_hash: Mapped[str] = mapped_column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), primary_key=True
    )
    parent_ref: Mapped[str] = mapped_column(String, nullable=False)
    parent_use_latest: Mapped[bool] = mapped_column(Boolean, nullable=False)
    size_mib: Mapped[int] = mapped_column(Integer, nullable=False)
    persistence: Mapped[VolumePersistence] = mapped_column(
        ChoiceType(VolumePersistence), nullable=False
    )

    instance: Mapped["VmInstanceDb"] = relationship(
        "VmInstanceDb", back_populates="rootfs"
    )


class CodeVolumeDb(Base, ProgramVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_code_volumes"
    __table_args__ = (Index("ix_program_code_volumes_ref", "ref"),)

    entrypoint: Mapped[str] = mapped_column(String, nullable=False)
    program: Mapped["ProgramDb"] = relationship(
        "ProgramDb", back_populates="code_volume"
    )


class DataVolumeDb(Base, ProgramVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_data_volumes"
    __table_args__ = (Index("ix_program_data_volumes_ref", "ref"),)

    mount: Mapped[str] = mapped_column(String, nullable=False)
    program: Mapped["ProgramDb"] = relationship(
        "ProgramDb", back_populates="data_volume"
    )


class ExportVolumeDb(Base, ProgramVolumeMixin):
    __tablename__ = "program_export_volumes"

    program: Mapped["ProgramDb"] = relationship(
        "ProgramDb", back_populates="export_volume"
    )


class RuntimeDb(Base, VolumeWithRefMixin):
    __tablename__ = "program_runtimes"
    __table_args__ = (Index("ix_program_runtimes_ref", "ref"),)

    program_hash: Mapped[str] = mapped_column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), primary_key=True
    )
    comment: Mapped[str] = mapped_column(String, nullable=False)
    program: Mapped["ProgramDb"] = relationship("ProgramDb", back_populates="runtime")


class MachineVolumeBaseDb(Base):
    __tablename__ = "vm_machine_volumes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    vm_hash: Mapped[str] = mapped_column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), nullable=False, index=True
    )
    comment: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    mount: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    size_mib: Mapped[int] = mapped_column(Integer, nullable=True)

    vm: Mapped["VmBaseDb"] = relationship("VmBaseDb", back_populates="volumes")

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }


class ImmutableVolumeDb(MachineVolumeBaseDb, VolumeWithRefMixin):
    __mapper_args__ = {"polymorphic_identity": "immutable"}


# `ref` is added to the shared `vm_machine_volumes` table by the
# ImmutableVolumeDb single-table-inheritance mapping above, so the index has
# to be declared here instead of in a `__table_args__` on that class.
Index("ix_vm_machine_volumes_ref", MachineVolumeBaseDb.__table__.c.ref)


class EphemeralVolumeDb(MachineVolumeBaseDb):
    __mapper_args__ = {"polymorphic_identity": "ephemeral"}


class PersistentVolumeDb(MachineVolumeBaseDb):
    parent_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    parent_use_latest: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    persistence: Mapped[VolumePersistence] = mapped_column(
        ChoiceType(VolumePersistence), nullable=True
    )
    name: Mapped[str] = mapped_column(String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "persistent"}


class VmBaseDb(Base):
    __tablename__ = "vms"

    item_hash: Mapped[str] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False, index=True)

    type: Mapped[VmType] = mapped_column(ChoiceType(VmType), nullable=False)

    allow_amend: Mapped[bool] = mapped_column(Boolean, nullable=False)
    # Note: metadata is a reserved keyword for SQLAlchemy
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        "metadata", JSONB, nullable=True
    )
    variables: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    message_triggers: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(
        JSONB, nullable=True
    )

    environment_reproducible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    environment_internet: Mapped[bool] = mapped_column(Boolean, nullable=False)
    environment_aleph_api: Mapped[bool] = mapped_column(Boolean, nullable=False)
    environment_shared_cache: Mapped[bool] = mapped_column(Boolean, nullable=False)

    environment_trusted_execution_policy: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    environment_trusted_execution_firmware: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )

    payment_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    resources_vcpus: Mapped[int] = mapped_column(Integer, nullable=False)
    resources_memory: Mapped[int] = mapped_column(Integer, nullable=False)
    resources_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    cpu_architecture: Mapped[Optional[CpuArchitecture]] = mapped_column(
        ChoiceType(CpuArchitecture), nullable=True
    )
    cpu_vendor: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    node_owner: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    node_address_regex: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    node_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    replaces: Mapped[Optional[str]] = mapped_column(
        ForeignKey(item_hash), nullable=True
    )
    created: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )

    authorized_keys: Mapped[Optional[List[str]]] = mapped_column(JSONB, nullable=True)

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }

    volumes: Mapped[List[MachineVolumeBaseDb]] = relationship(
        MachineVolumeBaseDb, back_populates="vm", uselist=True
    )


class VmInstanceDb(VmBaseDb):
    __mapper_args__ = {
        "polymorphic_identity": VmType.INSTANCE.value,
    }

    rootfs: Mapped[RootfsVolumeDb] = relationship(
        "RootfsVolumeDb", back_populates="instance", uselist=False
    )


class ProgramDb(VmBaseDb):
    __mapper_args__ = {
        "polymorphic_identity": VmType.PROGRAM.value,
    }

    program_type: Mapped[MachineType] = mapped_column(
        ChoiceType(MachineType), nullable=True
    )
    http_trigger: Mapped[bool] = mapped_column(Boolean, nullable=True)
    persistent: Mapped[bool] = mapped_column(Boolean, nullable=True)

    code_volume: Mapped[CodeVolumeDb] = relationship(
        "CodeVolumeDb",
        back_populates="program",
        uselist=False,
    )
    runtime: Mapped[RuntimeDb] = relationship(
        "RuntimeDb", back_populates="program", uselist=False
    )
    data_volume: Mapped[Optional[DataVolumeDb]] = relationship(
        "DataVolumeDb",
        back_populates="program",
        uselist=False,
    )
    export_volume: Mapped[Optional[ExportVolumeDb]] = relationship(
        "ExportVolumeDb",
        back_populates="program",
        uselist=False,
    )


class VProgramVerifiedVolumeDb(Base):
    """A verity-bound read-only volume of a V-Program.

    Volumes are positional: the measured cmdline carries the roothashes in
    list order, so `position` preserves the message's volume order.
    """

    __tablename__ = "vprogram_verified_volumes"
    __table_args__ = (
        Index("ix_vprogram_verified_volumes_ref", "ref"),
        Index("ix_vprogram_verified_volumes_hash_tree", "hash_tree"),
    )

    vm_hash: Mapped[str] = mapped_column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    ref: Mapped[str] = mapped_column(String, nullable=False)
    hash_tree: Mapped[str] = mapped_column(String, nullable=False)
    roothash: Mapped[str] = mapped_column(String, nullable=False)
    comment: Mapped[str] = mapped_column(String, nullable=False)

    vprogram: Mapped["VProgramDb"] = relationship(
        "VProgramDb", back_populates="verified_volumes"
    )


class VProgramDb(VmBaseDb):
    """A verifiable program (V-PROGRAM).

    The runtime manifest and the workload are single refs, so they live in
    columns (like ProgramDb's scalar fields); the verified volume list gets
    its own table. There is no use_latest anywhere: V-Programs pin exact
    item hashes, and they are immutable (the schema rejects allow_amend and
    replaces), so they also keep no vm_versions rows.
    """

    __mapper_args__ = {
        "polymorphic_identity": VmType.VPROGRAM.value,
    }

    runtime_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    runtime_comment: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # The bundle named by the runtime manifest, resolved once at processing
    # time. Persisting it means the manifest never has to be re-read (cost
    # recalculation) and the bundle STORE is forget-protected like the other
    # artifacts. Nullable: rows written before this column existed have None.
    runtime_bundle_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    workload_ref: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    workload_hash_tree: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    workload_roothash: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    verified_volumes: Mapped[List[VProgramVerifiedVolumeDb]] = relationship(
        VProgramVerifiedVolumeDb,
        back_populates="vprogram",
        uselist=True,
        order_by=VProgramVerifiedVolumeDb.position,
    )


# `runtime_ref`, `runtime_bundle_ref`, `workload_ref` and `workload_hash_tree`
# are added to the shared `vms` table by the VProgramDb single-table-inheritance
# mapping above, so the indexes have to be declared here instead of in a
# `__table_args__` on that class. They are partial: only V-PROGRAM rows ever
# populate these columns, matching the `type` discriminator VProgramDb's
# polymorphic queries filter on automatically.
_VPROGRAM_ONLY = text(f"type = '{VmType.VPROGRAM.value}'")
Index(
    "ix_vms_runtime_ref",
    VmBaseDb.__table__.c.runtime_ref,
    postgresql_where=_VPROGRAM_ONLY,
)
Index(
    "ix_vms_runtime_bundle_ref",
    VmBaseDb.__table__.c.runtime_bundle_ref,
    postgresql_where=_VPROGRAM_ONLY,
)
Index(
    "ix_vms_workload_ref",
    VmBaseDb.__table__.c.workload_ref,
    postgresql_where=_VPROGRAM_ONLY,
)
Index(
    "ix_vms_workload_hash_tree",
    VmBaseDb.__table__.c.workload_hash_tree,
    postgresql_where=_VPROGRAM_ONLY,
)


class VmVersionDb(Base):
    __tablename__ = "vm_versions"

    vm_hash: Mapped[str] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False)
    current_version: Mapped[VmVersion] = mapped_column(String, nullable=False)
    last_updated: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
