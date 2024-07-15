import datetime as dt
from typing import Any, Optional, Dict, List

from aleph_message.models.execution import MachineType, Encoding
from aleph_message.models.execution.volume import VolumePersistence
from sqlalchemy import Column, String, ForeignKey, Boolean, Integer, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declared_attr, Mapped
from sqlalchemy_utils import ChoiceType

from aleph.types.vms import CpuArchitecture, VmVersion, VmType
from .base import Base


class ProgramVolumeMixin:
    @declared_attr
    def program_hash(cls) -> Mapped[str]:
        return Column(
            "program_hash",
            ForeignKey("vms.item_hash", ondelete="CASCADE"),
            primary_key=True,
        )

    encoding: Encoding = Column(ChoiceType(Encoding), nullable=False)


class VolumeWithRefMixin:
    ref: str = Column(String, nullable=True)
    use_latest: bool = Column(Boolean, nullable=True)


class RootfsVolumeDb(Base):
    __tablename__ = "instance_rootfs"

    instance_hash: str = Column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), primary_key=True
    )
    parent_ref: str = Column(String, nullable=False)
    parent_use_latest: bool = Column(Boolean, nullable=False)
    size_mib: int = Column(Integer, nullable=False)
    persistence: VolumePersistence = Column(
        ChoiceType(VolumePersistence), nullable=False
    )

    instance: "VmInstanceDb" = relationship("VmInstanceDb", back_populates="rootfs")


class CodeVolumeDb(Base, ProgramVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_code_volumes"

    entrypoint: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="code_volume")


class DataVolumeDb(Base, ProgramVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_data_volumes"

    mount: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="data_volume")


class ExportVolumeDb(Base, ProgramVolumeMixin):
    __tablename__ = "program_export_volumes"

    program: "ProgramDb" = relationship("ProgramDb", back_populates="export_volume")


class RuntimeDb(Base, VolumeWithRefMixin):
    __tablename__ = "program_runtimes"

    program_hash: Mapped[str] = Column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), primary_key=True
    )
    comment: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="runtime")


class MachineVolumeBaseDb(Base):
    __tablename__ = "vm_machine_volumes"

    id: int = Column(Integer, primary_key=True)
    type: str = Column(String, nullable=False)
    vm_hash: str = Column(
        ForeignKey("vms.item_hash", ondelete="CASCADE"), nullable=False, index=True
    )
    comment: Optional[str] = Column(String, nullable=True)
    mount: Optional[str] = Column(String, nullable=True)
    size_mib: int = Column(Integer, nullable=True)

    vm: "VmBaseDb" = relationship("VmBaseDb", back_populates="volumes")

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }


class ImmutableVolumeDb(MachineVolumeBaseDb, VolumeWithRefMixin):
    __mapper_args__ = {"polymorphic_identity": "immutable"}


class EphemeralVolumeDb(MachineVolumeBaseDb):
    __mapper_args__ = {"polymorphic_identity": "ephemeral"}


class PersistentVolumeDb(MachineVolumeBaseDb):
    parent_ref: Optional[str] = Column(String, nullable=True)
    parent_use_latest: Optional[bool] = Column(Boolean, nullable=True)
    persistence: VolumePersistence = Column(
        ChoiceType(VolumePersistence), nullable=True
    )
    name: str = Column(String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "persistent"}


class VmBaseDb(Base):
    __tablename__ = "vms"

    item_hash: str = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False, index=True)

    type: VmType = Column(ChoiceType(VmType), nullable=False)

    allow_amend: bool = Column(Boolean, nullable=False)
    # Note: metadata is a reserved keyword for SQLAlchemy
    metadata_: Optional[Dict[str, Any]] = Column("metadata", JSONB, nullable=True)
    variables: Optional[Dict[str, Any]] = Column(JSONB, nullable=True)
    message_triggers: Optional[List[Dict[str, Any]]] = Column(JSONB, nullable=True)

    environment_reproducible: bool = Column(Boolean, nullable=False)
    environment_internet: bool = Column(Boolean, nullable=False)
    environment_aleph_api: bool = Column(Boolean, nullable=False)
    environment_shared_cache: bool = Column(Boolean, nullable=False)

    environment_trusted_execution_policy: Optional[int] = Column(Integer, nullable=True)
    environment_trusted_execution_firmware: Optional[str] = Column(
        String, nullable=True
    )

    resources_vcpus: int = Column(Integer, nullable=False)
    resources_memory: int = Column(Integer, nullable=False)
    resources_seconds: int = Column(Integer, nullable=False)

    cpu_architecture: Optional[CpuArchitecture] = Column(
        ChoiceType(CpuArchitecture), nullable=True
    )
    cpu_vendor: Optional[str] = Column(String, nullable=True)
    node_owner: Optional[str] = Column(String, nullable=True)
    node_address_regex: Optional[str] = Column(String, nullable=True)
    node_hash: Optional[str] = Column(String, nullable=True)

    replaces: Optional[str] = Column(ForeignKey(item_hash), nullable=True)
    created: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)

    authorized_keys: Optional[List[str]] = Column(JSONB, nullable=True)

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }

    volumes: List[MachineVolumeBaseDb] = relationship(
        MachineVolumeBaseDb, back_populates="vm", uselist=True
    )


class VmInstanceDb(VmBaseDb):
    __mapper_args__ = {
        "polymorphic_identity": VmType.INSTANCE.value,
    }

    rootfs: RootfsVolumeDb = relationship(
        "RootfsVolumeDb", back_populates="instance", uselist=False
    )


class ProgramDb(VmBaseDb):
    __mapper_args__ = {
        "polymorphic_identity": VmType.PROGRAM.value,
    }

    program_type: MachineType = Column(ChoiceType(MachineType), nullable=True)
    http_trigger: bool = Column(Boolean, nullable=True)
    persistent: bool = Column(Boolean, nullable=True)

    code_volume: CodeVolumeDb = relationship(
        "CodeVolumeDb",
        back_populates="program",
        uselist=False,
    )
    runtime: RuntimeDb = relationship(
        "RuntimeDb", back_populates="program", uselist=False
    )
    data_volume: Optional[DataVolumeDb] = relationship(
        "DataVolumeDb",
        back_populates="program",
        uselist=False,
    )
    export_volume: Optional[ExportVolumeDb] = relationship(
        "ExportVolumeDb",
        back_populates="program",
        uselist=False,
    )


class VmVersionDb(Base):
    __tablename__ = "vm_versions"

    vm_hash: str = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False)
    current_version: VmVersion = Column(String, nullable=False)
    last_updated: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)
