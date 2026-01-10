import datetime as dt
from typing import Any, Dict, List, Optional

from aleph_message.models.execution import Encoding, MachineType
from aleph_message.models.execution.volume import VolumePersistence
from sqlalchemy import TIMESTAMP, Boolean, ForeignKey, Integer, String
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

    entrypoint: Mapped[str] = mapped_column(String, nullable=False)
    program: Mapped["ProgramDb"] = relationship(
        "ProgramDb", back_populates="code_volume"
    )


class DataVolumeDb(Base, ProgramVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_data_volumes"

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


class VmVersionDb(Base):
    __tablename__ = "vm_versions"

    vm_hash: Mapped[str] = mapped_column(String, primary_key=True)
    owner: Mapped[str] = mapped_column(String, nullable=False)
    current_version: Mapped[VmVersion] = mapped_column(String, nullable=False)
    last_updated: Mapped[dt.datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
