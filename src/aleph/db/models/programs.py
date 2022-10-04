from typing import Any, Optional, Dict, List

from aleph_message.models.program import MachineType, Encoding, VolumePersistence
from sqlalchemy import Column, String, ForeignKey, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, declared_attr, Mapped
from sqlalchemy_utils import ChoiceType

from aleph.types.vms import CpuArchitecture
from .base import Base


class RootVolumeMixin:
    @declared_attr
    def program_hash(cls) -> Mapped[str]:
        return Column(
            "program_hash", ForeignKey("programs.item_hash"), primary_key=True
        )

    encoding: Encoding = Column(ChoiceType(Encoding), nullable=False)


class VolumeWithRefMixin:
    ref: str = Column(String, nullable=True)
    use_latest: bool = Column(Boolean, nullable=True)


class CodeVolumeDb(Base, RootVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_code_volumes"

    entrypoint: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="code_volume")


class DataVolumeDb(Base, RootVolumeMixin, VolumeWithRefMixin):
    __tablename__ = "program_data_volumes"

    mount: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="data_volume")


class ExportVolumeDb(Base, RootVolumeMixin):
    __tablename__ = "program_export_volumes"

    program: "ProgramDb" = relationship("ProgramDb", back_populates="export_volume")


class RuntimeDb(Base, VolumeWithRefMixin):
    __tablename__ = "program_runtimes"

    program_hash: Mapped[str] = Column(
        ForeignKey("programs.item_hash"), primary_key=True
    )
    comment: str = Column(String, nullable=False)
    program: "ProgramDb" = relationship("ProgramDb", back_populates="runtime")


class MachineVolumeBaseDb(Base):
    __tablename__ = "program_machine_volumes"

    id: int = Column(Integer, primary_key=True)
    type: str = Column(String, nullable=False)
    program_hash: str = Column(
        ForeignKey("programs.item_hash"), nullable=False, index=True
    )
    comment: Optional[str] = Column(String, nullable=True)
    mount: Optional[str] = Column(String, nullable=True)
    size_mib: int = Column(Integer, nullable=True)

    program: "ProgramDb" = relationship("ProgramDb", back_populates="volumes")

    __mapper_args__: Dict[str, Any] = {
        "polymorphic_on": type,
    }


class ImmutableVolumeDb(MachineVolumeBaseDb, VolumeWithRefMixin):
    __mapper_args__ = {"polymorphic_identity": "immutable"}


class EphemeralVolumeDb(MachineVolumeBaseDb):
    __mapper_args__ = {"polymorphic_identity": "ephemeral"}


class PersistentVolumeDb(MachineVolumeBaseDb):
    persistence: VolumePersistence = Column(
        ChoiceType(VolumePersistence), nullable=True
    )
    name: str = Column(String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "persistent"}


class ProgramDb(Base):
    __tablename__ = "programs"

    item_hash: str = Column(String, primary_key=True)
    owner: str = Column(String, nullable=False, index=True)

    type: MachineType = Column(ChoiceType(MachineType), nullable=False)
    allow_amend: bool = Column(Boolean, nullable=False)
    # Note: metadata is a reserved keyword for SQLAlchemy
    metadata_: Optional[Dict[str, Any]] = Column("metadata", JSONB, nullable=True)
    variables: Optional[Dict[str, Any]] = Column(JSONB, nullable=True)
    http_trigger: bool = Column(Boolean, nullable=False)
    message_triggers: Optional[List[Dict[str, Any]]] = Column(JSONB, nullable=True)
    persistent: bool = Column(Boolean, nullable=False)

    environment_reproducible: bool = Column(Boolean, nullable=False)
    environment_internet: bool = Column(Boolean, nullable=False)
    environment_aleph_api: bool = Column(Boolean, nullable=False)
    environment_shared_cache: bool = Column(Boolean, nullable=False)

    resources_vcpus: int = Column(Integer, nullable=False)
    resources_memory: int = Column(Integer, nullable=False)
    resources_seconds: int = Column(Integer, nullable=False)

    cpu_architecture: Optional[CpuArchitecture] = Column(
        ChoiceType(CpuArchitecture), nullable=True
    )
    cpu_vendor: Optional[str] = Column(String, nullable=True)
    node_owner: Optional[str] = Column(String, nullable=True)
    node_address_regex: Optional[str] = Column(String, nullable=True)

    replaces: Optional[str] = Column(ForeignKey(item_hash), nullable=True)

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
    volumes: List[MachineVolumeBaseDb] = relationship(
        MachineVolumeBaseDb, back_populates="program", uselist=True
    )
