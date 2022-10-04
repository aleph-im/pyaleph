from sqlalchemy import delete

from aleph.db.models.programs import (
    CodeVolumeDb,
    DataVolumeDb,
    ExportVolumeDb,
    ProgramDb,
    RuntimeDb,
    MachineVolumeBaseDb,
)
from aleph.types.db_session import DbSession


def delete_program(session: DbSession, item_hash: str) -> None:
    volume_tables = (
        CodeVolumeDb,
        DataVolumeDb,
        ExportVolumeDb,
        RuntimeDb,
        MachineVolumeBaseDb,
    )

    for table in volume_tables:
        session.execute(delete(table).where(table.program_hash == item_hash))  # type: ignore[attr-defined]

    session.execute(delete(ProgramDb).where(ProgramDb.item_hash == item_hash))
