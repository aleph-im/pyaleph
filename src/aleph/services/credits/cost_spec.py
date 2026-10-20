from typing import Any, Dict, List, Optional, Tuple

from aleph_message.models import (
    InstanceContent,
    MessageType,
    ProgramContent,
    StoreContent,
    VerifiableProgramContent,
)
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.accessors.files import get_file
from aleph.db.models import MessageDb
from aleph.services.cost import _get_file_from_ref
from aleph.types.db_session import DbSession

# Message types whose costs are billed per period from a billing_resources row.
BILLABLE_MESSAGE_TYPES = (
    MessageType.instance,
    MessageType.program,
    MessageType.v_program,
    MessageType.store,
)


def _referenced_volumes(content: Any) -> List[Tuple[str, bool]]:
    """(ref, use_latest) of every reference volume the cost code resolves."""
    refs: List[Tuple[str, bool]] = []

    if isinstance(content, InstanceContent):
        refs.append((content.rootfs.parent.ref, content.rootfs.parent.use_latest))
    elif isinstance(content, ProgramContent):
        refs.append((content.code.ref, content.code.use_latest))
        refs.append((content.runtime.ref, content.runtime.use_latest))
        if content.data and content.data.ref is not None:
            refs.append((content.data.ref, content.data.use_latest or False))
    elif isinstance(content, VerifiableProgramContent):
        refs.append((str(content.workload.ref), False))
        refs.append((str(content.workload.hash_tree), False))

    for volume in getattr(content, "volumes", []):
        if isinstance(volume, ImmutableVolume):
            if volume.ref is not None:
                refs.append((volume.ref, volume.use_latest))
        elif hasattr(volume, "hash_tree"):
            refs.append((str(volume.ref), False))
            refs.append((str(volume.hash_tree), False))

    return refs


def build_cost_spec(session: DbSession, message: MessageDb) -> Dict[str, Any]:
    """
    Snapshot of everything needed to price a message later: its content and the
    exact size in bytes (files.size) of the files it references, as resolved now.
    """
    content = message.parsed_content
    volume_sizes_bytes: Dict[str, int] = {}
    file_size_bytes: Optional[int] = None

    if isinstance(content, StoreContent):
        stored_file = get_file(session, content.item_hash)
        file_size_bytes = None if stored_file is None else stored_file.size
    else:
        for ref, use_latest in _referenced_volumes(content):
            file = _get_file_from_ref(session=session, ref=ref, use_latest=use_latest)
            if file is not None:
                volume_sizes_bytes[ref] = file.size

    return {
        "type": MessageType(message.type).value,
        "content": message.content,
        "volume_sizes_bytes": volume_sizes_bytes,
        "file_size_bytes": file_size_bytes,
    }
