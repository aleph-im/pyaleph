import math
from typing import Any, Dict, List, Optional, Tuple

from aleph_message.models import (
    InstanceContent,
    MessageType,
    ProgramContent,
    StoreContent,
    VerifiableProgramContent,
)
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.models import MessageDb
from aleph.services.cost import _get_file_from_ref, calculate_storage_size
from aleph.toolkit.constants import MiB
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


def _to_mib(size_bytes: int) -> int:
    return math.ceil(size_bytes / MiB)


def build_cost_spec(session: DbSession, message: MessageDb) -> Dict[str, Any]:
    """
    Snapshot of everything needed to price a message later: its content and the
    size of the files it references, as resolved now.
    """
    content = message.parsed_content
    volume_sizes_mib: Dict[str, int] = {}
    file_size_mib: Optional[int] = None

    if isinstance(content, StoreContent):
        size = calculate_storage_size(session, content)
        file_size_mib = None if size is None else math.ceil(size)
    else:
        for ref, use_latest in _referenced_volumes(content):
            file = _get_file_from_ref(session=session, ref=ref, use_latest=use_latest)
            if file is not None:
                volume_sizes_mib[ref] = _to_mib(file.size)

    return {
        "type": MessageType(message.type).value,
        "content": message.content,
        "volume_sizes_mib": volume_sizes_mib,
        "file_size_mib": file_size_mib,
    }
