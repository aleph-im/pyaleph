from decimal import Decimal
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
from aleph.services.cost import (
    _get_file_from_ref,
    _get_product_price_type,
    get_detailed_costs,
)
from aleph.toolkit.constants import ProductPriceType
from aleph.toolkit.costs import format_cost
from aleph.types.cost import ProductPricing, resolve_price_type_key
from aleph.types.db_session import DbSession
from aleph.types.settings import Settings

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


_CONTENT_CLASSES: Dict[MessageType, Any] = {
    MessageType.instance: InstanceContent,
    MessageType.program: ProgramContent,
    MessageType.v_program: VerifiableProgramContent,
    MessageType.store: StoreContent,
}


def rate_from_spec(
    session: DbSession,
    spec: Dict[str, Any],
    item_hash: str,
    pricing_model: Dict[ProductPriceType, ProductPricing],
    settings: Settings,
) -> Decimal:
    """
    Credit cost per second of the message described by `spec`, under
    `pricing_model`, from the snapshotted content and file sizes only (the
    price aggregate and the file tables are not read).
    """
    content = _CONTENT_CLASSES[MessageType(spec["type"])].model_validate(
        spec["content"]
    )
    file_size_bytes = spec.get("file_size_bytes")
    if isinstance(content, StoreContent) and file_size_bytes is None:
        # The file was not known at snapshot time: nothing to bill.
        return Decimal(0)

    product_type = _get_product_price_type(content, settings, pricing_model)
    pricing_key = resolve_price_type_key(product_type, pricing_model.keys())
    pricing = pricing_model[pricing_key]
    if pricing.type != product_type:
        pricing = pricing.with_type(product_type)

    costs = get_detailed_costs(
        session,
        content,
        item_hash,
        pricing=pricing,
        settings=settings,
        pricing_model=pricing_model,
        volume_sizes_bytes=spec.get("volume_sizes_bytes") or {},
        file_size_bytes=file_size_bytes,
    )
    return format_cost(sum((c.cost_credit for c in costs), Decimal(0)))
