import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from aleph.db.models import AggregateDb
from aleph.types.db_session import DbSession

logger = logging.getLogger(__name__)


def get_granted_authorizations(
    session: DbSession,
    owner: str,
) -> Optional[Dict[str, Any]]:
    """Get the security aggregate content for an owner (forward lookup).

    Returns the raw security aggregate content dict, or None if no
    security aggregate exists for the owner.
    """
    select_stmt = select(AggregateDb.content).where(
        (AggregateDb.key == "security") & (AggregateDb.owner == owner)
    )
    result = session.execute(select_stmt).scalar()
    if result is None:
        return None
    return result


def get_received_authorizations(
    session: DbSession,
    address: str,
) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Reverse lookup: find all security aggregates that grant permissions to address.

    Uses the GIN index on content->'authorizations' for efficient containment query.

    Returns a list of (owner, matching_authorizations) tuples where
    matching_authorizations contains only the entries for the target address.
    """
    select_stmt = (
        select(AggregateDb.owner, AggregateDb.content)
        .where(
            (AggregateDb.key == "security")
            & AggregateDb.content["authorizations"].contains([{"address": address}])
        )
        .order_by(AggregateDb.owner)
    )
    rows = session.execute(select_stmt).all()

    results = []
    for owner, content in rows:
        all_auths = content.get("authorizations", [])
        # Filter to matching entries and strip the redundant 'address' field
        matching = [
            {k: v for k, v in auth.items() if k != "address"}
            for auth in all_auths
            if auth.get("address") == address
        ]
        if matching:
            results.append((owner, matching))

    return results


def filter_authorizations(
    grouped_auths: Dict[str, List[Dict[str, Any]]],
    *,
    channels: Optional[List[str]] = None,
    types: Optional[List[str]] = None,
    post_types: Optional[List[str]] = None,
    chains: Optional[List[str]] = None,
    aggregate_keys: Optional[List[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Filter authorization entries and remove addresses with no remaining entries.

    Filtering logic mirrors the permission model: if a filter field is not set
    on an authorization entry, the entry is unrestricted for that dimension and
    matches any filter value.
    """

    def _entry_matches(entry: Dict[str, Any]) -> bool:
        if channels:
            entry_channels = entry.get("channels", [])
            if entry_channels and not set(entry_channels) & set(channels):
                return False

        if types:
            entry_types = entry.get("types", [])
            if entry_types and not set(entry_types) & set(types):
                return False

        if post_types:
            entry_post_types = entry.get("post_types", [])
            if entry_post_types and not set(entry_post_types) & set(post_types):
                return False

        if chains:
            entry_chain = entry.get("chain")
            if entry_chain and entry_chain not in chains:
                return False

        if aggregate_keys:
            entry_akeys = entry.get("aggregate_keys", [])
            if entry_akeys and not set(entry_akeys) & set(aggregate_keys):
                return False

        return True

    result = {}
    for address, entries in grouped_auths.items():
        filtered = [e for e in entries if _entry_matches(e)]
        if filtered:
            result[address] = filtered

    return result


def paginate_authorizations(
    grouped_auths: Dict[str, List[Dict[str, Any]]],
    page: int,
    pagination: int,
) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    """Paginate authorization results by address.

    Returns (paginated_dict, total_count) where total_count is the number
    of distinct addresses before pagination.
    """
    total = len(grouped_auths)
    keys = list(grouped_auths.keys())
    start = (page - 1) * pagination
    end = start + pagination
    page_keys = keys[start:end]
    result = {k: grouped_auths[k] for k in page_keys}
    return result, total
