import datetime as dt
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession

from ..models.peers import PeerDb, PeerType


def get_all_addresses_by_peer_type(
    session: DbSession,
    peer_type: PeerType,
    last_seen: Optional[dt.datetime] = None,
) -> Sequence[str]:
    """
    Fetches all peer addresses filtered by peer type and optionally by the last_seen
    timestamp. This function retrieves addresses for peers of a specific type from
    the database. If a `last_seen` timestamp is provided, only peers with a `last_seen`
    timestamp greater than or equal to the provided value are considered.

    Arguments:
        session (DbSession): Database session for querying data.
        peer_type (PeerType): Type of peer to filter the addresses.
        last_seen (Optional[datetime.datetime], optional): Timestamp to filter peers
            last seen after or equal to this value. Defaults to None.

    Returns:
        Sequence[str]: List of addresses corresponding to the filtered peer type and
        optional last_seen timestamp.
    """

    select_peers_stmt = select(PeerDb.address).where(PeerDb.peer_type == peer_type)

    if last_seen is not None:
        select_peers_stmt = select_peers_stmt.where(PeerDb.last_seen >= last_seen)

    addresses = session.execute(select_peers_stmt)
    return addresses.scalars().all()


def upsert_peer(
    session: DbSession,
    peer_id: str,
    peer_type: PeerType,
    address: str,
    source: PeerType,
    last_seen: Optional[dt.datetime] = None,
) -> None:
    last_seen = last_seen or utc_now()

    upsert_stmt = (
        insert(PeerDb)
        .values(
            peer_id=peer_id,
            address=address,
            peer_type=peer_type,
            source=source,
            last_seen=last_seen,
        )
        .on_conflict_do_update(
            constraint="peers_pkey",
            set_={"address": address, "source": source, "last_seen": last_seen},
        )
    )
    session.execute(upsert_stmt)
