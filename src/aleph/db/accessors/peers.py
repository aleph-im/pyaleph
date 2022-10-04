import datetime as dt
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession
from ..models.peers import PeerDb, PeerType


def get_all_addresses_by_peer_type(session: DbSession, peer_type: PeerType) -> Sequence[str]:
    select_peers_stmt = select(PeerDb.address).where(PeerDb.peer_type == peer_type)

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
