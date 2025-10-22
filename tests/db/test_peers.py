import datetime as dt

import pytest
import pytz
from sqlalchemy import select

from aleph.db.accessors.peers import get_all_addresses_by_peer_type, upsert_peer
from aleph.db.models.peers import PeerDb, PeerType
from aleph.types.db_session import DbSessionFactory


@pytest.mark.asyncio
async def test_get_all_addresses_by_peer_type(session_factory: DbSessionFactory):
    peer_id = "some-peer-id"
    last_seen = pytz.utc.localize(dt.datetime(2022, 10, 1))
    source = PeerType.P2P

    http_entry = PeerDb(
        peer_id=peer_id,
        peer_type=PeerType.HTTP,
        address="http://127.0.0.1:4024",
        source=source,
        last_seen=last_seen,
    )
    p2p_entry = PeerDb(
        peer_id=peer_id,
        peer_type=PeerType.P2P,
        address="/ip4/127.0.0.1/tcp/4025",
        source=source,
        last_seen=last_seen,
    )

    ipfs_entry = PeerDb(
        peer_id=peer_id,
        peer_type=PeerType.IPFS,
        address="http://127.0.0.1:4001",
        source=source,
        last_seen=last_seen,
    )

    with session_factory() as session:
        session.add_all([http_entry, p2p_entry, ipfs_entry])
        session.commit()

    with session_factory() as session:
        http_entries = get_all_addresses_by_peer_type(
            session=session, peer_type=PeerType.HTTP
        )
        p2p_entries = get_all_addresses_by_peer_type(
            session=session, peer_type=PeerType.P2P
        )

        ipfs_entries = get_all_addresses_by_peer_type(
            session=session, peer_type=PeerType.IPFS
        )

    assert http_entries == [http_entry.address]
    assert p2p_entries == [p2p_entry.address]
    assert ipfs_entries == [ipfs_entry.address]

    with session_factory() as session:
        recent_p2p_entries = get_all_addresses_by_peer_type(
            session=session, peer_type=PeerType.P2P, last_seen=last_seen
        )
        old_p2p_entries = get_all_addresses_by_peer_type(
            session=session,
            peer_type=PeerType.P2P,
            last_seen=last_seen + dt.timedelta(days=1),
        )
        assert recent_p2p_entries == [p2p_entry.address]
        assert old_p2p_entries == []


@pytest.mark.asyncio
@pytest.mark.parametrize("peer_type", (PeerType.HTTP, PeerType.P2P, PeerType.IPFS))
async def test_get_all_addresses_by_peer_type_no_match(
    session_factory: DbSessionFactory, peer_type: PeerType
):
    with session_factory() as session:
        entries = get_all_addresses_by_peer_type(session=session, peer_type=peer_type)

    assert entries == []


@pytest.mark.asyncio
async def test_upsert_peer_insert(session_factory: DbSessionFactory):
    peer_id = "peer-id"
    peer_type = PeerType.HTTP
    address = "http://127.0.0.1:4024"
    source = PeerType.IPFS
    last_seen = pytz.utc.localize(dt.datetime(2022, 10, 1))

    with session_factory() as session:
        upsert_peer(
            session=session,
            peer_id=peer_id,
            address=address,
            peer_type=peer_type,
            source=source,
            last_seen=last_seen,
        )
        session.commit()

    with session_factory() as session:
        peer = (
            (
                session.execute(
                    select(PeerDb).where(
                        (PeerDb.peer_id == peer_id) & (PeerDb.peer_type == peer_type)
                    )
                )
            )
            .scalars()
            .one()
        )

    assert peer.peer_id == peer_id
    assert peer.peer_type == peer_type
    assert peer.address == address
    assert peer.source == source
    assert peer.last_seen == last_seen


@pytest.mark.asyncio
async def test_upsert_peer_replace(session_factory: DbSessionFactory):
    peer_id = "peer-id"
    peer_type = PeerType.HTTP
    address = "http://127.0.0.1:4024"
    source = PeerType.P2P
    last_seen = pytz.utc.localize(dt.datetime(2022, 10, 1))

    with session_factory() as session:
        upsert_peer(
            session=session,
            peer_id=peer_id,
            peer_type=peer_type,
            address=address,
            source=source,
            last_seen=last_seen,
        )
        session.commit()

    new_address = "http://0.0.0.0:4024"
    new_source = PeerType.IPFS
    new_last_seen = pytz.utc.localize(dt.datetime(2022, 10, 2))

    with session_factory() as session:
        upsert_peer(
            session=session,
            peer_id=peer_id,
            peer_type=peer_type,
            address=new_address,
            source=new_source,
            last_seen=new_last_seen,
        )
        session.commit()

    with session_factory() as session:
        peer = (
            (
                session.execute(
                    select(PeerDb).where(
                        (PeerDb.peer_id == peer_id) & (PeerDb.peer_type == peer_type)
                    )
                )
            )
            .scalars()
            .one()
        )

    assert peer.peer_id == peer_id
    assert peer.peer_type == peer_type
    assert peer.address == new_address
    assert peer.source == new_source
    assert peer.last_seen == new_last_seen
