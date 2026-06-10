import datetime as dt

import pytest

from aleph.db.accessors.files import upsert_file
from aleph.db.accessors.ipns import upsert_ipns_record
from aleph.types.db_session import DbSessionFactory
from aleph.types.files import FileType
from aleph.types.ipns import IpnsStatus

# A valid IPNS name (62-char base36 k51... CIDv1 key)
IPNS_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v8"
# Same length, last char changed -- still a syntactically valid-looking key but
# not registered in the DB (used for the 404 test).
UNREGISTERED_NAME = "k51qzi5uqu5dlvj2baxnqndepeb86cbk3ng7n3i46uzyxzyqj2xjonzllnv0v7"

OWNER = "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874"
CID = "QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB"
MSG_HASH = "7f2d09b2c4e1a8f3d6b5c2e9a1f4d7b8e3c6a9f2d5b8e1c4a7f0d3b6e9c2a5f8"
NOW = dt.datetime(2026, 6, 10, tzinfo=dt.timezone.utc)


@pytest.fixture
def seeded_ipns(session_factory: DbSessionFactory):
    with session_factory() as session:
        upsert_file(session, file_hash=CID, size=1024, file_type=FileType.FILE)
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME,
            owner=OWNER,
            item_hash=MSG_HASH,
            record=b"\x0a\x01raw",
            record_sequence=42,
            record_validity=NOW + dt.timedelta(days=365),
            max_size_mib=100,
            resolved_cid=CID,
            last_resolved=NOW,
            status=IpnsStatus.OK,
            created=NOW,
        )
        session.commit()


@pytest.mark.asyncio
async def test_get_ipns_name_registered(
    ccn_api_client,
    session_factory: DbSessionFactory,
    seeded_ipns,
):
    """GET /api/v0/ipns/{name} returns 200 with correct JSON for a registered name."""
    response = await ccn_api_client.get(f"/api/v0/ipns/{IPNS_NAME}")
    assert response.status == 200
    data = await response.json()

    assert data["name"] == IPNS_NAME
    assert data["resolved_cid"] == CID
    assert data["sequence"] == 42
    assert data["validity"] is not None
    assert data["status"] == IpnsStatus.OK.value

    registrations = data["registrations"]
    assert len(registrations) == 1
    reg = registrations[0]
    assert reg["owner"] == OWNER
    assert reg["max_size_mib"] == 100
    assert reg["status"] == IpnsStatus.OK.value


@pytest.mark.asyncio
async def test_get_ipns_name_not_registered(
    ccn_api_client,
    session_factory: DbSessionFactory,
):
    """GET /api/v0/ipns/{name} with a valid-but-unregistered name returns 404."""
    response = await ccn_api_client.get(f"/api/v0/ipns/{UNREGISTERED_NAME}")
    assert response.status == 404


@pytest.mark.asyncio
async def test_get_ipns_name_invalid(
    ccn_api_client,
):
    """GET /api/v0/ipns/{name} with a non-IPNS name returns 422."""
    response = await ccn_api_client.get("/api/v0/ipns/not-a-name")
    assert response.status == 422


@pytest.mark.asyncio
async def test_get_ipns_raw_redirect(
    ccn_api_client,
    session_factory: DbSessionFactory,
    seeded_ipns,
):
    """GET /api/v0/ipns/{name}/raw returns 302 to /api/v0/storage/raw/{cid}."""
    response = await ccn_api_client.get(
        f"/api/v0/ipns/{IPNS_NAME}/raw", allow_redirects=False
    )
    assert response.status == 302
    assert response.headers["Location"] == f"/api/v0/storage/raw/{CID}"


@pytest.mark.asyncio
async def test_get_ipns_raw_no_resolved_cid(
    ccn_api_client,
    session_factory: DbSessionFactory,
):
    """GET /api/v0/ipns/{name}/raw returns 404 when resolved_cid is None."""
    with session_factory() as session:
        upsert_ipns_record(
            session=session,
            name=IPNS_NAME,
            owner=OWNER,
            item_hash=MSG_HASH,
            record=b"\x0a\x01raw",
            record_sequence=42,
            record_validity=NOW + dt.timedelta(days=365),
            max_size_mib=100,
            resolved_cid=None,
            last_resolved=None,
            status=IpnsStatus.OK,
            created=NOW,
        )
        session.commit()

    response = await ccn_api_client.get(f"/api/v0/ipns/{IPNS_NAME}/raw")
    assert response.status == 404


@pytest.mark.asyncio
async def test_list_ipns_by_address(
    ccn_api_client,
    session_factory: DbSessionFactory,
    seeded_ipns,
):
    """GET /api/v0/addresses/{address}/ipns lists seeded registrations for the owner."""
    response = await ccn_api_client.get(f"/api/v0/addresses/{OWNER}/ipns")
    assert response.status == 200
    data = await response.json()

    assert data["address"] == OWNER
    registrations = data["registrations"]
    assert len(registrations) == 1
    reg = registrations[0]
    assert reg["name"] == IPNS_NAME
    assert reg["owner"] == OWNER
    assert reg["max_size_mib"] == 100
    assert reg["status"] == IpnsStatus.OK.value
