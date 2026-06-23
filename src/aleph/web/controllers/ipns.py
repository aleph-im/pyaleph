"""
API endpoints for IPNS registrations.

Only names registered on Aleph (paid STORE messages with item_type=ipns)
are served: CCNs are not open IPNS resolvers. Resolution is a DB lookup
of the last good CID; content is served from already-pinned data through
the existing storage endpoints.
"""

from aiohttp import web
from aleph_message.models import ItemType

from aleph.db.accessors.ipns import get_ipns_records_by_name, get_ipns_records_by_owner
from aleph.db.models.ipns import IpnsRecordDb
from aleph.exceptions import UnknownHashError
from aleph.utils import item_type_from_hash
from aleph.web.controllers.app_state_getters import get_session_factory_from_request


def _validate_ipns_name(name: str) -> None:
    try:
        is_ipns = item_type_from_hash(name) == ItemType.ipns
    except UnknownHashError:
        is_ipns = False
    if not is_ipns:
        raise web.HTTPUnprocessableEntity(reason=f"Not an IPNS name: '{name}'")


def _registration_to_dict(record_db: IpnsRecordDb) -> dict:
    return {
        "owner": record_db.owner,
        "item_hash": record_db.item_hash,
        "max_size_mib": record_db.max_size_mib,
        "status": record_db.status.value,
        "sequence": record_db.record_sequence,
        "validity": (
            record_db.record_validity.isoformat() if record_db.record_validity else None
        ),
        "resolved_cid": record_db.resolved_cid,
        "last_resolved": (
            record_db.last_resolved.isoformat() if record_db.last_resolved else None
        ),
    }


def _best_registration(records):
    # Among multiple owners of the same name, serve the registration with
    # the highest record sequence; tie-break by owner for determinism.
    return max(records, key=lambda r: (r.record_sequence or 0, r.owner))


async def get_ipns_name(request: web.Request) -> web.Response:
    name = request.match_info["name"]
    _validate_ipns_name(name)

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        records = list(get_ipns_records_by_name(session, name=name))
        if not records:
            raise web.HTTPNotFound(reason=f"IPNS name not registered: '{name}'")
        best = _best_registration(records)
        return web.json_response(
            {
                "name": name,
                "resolved_cid": best.resolved_cid,
                "sequence": best.record_sequence,
                "validity": (
                    best.record_validity.isoformat() if best.record_validity else None
                ),
                "status": best.status.value,
                "registrations": [_registration_to_dict(r) for r in records],
            }
        )


async def get_ipns_raw(request: web.Request) -> web.Response:
    name = request.match_info["name"]
    _validate_ipns_name(name)

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        records = list(get_ipns_records_by_name(session, name=name))
        if not records:
            raise web.HTTPNotFound(reason=f"IPNS name not registered: '{name}'")
        best = _best_registration(records)
        if best.resolved_cid is None:
            raise web.HTTPNotFound(
                reason=f"IPNS name has no resolved content yet: '{name}'"
            )
        raise web.HTTPFound(location=f"/api/v0/storage/raw/{best.resolved_cid}")


async def list_ipns_by_address(request: web.Request) -> web.Response:
    address = request.match_info["address"]
    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        records = list(get_ipns_records_by_owner(session, owner=address))
        return web.json_response(
            {
                "address": address,
                "registrations": [
                    {**_registration_to_dict(r), "name": r.name} for r in records
                ],
            }
        )
