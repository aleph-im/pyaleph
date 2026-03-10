import logging
from typing import Any, Dict, List, Optional

from aiohttp import web
from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import select

from aleph.db.accessors.aggregates import refresh_aggregate
from aleph.db.accessors.authorizations import (
    filter_authorizations,
    get_granted_authorizations,
    get_received_authorizations,
    paginate_authorizations,
)
from aleph.db.models import AggregateDb
from aleph.schemas.messages_query_params import LIST_FIELD_SEPARATOR
from aleph.web.controllers.app_state_getters import get_session_factory_from_request

LOGGER = logging.getLogger(__name__)


class AuthorizationsQueryParams(BaseModel):
    channels: Optional[List[str]] = None
    types: Optional[List[str]] = None
    post_types: Optional[List[str]] = Field(default=None, alias="postTypes")
    chains: Optional[List[str]] = None
    aggregate_keys: Optional[List[str]] = Field(default=None, alias="aggregateKeys")
    pagination: int = Field(default=20, ge=1, le=500, alias="pagination")
    page: int = Field(default=1, ge=1, alias="page")

    model_config = {"populate_by_name": True}

    @field_validator(
        "channels", "types", "post_types", "chains", "aggregate_keys", mode="before"
    )
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v


class GrantedQueryParams(AuthorizationsQueryParams):
    grantee: Optional[str] = None


class ReceivedQueryParams(AuthorizationsQueryParams):
    granter: Optional[str] = None


def _build_grouped_from_content(
    content: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group authorization entries by their 'address' field.

    The 'address' key is stripped from entries since it's redundant
    with the grouping key.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for auth in content.get("authorizations", []):
        addr = auth.get("address", "")
        if addr:
            entry = {k: v for k, v in auth.items() if k != "address"}
            grouped.setdefault(addr, []).append(entry)
    return grouped


async def view_granted_authorizations(request: web.Request) -> web.Response:
    address: str = request.match_info["address"]

    try:
        query_params = GrantedQueryParams.model_validate(dict(request.query))
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        # Refresh dirty security aggregate for this address
        dirty = session.execute(
            select(AggregateDb.key).where(
                (AggregateDb.owner == address)
                & (AggregateDb.key == "security")
                & AggregateDb.dirty
            )
        ).scalar()
        if dirty:
            LOGGER.info("Refreshing dirty security aggregate for %s", address)
            refresh_aggregate(session=session, owner=address, key="security")
            session.commit()

        content = get_granted_authorizations(session=session, owner=address)

    if content is None:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
    else:
        grouped = _build_grouped_from_content(content)

    # Apply grantee filter
    if query_params.grantee:
        grouped = {k: v for k, v in grouped.items() if k == query_params.grantee}

    # Apply authorization filters
    grouped = filter_authorizations(
        grouped,
        channels=query_params.channels,
        types=query_params.types,
        post_types=query_params.post_types,
        chains=query_params.chains,
        aggregate_keys=query_params.aggregate_keys,
    )

    # Paginate
    paginated, total = paginate_authorizations(
        grouped, page=query_params.page, pagination=query_params.pagination
    )

    return web.json_response(
        {
            "authorizations": paginated,
            "pagination_page": query_params.page,
            "pagination_per_page": query_params.pagination,
            "pagination_total": total,
            "pagination_item": "authorizations",
            "address": address,
        }
    )


async def view_received_authorizations(request: web.Request) -> web.Response:
    address: str = request.match_info["address"]

    try:
        query_params = ReceivedQueryParams.model_validate(dict(request.query))
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(
            text=e.json(), content_type="application/json"
        )

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        results = get_received_authorizations(session=session, address=address)

    # Build grouped dict: {granter_address: [auth_entries]}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for owner, auths in results:
        grouped[owner] = auths

    # Apply granter filter
    if query_params.granter:
        grouped = {k: v for k, v in grouped.items() if k == query_params.granter}

    # Apply authorization filters
    grouped = filter_authorizations(
        grouped,
        channels=query_params.channels,
        types=query_params.types,
        post_types=query_params.post_types,
        chains=query_params.chains,
        aggregate_keys=query_params.aggregate_keys,
    )

    # Paginate
    paginated, total = paginate_authorizations(
        grouped, page=query_params.page, pagination=query_params.pagination
    )

    return web.json_response(
        {
            "authorizations": paginated,
            "pagination_page": query_params.page,
            "pagination_per_page": query_params.pagination,
            "pagination_total": total,
            "pagination_item": "authorizations",
            "address": address,
        }
    )
