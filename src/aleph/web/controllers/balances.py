from aiohttp import web

from aleph.db.accessors.balances import get_total_balance
from aleph.schemas.api.balances import GetBalanceResponse
from aleph.types.db_session import DbSessionFactory


def get_address_balance(request: web.Request):
    address = request.match_info.get("address")
    if address is None:
        raise web.HTTPUnprocessableEntity(body="Address must be specified.")

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        balance = get_total_balance(
            session=session, address=address, include_dapps=False
        )

    if balance is None:
        raise web.HTTPNotFound()

    return web.json_response(
        text=GetBalanceResponse(address=address, balance=balance).json()
    )
