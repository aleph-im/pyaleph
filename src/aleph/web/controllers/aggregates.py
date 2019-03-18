from aleph.web import app
from aleph.model.messages import get_computed_address_aggregates

from aiohttp import web


async def address_aggregate(request):
    """ Returns the aggregate of an address.
    TODO: handle filter on a single key, or even subkey.
    """

    address = request.match_info['address']
    aggregates = await get_computed_address_aggregates(address_list=[address])

    output = {
        'address': address,
        'data': aggregates.get(address, {})
    }
    return web.json_response(output)

app.router.add_get('/api/v0/aggregates/{address}.json', address_aggregate)
