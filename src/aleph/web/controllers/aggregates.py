from aiohttp import web

from aleph.model.messages import get_computed_address_aggregates
from aleph.web import app


async def address_aggregate(request):
    """ Returns the aggregate of an address.
    TODO: handle filter on a single key, or even subkey.
    """

    address = request.match_info['address']

    keys = request.query.get('keys', None)
    if keys is not None:
        keys = keys.split(',')

    limit = request.query.get('limit', '1000')
    limit = int(limit)

    aggregates = await get_computed_address_aggregates(address_list=[address],
                                                       key_list=keys,
                                                       limit=limit)

    output = {
        'address': address,
        'data': aggregates.get(address, {})
    }
    return web.json_response(output)

app.router.add_get('/api/v0/aggregates/{address}.json', address_aggregate)