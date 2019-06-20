from aleph.web import app
from aleph.model.messages import Message

from aiocache import cached, SimpleMemoryCache
from aiohttp import web
from bson import json_util
import json
import datetime


# WARNING: we are storing this in memory... memcached or similar would
#          be better if volume starts to be too big.
@cached(ttl=60*120, cache=SimpleMemoryCache, timeout=120)
# 60*120 seconds or 2 hours minutes, 120 seconds timeout
async def addresses_stats(check_time=None, address_list=None,
                          output_collection=None):
    if check_time is None:
        check_time = datetime.datetime.now()

    matches = []

    if address_list is not None:
        if len(address_list) > 1:
            matches.append(
                {'$match': {'$or': [
                    # {'sender': {'$in': address_list}},
                    {'content.address': {'$in': address_list}}]
                    }})
        else:
            matches.append(
                {'$match': {'$or': [
                    # {'sender': address_list[0]},
                    {'content.address': address_list[0]}]
                    }})

    aggregate = Message.collection.aggregate(
        matches +
        [
         {'$group': {'_id': 'content.address',
                     'messages': {'$sum': 1},
                     'posts': {'$sum': {"$cond": [
                         {'type': 'POST'},
                         1, 0
                        ]}},
                     'aggregates': {'$sum': {"$cond": [
                         {'type': 'AGGREGATE'},
                         1, 0
                        ]}},
                     }},
         {'$project': {
                '_id': 0,
                'messages': 1,
                'posts': 1,
                'aggregates': 1,
                'address': '$_id'
            }},
         {'$sort': {'address': -1}}
         ], allowDiskUse=(address_list is None))
    items = [item async for item in aggregate]
    return items


@cached(ttl=60*10, cache=SimpleMemoryCache)  # 600 seconds or 10 minutes
async def addresses_infos(check_time=None, address_list=None):
    address_stats = await addresses_stats(check_time=None,
                                          address_list=address_list)
    return {info['address']: info
            for info in address_stats}


async def addresses_stats_view(request):
    """ Returns the stats of some addresses.
    """

    addresses = request.query.getall('addresses[]', [])
    check_time = None

    if len(addresses) and len(addresses) < 200:  # don't use cached values
        check_time = datetime.datetime.now()

    stats = await addresses_infos(address_list=addresses,
                                  check_time=check_time)

    output = {
        'data': stats
    }
    return web.json_response(output,
                             dumps=lambda v: json.dumps(
                                 v, default=json_util.default))


app.router.add_get('/api/v0/addresses/stats.json', addresses_stats_view)
