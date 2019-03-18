from aleph.model.base import BaseClass, Index
import pymongo
import logging
LOGGER = logging.getLogger('model.posts')


class Message(BaseClass):
    COLLECTION = "messages"

    INDEXES = [  # Index("hash", unique=True),
               Index("item_hash"),  # Content IPFS hash
               Index("sender"),
               Index("content.address"),
               Index("content.ref"),
               Index("content.type"),
               Index("content.tags"),
               Index("content.time"),
               Index("time", pymongo.DESCENDING),
               Index("time", pymongo.ASCENDING),
               Index("chain", pymongo.ASCENDING),
               Index("confirmations.chain", pymongo.ASCENDING),
               Index("confirmations.height", pymongo.ASCENDING),
               Index("confirmations.height", pymongo.DESCENDING),
               Index("confirmed")]


async def get_computed_address_aggregates(address_list=None, key_list=None):
    aggregate = [
        {'$match': {
            'type': 'AGGREGATE'
        }},
        {'$group': {
            '_id': {
                'address': '$content.address',
                'key': '$content.key'
                },
            'content': {
                '$mergeObjects': '$content.content'
            }
        }},
        {'$group': {
            '_id': '$_id.address',
            'items': {
                '$push': {
                    'k': '$_id.key',
                    'v': '$content'
                }
            }
        }},
        {'$addFields': {
            'address': '$_id',
            'contents': {
                '$arrayToObject': '$items'
            }
        }},
        {'$project': {
            '_id': 0,
            'address': 1,
            'contents': 1
        }}
    ]
    if address_list is not None:
        aggregate[0]['$match']['content.address'] = {'$in': address_list}

    if key_list is not None:
        aggregate[0]['$match']['content.key'] = {'$in': key_list}

    results = Message.collection.aggregate(aggregate)

    return {result['address']: result['contents']
            async for result in results}
