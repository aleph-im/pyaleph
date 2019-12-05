from aleph.model.base import BaseClass
from aleph.network import INCOMING_MESSAGE_AUTHORIZED_FIELDS
from pymongo import ASCENDING, DESCENDING, IndexModel
# from aleph import LOGGER

RAW_MSG_PROJECTION = {field: 1 for field
                      in INCOMING_MESSAGE_AUTHORIZED_FIELDS}
RAW_MSG_PROJECTION.update({'_id': 0})


class Message(BaseClass):
    COLLECTION = "messages"
    INDEXES = [  # Index("hash", unique=True),
               IndexModel([("item_hash", ASCENDING),
                           ("chain", ASCENDING),
                           ("sender", ASCENDING),
                           ("type", ASCENDING)], unique=True),
               IndexModel([("item_hash", ASCENDING)]),  # Content IPFS hash
               IndexModel([("tx_hash", ASCENDING)]),  # TX Hash (if there is one)
               IndexModel([("sender", ASCENDING)]),
               IndexModel([("content.address", ASCENDING)]),
               IndexModel([("content.key", ASCENDING)]),
               IndexModel([("content.ref", ASCENDING)]),
               IndexModel([("content.type", ASCENDING)]),
               IndexModel([("content.content.tags", ASCENDING)]),
            #    IndexModel([("content.time", ASCENDING)]),
               IndexModel([("time", DESCENDING)]),
               IndexModel([("time", ASCENDING)]),
               IndexModel([("type", ASCENDING)]),
               IndexModel([("type", ASCENDING),
                           ("content.address", ASCENDING),
                           ("time", DESCENDING)]),
               IndexModel([("type", ASCENDING),
                           ("content.address", ASCENDING),
                           ("content.key", ASCENDING),
                           ("time", DESCENDING)]),
            #    IndexModel([("chain", ASCENDING)]),
            #    IndexModel([("confirmations.chain", ASCENDING)]),
            #    IndexModel([("confirmations.height", ASCENDING)]),
            #    IndexModel([("confirmations.height", DESCENDING)]),
               IndexModel([("confirmed", DESCENDING)])]

    @classmethod
    async def get_unconfirmed_raw(cls, limit=100, for_chain=None):
        """ Return raw unconfirmed txs, ready for broadcast.
        """
        if for_chain is None:
            return cls.collection.find(
                {'confirmed': False},
                projection=RAW_MSG_PROJECTION).sort([('time', 1)]).limit(limit)
        else:
            return cls.collection.find(
                {'confirmations.chain': {'$ne': for_chain},
                 'tx_hash': {"$exists": False}},  # tx_hash means chain native
                projection=RAW_MSG_PROJECTION).sort([('time', 1)]).limit(limit)


async def get_computed_address_aggregates(address_list=None, key_list=None, limit=100):
    aggregate = [
        {'$match': {
            'type': 'AGGREGATE'
        }},
        {'$sort': {'time': -1}},
        {'$limit': limit},
        {'$match': {
            'content.content': {'$type': 3}
        }},        
        {'$project': {
            'time': 1,
            'content.address': 1,
            'content.key': 1,
            'content.content': 1
        }},
        {'$sort': {'time': 1}},
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
        if len(address_list) > 1:
            aggregate[0]['$match']['content.address'] = {'$in': address_list}
        else:
            aggregate[0]['$match']['content.address'] = address_list[0]

    if key_list is not None:
        if len(key_list) > 1:
            aggregate[0]['$match']['content.key'] = {'$in': key_list}
        else:
            aggregate[0]['$match']['content.key'] = key_list[0]

    results = Message.collection.aggregate(aggregate)

    return {result['address']: result['contents']
            async for result in results}


async def get_merged_posts(filters, sort=None, limit=100,
                           skip=0, amend_limit=1):
    if sort is None:
        sort = {'confirmed': 1,
                'time': -1,
                'confirmations.height': -1}

    aggregate = [
        {'$match': {
            'type': 'POST',
            **filters
        }},
        {'$sort': sort},
        {'$skip': skip},
        {'$limit': limit},
        {'$addFields': {
            'original_item_hash': '$item_hash',
            'original_signature': '$signature',
            'original_tx_hash': '$tx_hash',
            'hash': '$item_hash',
            'ref': '$content.content.ref'
        }},
        {'$lookup': {
            'from': 'messages',
            'let': {'item_hash': "$item_hash",
                    'tx_hash': "$tx_hash",
                    'address': '$content.address'},
            'pipeline': [
                {'$match': {
                    '$and': [
                        {'type': 'POST'},
                        {'content.type': 'amend'},
                        # {'content.ref': {'$in': ['$$item_hash',
                        #                         '$$tx_hash']}}
                        {'$expr':
                            {'$and': [
                                {'$eq': ['$content.ref', '$$item_hash']},
                                {'$eq': ['$content.address', '$$address']}
                            ]}
                         }
                    ]
                }},
                {'$sort': {'confirmed': 1,
                           'confirmations.height': -1,
                           'time': -1}},
                {'$limit': amend_limit}
            ],
            'as': 'amends'
        }},
        {'$replaceRoot': {
            'newRoot': {'$mergeObjects': ["$$ROOT",
                                          {'$arrayElemAt': ["$amends", 0]}]}}},
        {'$project': {'amends': 0}},
        {'$replaceRoot': {
            'newRoot': {'$mergeObjects': ["$$ROOT", "$content"]}}}
    ]

    return Message.collection.aggregate(aggregate)
