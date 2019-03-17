""" 
"""

from aleph.model.base import BaseClass, Index
import logging
LOGGER = logging.getLogger('model.posts')


class Chain(BaseClass):
    """Holds information about the chains state."""
    COLLECTION = "chains"

    INDEXES = [Index("name")]

    @classmethod
    async def get_last_height(cls, chain):
        obj = await cls.collection.find_one(
            {'name': chain},
            projection={'last_commited_height': 1})
        return obj['last_commited_height']

    @classmethod
    async def set_last_height(cls, chain, height):
        await cls.collection.update({'name': chain},
                                    {'$currentDate': {
                                        'last_update': True
                                     },
                                     '$set': {"last_commited_height": height}
                                     },
                                    upsert=True)
