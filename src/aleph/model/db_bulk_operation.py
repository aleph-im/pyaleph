from .base import BaseClass
from typing import Type, Union
from dataclasses import dataclass
from pymongo import DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne


MongoOperation = Union[DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne]


@dataclass
class DbBulkOperation:
    collection: Type[BaseClass]
    operation: MongoOperation
