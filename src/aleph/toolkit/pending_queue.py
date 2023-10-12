import logging
from typing import Optional, Generic, TypeVar, Type

import aio_pika
from sqlalchemy import select

from aleph.db.models import Base
from aleph.types.db_session import DbSession

LOGGER = logging.getLogger(__name__)


T = TypeVar("T", bound=Base)


class PendingQueuePublisher(Generic[T]):
    def __init__(
        self,
        mq_exchange: aio_pika.abc.AbstractRobustExchange,
        session: DbSession,
        db_model: Type[T],
        id_key: str,
    ):
        self.mq_exchange = mq_exchange
        self.session = session
        self.db_model = db_model
        self.id_key = id_key

    async def add(self, db_obj: T) -> None:
        self.session.add(db_obj)
        self.session.commit()

        obj_id = str(getattr(db_obj, self.id_key))
        message = aio_pika.Message(body=obj_id.encode("utf-8"))
        await self.mq_exchange.publish(message=message, routing_key=obj_id)


class PendingQueueConsumer(Generic[T]):
    """
    A pending queue consumer that reads object IDs from a RabbitMQ message queue and then fetches
    the corresponding ID from the database.
    """

    def __init__(
        self,
        mq_queue: aio_pika.abc.AbstractRobustQueue,
        session: DbSession,
        db_model: Type[T],
        id_key: str,
        where: Optional = None,
    ):
        self.mq_queue = mq_queue
        self.session = session
        self.db_model = db_model
        self.id_key = id_key
        self.where = where

    def _get_db_obj(self, db_id: str) -> Optional[T]:
        return self.session.execute(
            select(self.db_model).where(getattr(self.db_model, self.id_key) == db_id)
        ).scalar_one_or_none()


    async def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.mq_queue.get(fail=False)
        if message is None:
            raise StopAsyncIteration

        await message.ack()

        db_id = message.body.decode("utf-8")
        db_obj = self._get_db_obj(db_id=db_id)
        if db_obj:
            yield db_obj
        else:
            LOGGER.warning(
                "Missing %s object in DB: %s. Skipping...",
                self.db_model.__class__.__name__,
                db_id,
            )
