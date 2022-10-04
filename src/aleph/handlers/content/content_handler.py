import abc
from typing import List

from aleph.db.models import MessageDb
from aleph.permissions import check_sender_authorization
from aleph.types.db_session import DbSession


class ContentHandler(abc.ABC):
    async def is_related_content_fetched(
        self, session: DbSession, message: MessageDb
    ) -> bool:
        return True

    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        """
        Fetch additional content from the network based on the content of a message.

        The implementation is expected to be stateless in terms of DB operations.
        Other operations like storing a file on disk are allowed.

        Note: this function should only be overridden if the content field of
        a message can contain additional data to fetch. Most message types should
        keep the default implementation.
        """
        pass

    @abc.abstractmethod
    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        """
        Process several messages of the same type and applies the resulting changes.

        This function is in charge of:
        * checking permissions
        * applying DB updates.
        """
        pass

    async def check_dependencies(self, session: DbSession, message: MessageDb):
        pass

    async def check_permissions(self, session: DbSession, message: MessageDb):
        await check_sender_authorization(session=session, message=message)

    @abc.abstractmethod
    async def forget_message(self, session: DbSession, message: MessageDb):
        pass
