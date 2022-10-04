from typing import AsyncContextManager, Callable, ContextManager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing_extensions import TypeAlias

DbSession: TypeAlias = Session
DbSessionFactory = Callable[[], ContextManager[DbSession]]

AsyncDbSession: TypeAlias = AsyncSession
AsyncDbSessionFactory = Callable[[], AsyncContextManager[AsyncDbSession]]
