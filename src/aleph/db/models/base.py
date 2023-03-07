from typing import Dict, Any, Optional, Set, List, Iterable

from sqlalchemy import Table, exists, text, Column, func, select
from sqlalchemy.orm import declarative_base

from aleph.types.db_session import DbSession


class AugmentedBase:
    __tablename__: str
    __table__: Table

    def to_dict(self, exclude: Optional[Set[str]] = None) -> Dict[str, Any]:
        exclude_set = exclude if exclude is not None else set()

        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
            if column.name not in exclude_set
        }

    @classmethod
    def count(cls, session: DbSession) -> int:
        return (
            session.execute(text(f"SELECT COUNT(*) FROM {cls.__tablename__}"))
        ).scalar_one()

    @classmethod
    def estimate_count(cls, session: DbSession) -> int:
        """
        Returns an approximation of the number of rows in a table.

        SELECT COUNT(*) can be quite slow. There are techniques to retrieve an
        approximation of the number of rows in a table that are much faster.
        Refer to https://wiki.postgresql.org/wiki/Count_estimate for an explanation.

        :param session: DB session.
        :return: The approximate number of rows in a table. Can be -1 if the table
                 has never been analyzed or vacuumed.
        """

        return session.execute(
            text(
                f"SELECT reltuples::bigint FROM pg_class WHERE relname = '{cls.__tablename__}'"
            )
        ).scalar_one()

    @classmethod
    def fast_count(cls, session: DbSession) -> int:
        """
        :param session: DB session.
        :return: The estimate count of the table if available from pg_class, otherwise
                 the real count of rows.
        """
        estimate_count = cls.estimate_count(session)
        if estimate_count == -1:
            return cls.count(session)

        return estimate_count

    # TODO: set type of "where" to the SQLA boolean expression class
    @classmethod
    def exists(cls, session: DbSession, where) -> bool:

        exists_stmt = exists(text("1")).select().where(where)
        result = (session.execute(exists_stmt)).scalar()
        return result is not None

    @classmethod
    def jsonb_keys(cls, session: DbSession, column: Column, where) -> Iterable[str]:
        select_stmt = select(func.jsonb_object_keys(column)).where(where)
        return session.execute(select_stmt).scalars()


Base = declarative_base(cls=AugmentedBase)
