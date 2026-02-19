from typing import Any, Dict, Iterable, Optional, Set, Union

from sqlalchemy import Column, func, inspect, select, text
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute
from sqlalchemy.sql import exists as sql_exists

from aleph.types.db_session import DbSession


class Base(DeclarativeBase):
    """
    Augmented declarative base providing utility methods for all models.
    """

    @classmethod
    def _col_name_to_attr(cls) -> Dict[str, str]:
        """Build mapping from DB column names to Python attribute names.
        Cached on the class after first call."""
        cache_attr = "_col_attr_map"
        if not hasattr(cls, cache_attr) or getattr(cls, cache_attr) is None:
            mapper = inspect(cls)
            mapping = {}
            for prop in mapper.column_attrs:
                for col in prop.columns:
                    mapping[col.name] = prop.key
            setattr(cls, cache_attr, mapping)
        return getattr(cls, cache_attr)

    def to_dict(self, exclude: Optional[Set[str]] = None) -> Dict[str, Any]:
        exclude_set = exclude if exclude is not None else set()
        col_map = self._col_name_to_attr()

        return {
            column.name: getattr(self, col_map.get(column.name, column.name))
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
        exists_stmt = sql_exists(text("1")).select().where(where)
        result = (session.execute(exists_stmt)).scalar()
        return result is not None

    @classmethod
    def jsonb_keys(
        cls,
        session: DbSession,
        column: Union[Column[Any], InstrumentedAttribute[Any]],
        where,
    ) -> Iterable[str]:
        select_stmt = select(func.jsonb_object_keys(column)).where(where)
        return session.execute(select_stmt).scalars()
