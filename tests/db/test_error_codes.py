import pytest
from sqlalchemy import select

from aleph.db.models import ErrorCodeDb
from aleph.types.db_session import AsyncDbSessionFactory
from aleph.types.message_status import ErrorCode


@pytest.mark.asyncio
async def test_all_error_codes_mapped_in_db(session_factory: AsyncDbSessionFactory):
    """
    Check that the ErrorCode enum values are all mapped in the database and vice-versa.
    Sanity check for developers.
    """

    async with session_factory() as session:
        db_error_codes = (await session.execute(select(ErrorCodeDb))).scalars()
        db_error_codes_dict = {e.code: e for e in db_error_codes}

    # All error code enum values must be mapped in the DB
    for error_code in ErrorCode:
        assert error_code.value in db_error_codes_dict

    # All DB entries must be mapped in the error code enum
    for db_error_code in db_error_codes_dict.keys():
        _ = ErrorCode(db_error_code)
