import datetime as dt
from typing import Optional, Sequence, Tuple

import pytest
import pytz
import sqlalchemy.orm.exc

from aleph.db.accessors.aggregates import (
    get_aggregate_by_key,
    get_aggregate_content_keys,
    refresh_aggregate,
)
from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import AsyncDbSessionFactory


@pytest.mark.asyncio
async def test_get_aggregate_by_key(session_factory: AsyncDbSessionFactory):
    key = "key"
    owner = "Me"
    creation_datetime = dt.datetime(2022, 1, 1)

    aggregate = AggregateDb(
        key=key,
        owner=owner,
        content={"a": 1, "b": 2},
        creation_datetime=creation_datetime,
        dirty=False,
        last_revision=AggregateElementDb(
            item_hash="1234",
            key=key,
            owner=owner,
            content={},
            creation_datetime=creation_datetime,
        ),
    )

    async with session_factory() as session:
        session.add(aggregate)
        await session.commit()

    async with session_factory() as session:
        aggregate_db = await get_aggregate_by_key(session=session, owner=owner, key=key)
        assert aggregate_db
        assert aggregate_db.key == key
        assert aggregate_db.owner == owner
        assert aggregate_db.content == aggregate.content
        assert aggregate_db.last_revision_hash == aggregate.last_revision.item_hash

    # Try not loading the content
    async with session_factory() as session:
        aggregate_db = await get_aggregate_by_key(
            session=session, owner=owner, key=key, with_content=False
        )

    assert aggregate_db
    with pytest.raises(sqlalchemy.orm.exc.DetachedInstanceError):
        _ = aggregate_db.content


@pytest.mark.asyncio
async def test_get_aggregate_by_key_no_data(session_factory: AsyncDbSessionFactory):
    async with session_factory() as session:
        aggregate = await get_aggregate_by_key(
            session=session, owner="owner", key="key"
        )

    assert aggregate is None


@pytest.fixture
def aggregate_fixtures() -> Tuple[AggregateDb, Sequence[AggregateElementDb]]:
    return AggregateDb(
        owner="me",
        key="key",
        content={"a": "aleph", "b": "batman", "c": "chianti"},
        last_revision_hash="4",
        creation_datetime=pytz.utc.localize(dt.datetime(2022, 1, 1)),
        dirty=False,
    ), [
        AggregateElementDb(
            item_hash="1",
            owner="me",
            key="key",
            content={"a": "alien"},
            creation_datetime=pytz.utc.localize(dt.datetime(2022, 1, 1)),
        ),
        AggregateElementDb(
            item_hash="2",
            owner="me",
            key="key",
            content={"b": "batman"},
            creation_datetime=pytz.utc.localize(dt.datetime(2022, 1, 2)),
        ),
        AggregateElementDb(
            item_hash="3",
            owner="me",
            key="key",
            content={"a": "aleph"},
            creation_datetime=pytz.utc.localize(dt.datetime(2022, 1, 3)),
        ),
        AggregateElementDb(
            item_hash="4",
            owner="me",
            key="key",
            content={"c": "chianti"},
            creation_datetime=pytz.utc.localize(dt.datetime(2022, 1, 4)),
        ),
    ]


async def _test_refresh_aggregate(
    session_factory: AsyncDbSessionFactory,
    aggregate: Optional[AggregateDb],
    expected_aggregate: AggregateDb,
    elements: Sequence[AggregateElementDb],
):
    async with session_factory() as session:
        session.add_all(elements)
        if aggregate:
            session.add(aggregate)
        await session.commit()

    async with session_factory() as session:
        await refresh_aggregate(
            session=session, owner=expected_aggregate.owner, key=expected_aggregate.key
        )
        await session.commit()

        aggregate_db = await get_aggregate_by_key(
            session=session, owner=expected_aggregate.owner, key=expected_aggregate.key
        )

    assert aggregate_db

    assert aggregate_db.owner == expected_aggregate.owner
    assert aggregate_db.key == expected_aggregate.key
    assert aggregate_db.creation_datetime == expected_aggregate.creation_datetime
    assert aggregate_db.last_revision_hash == expected_aggregate.last_revision_hash
    assert aggregate_db.content == expected_aggregate.content
    assert aggregate_db.dirty == expected_aggregate.dirty


@pytest.mark.asyncio
async def test_refresh_aggregate_insert(
    session_factory: AsyncDbSessionFactory,
    aggregate_fixtures: Tuple[AggregateDb, Sequence[AggregateElementDb]],
):
    aggregate, elements = aggregate_fixtures
    await _test_refresh_aggregate(
        session_factory=session_factory,
        aggregate=None,
        expected_aggregate=aggregate,
        elements=elements,
    )


@pytest.mark.asyncio
async def test_refresh_aggregate_update(
    session_factory: AsyncDbSessionFactory,
    aggregate_fixtures: Tuple[AggregateDb, Sequence[AggregateElementDb]],
):
    updated_aggregate, elements = aggregate_fixtures
    aggregate = AggregateDb(
        key=elements[0].key,
        owner=elements[0].owner,
        creation_datetime=elements[0].creation_datetime,
        content=elements[0].content,
        last_revision_hash=elements[0].item_hash,
        dirty=True,
    )
    await _test_refresh_aggregate(
        session_factory=session_factory,
        aggregate=aggregate,
        expected_aggregate=updated_aggregate,
        elements=elements,
    )


@pytest.mark.asyncio
async def test_refresh_aggregate_update_no_op(
    session_factory: AsyncDbSessionFactory,
    aggregate_fixtures: Tuple[AggregateDb, Sequence[AggregateElementDb]],
):
    aggregate, elements = aggregate_fixtures
    await _test_refresh_aggregate(
        session_factory=session_factory,
        aggregate=aggregate,
        expected_aggregate=aggregate,
        elements=elements,
    )


@pytest.mark.asyncio
async def test_get_content_keys(
    session_factory: AsyncDbSessionFactory,
    aggregate_fixtures: Tuple[AggregateDb, Sequence[AggregateElementDb]],
):
    aggregate, elements = aggregate_fixtures

    async with session_factory() as session:
        session.add_all(elements)
        session.add(aggregate)
        await session.commit()

    async with session_factory() as session:
        keys = set(
            await get_aggregate_content_keys(
                session=session, key=aggregate.key, owner=aggregate.owner
            )
        )
        assert keys == set(aggregate.content.keys())

    # Test no match
    async with session_factory() as session:
        keys = set(
            await get_aggregate_content_keys(
                session=session, key="not-a-key", owner="no-one"
            )
        )
        assert keys == set()
