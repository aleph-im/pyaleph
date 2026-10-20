import copy
import datetime as dt
from decimal import Decimal

from aleph_message.models import MessageType, PaymentType
from services.test_cost_spec_rates import OWNER, ROOTFS_REF, instance_content
from sqlalchemy import select

from aleph.db.accessors.credit_ledger import get_billing_rate
from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.db.models.credit_ledger import BillingRateDb, BillingResourceDb
from aleph.services.credits import cost_spec
from aleph.services.credits.rates import rate_for
from aleph.services.pricing_utils import pricing_model_at
from aleph.toolkit.constants import (
    DEFAULT_PRICE_AGGREGATE,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    ProductPriceType,
)
from aleph.types.db_session import DbSessionFactory

FIRST = dt.datetime(2025, 1, 31, tzinfo=dt.timezone.utc)
SECOND = dt.datetime(2025, 6, 1, tzinfo=dt.timezone.utc)
BETWEEN = dt.datetime(2025, 3, 1, tzinfo=dt.timezone.utc)
AFTER = dt.datetime(2025, 7, 1, tzinfo=dt.timezone.utc)


def add_second_pricing_element(session_factory: DbSessionFactory) -> None:
    content = copy.deepcopy(DEFAULT_PRICE_AGGREGATE)
    key = (
        ProductPriceType.INSTANCE
        if ProductPriceType.INSTANCE in content
        else ProductPriceType.INSTANCE.value
    )
    credit = Decimal(content[key]["price"]["compute_unit"]["credit"])
    content[key]["price"]["compute_unit"]["credit"] = str(credit * 2)
    with session_factory() as session:
        item_hash = "c" * 64
        session.add(
            AggregateElementDb(
                item_hash=item_hash,
                key=PRICE_AGGREGATE_KEY,
                owner=PRICE_AGGREGATE_OWNER,
                content=content,
                creation_datetime=SECOND,
            )
        )
        aggregate = session.execute(
            select(AggregateDb).where(
                AggregateDb.key == PRICE_AGGREGATE_KEY,
                AggregateDb.owner == PRICE_AGGREGATE_OWNER,
            )
        ).scalar_one()
        aggregate.content = content
        aggregate.last_revision_hash = item_hash
        session.commit()


def make_resource() -> BillingResourceDb:
    return BillingResourceDb(
        item_hash="d" * 64,
        type=MessageType.instance,
        owner=OWNER,
        payment_type=PaymentType.credit,
        first_confirmed_at=FIRST,
        cost_spec={
            "type": "INSTANCE",
            "content": instance_content(volumes=[]),
            "volume_sizes_bytes": {ROOTFS_REF: 1024 * 1024},
            "file_size_bytes": None,
        },
    )


def test_pricing_model_at_picks_the_element_in_force(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
):
    add_second_pricing_element(session_factory)
    with session_factory() as session:
        before_version, _ = pricing_model_at(
            session, dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        )
        first_version, first_model = pricing_model_at(session, BETWEEN)
        second_version, second_model = pricing_model_at(session, AFTER)
        again_version, _ = pricing_model_at(session, BETWEEN)

    assert before_version == "default"
    assert first_version != "default"
    assert len(first_version) == 64
    assert second_version not in (first_version, "default")
    assert again_version == first_version
    first_unit = first_model[ProductPriceType.INSTANCE].price.compute_unit
    second_unit = second_model[ProductPriceType.INSTANCE].price.compute_unit
    assert first_unit is not None and second_unit is not None
    first_credit, second_credit = first_unit.credit, second_unit.credit
    assert second_credit == first_credit * 2


def test_rate_for_caches_per_version(
    mocker,
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    spy = mocker.spy(cost_spec, "rate_from_spec")
    with session_factory() as session:
        resource = make_resource()
        session.add(resource)
        session.flush()

        first = rate_for(session, resource, BETWEEN)
        second = rate_for(session, resource, BETWEEN)
        version, _ = pricing_model_at(session, BETWEEN)
        cached = get_billing_rate(session, resource.item_hash, version)

    assert first == second == cached
    assert first > 0
    assert spy.call_count == 1


def test_rate_for_recomputes_when_version_changes(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    add_second_pricing_element(session_factory)
    with session_factory() as session:
        resource = make_resource()
        session.add(resource)
        session.flush()

        before = rate_for(session, resource, BETWEEN)
        after = rate_for(session, resource, AFTER)
        rows = (
            session.execute(
                select(BillingRateDb).where(
                    BillingRateDb.item_hash == resource.item_hash
                )
            )
            .scalars()
            .all()
        )

    assert after > before
    assert len(rows) == 2


def test_rate_for_without_write_does_not_cache(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_settings_aggregate_in_db,
):
    """Read paths compute a missing rate without inserting into billing_rates."""
    with session_factory() as session:
        resource = make_resource()
        session.add(resource)
        session.flush()

        uncached = rate_for(session, resource, BETWEEN, write=False)
        version, _ = pricing_model_at(session, BETWEEN)
        assert get_billing_rate(session, resource.item_hash, version) is None

        cached = rate_for(session, resource, BETWEEN)
        assert get_billing_rate(session, resource.item_hash, version) == cached
        # A cached rate is still served on the read path.
        again = rate_for(session, resource, BETWEEN, write=False)

    assert uncached == cached == again
    assert uncached > 0
