import datetime as dt

import pytest

from aleph.db.accessors.balances import update_credit_balances_distribution

CREDIT_HISTORY_URI = "/api/v0/addresses/0xtest/credit_history"


@pytest.mark.asyncio
async def test_credit_history_rejects_negative_start_date(ccn_api_client):
    response = await ccn_api_client.get(CREDIT_HISTORY_URI, params={"startDate": "-1"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_rejects_non_numeric_end_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"endDate": "yesterday"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_accepts_valid_time_filters(ccn_api_client):
    # No data for this address: a valid filtered query returns 404, not 422.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI,
        params={"startDate": "1768000000", "endDate": "1769000000"},
    )
    assert response.status == 404


@pytest.mark.asyncio
async def test_credit_history_rejects_out_of_range_start_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"startDate": "1e308"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_validates_snake_case_params(ccn_api_client):
    # The model uses populate_by_name=True (mirroring the messages query params
    # convention), so snake_case spellings are also accepted as field names.
    # The 422 (rather than the 404 an unbound param would yield) proves the
    # snake_case alias was bound and then validated against the negative value.
    response = await ccn_api_client.get(CREDIT_HISTORY_URI, params={"start_date": "-1"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_rejects_inverted_date_range(ccn_api_client):
    # Mirrors the messages query params convention: end_date < start_date is
    # rejected up front rather than silently returning empty results.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI,
        params={"startDate": "1769000000", "endDate": "1768000000"},
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_time_filter_filters_entries(
    ccn_api_client, session_factory
):
    with session_factory() as session:
        for month, message_hash in [(3, "e2e_dist_march"), (5, "e2e_dist_may")]:
            update_credit_balances_distribution(
                session=session,
                credits_list=[
                    {
                        "address": "0xe2etimefilter",
                        "amount": 1000,
                        "price": "0.000001",
                        "tx_hash": f"0xtx_{message_hash}",
                        "provider": "test_provider",
                        "expiration": None,
                        "origin": "test_origin",
                        "ref": f"ref_{message_hash}",
                        "payment_method": "test_payment",
                    }
                ],
                token="ALEPH",
                chain="ETH",
                message_hash=message_hash,
                message_timestamp=dt.datetime(2026, month, 1, tzinfo=dt.timezone.utc),
            )
        session.commit()

    # Window covering only the March entry
    start = dt.datetime(2026, 2, 15, tzinfo=dt.timezone.utc).timestamp()
    end = dt.datetime(2026, 4, 15, tzinfo=dt.timezone.utc).timestamp()
    response = await ccn_api_client.get(
        "/api/v0/addresses/0xe2etimefilter/credit_history",
        params={"startDate": str(start), "endDate": str(end)},
    )
    assert response.status == 200
    data = await response.json()
    refs = [entry["credit_ref"] for entry in data["credit_history"]]
    assert refs == ["e2e_dist_march"]
