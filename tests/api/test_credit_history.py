import pytest

CREDIT_HISTORY_URI = "/api/v0/addresses/0xtest/credit_history"


@pytest.mark.asyncio
async def test_credit_history_rejects_negative_start_date(ccn_api_client):
    response = await ccn_api_client.get(CREDIT_HISTORY_URI, params={"start_date": "-1"})
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_rejects_non_numeric_end_date(ccn_api_client):
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI, params={"end_date": "yesterday"}
    )
    assert response.status == 422


@pytest.mark.asyncio
async def test_credit_history_accepts_valid_time_filters(ccn_api_client):
    # No data for this address: a valid filtered query returns 404, not 422.
    response = await ccn_api_client.get(
        CREDIT_HISTORY_URI,
        params={"start_date": "1768000000", "end_date": "1769000000"},
    )
    assert response.status == 404
