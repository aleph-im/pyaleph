import pytest
from aleph.db.models import (
    AlephBalanceDb,
)
MESSAGES_URI = "/api/v0/addresses/0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba/balance"

@pytest.mark.asyncio
async def test_get_balance(ccn_api_client, user_balance: AlephBalanceDb):
    response = await ccn_api_client.get(MESSAGES_URI)
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["balance"] == user_balance.balance
    assert data["locked_amount"] == 6
