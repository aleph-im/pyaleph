"""
Two CCNs fed the same ledger inputs in a different order serve identical
credit responses (spec 14: two-CCN determinism). The second CCN is simulated
on the same database: the period is folded, the responses recorded, the v2
tables cleared by a refold, the period folded again from the reversed inputs.
"""

from typing import Any, Dict

import pytest
from configmanager import Config
from credit_v2_helpers import set_last_closed_period
from services.test_credit_fold_golden import (
    ALICE,
    BOB,
    CAROL,
    PERIOD,
    UNIX,
    golden_inputs,
    ledger_snapshot,
)

from aleph.services.credits.fold import apply_period, refold_from, write_checkpoint
from aleph.types.db_session import DbSessionFactory


@pytest.fixture
def serve_v2(mock_config: Config) -> Config:
    credits = mock_config.aleph.credits
    credits.serve_v2.value = True
    credits.genesis.period.value = UNIX
    credits.attestation.start_period.value = UNIX
    return mock_config


def _fold(session_factory: DbSessionFactory, reverse: bool) -> None:
    with session_factory() as session:
        refold_from(session, PERIOD)
        assert ledger_snapshot(session) == []
        apply_period(session, PERIOD, golden_inputs(reverse=reverse))
        write_checkpoint(session, PERIOD)
        set_last_closed_period(session, PERIOD)
        session.commit()


async def _responses(ccn_api_client) -> Dict[str, Any]:
    uris = [f"/api/v0/credits/periods/{UNIX}"]
    for address in (ALICE, BOB, CAROL):
        uris += [
            f"/api/v0/addresses/{address}/balance",
            f"/api/v0/addresses/{address}/credit_history",
            f"/api/v0/addresses/{address}/credit_history/summary",
        ]
    responses = {}
    for uri in uris:
        response = await ccn_api_client.get(uri)
        assert response.status == 200, f"{uri}: {await response.text()}"
        responses[uri] = await response.json()
    return responses


@pytest.mark.asyncio
async def test_two_ccns_folding_reordered_inputs_serve_identical_responses(
    serve_v2: Config, ccn_api_client, session_factory: DbSessionFactory
):
    _fold(session_factory, reverse=False)
    first = await _responses(ccn_api_client)

    _fold(session_factory, reverse=True)
    second = await _responses(ccn_api_client)

    assert second == first
    # The responses carry the folded figures, not empty defaults.
    period = first[f"/api/v0/credits/periods/{UNIX}"]
    assert period["status"] == "closed" and period["period_hash"]
    assert first[f"/api/v0/addresses/{ALICE}/balance"]["credit_balance"] == 699_955
    assert first[f"/api/v0/addresses/{BOB}/credit_history"]["credit_history"]
