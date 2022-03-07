import os.path
from pathlib import Path

import pytest
from typing import Dict
import json
from aleph.chains.balance.ethplorer import EthplorerBalanceChecker

FIXTURES_DIR = Path(os.path.dirname(__file__)) / "fixtures"


def get_json(fixture_file: Path) -> Dict:
    with fixture_file.open() as f:
        return json.load(f)


@pytest.mark.asyncio
async def test_balance_ethplorer_with_balance(mocker):
    fixture_file = FIXTURES_DIR / "ethplorer_account_with_balance.json"
    mock_response = get_json(fixture_file)

    mock_fetch_token = mocker.patch.object(EthplorerBalanceChecker, "fetch_token_data")
    mock_fetch_token.return_value = mock_response

    balance = await EthplorerBalanceChecker(api_key="fake-key").get_balance(
        "0x27702a26126e0b3702af63ee09ac4d1a084ef628"
    )
    assert balance == 217560.0


@pytest.mark.asyncio
async def test_balance_ethplorer_no_tokens(mocker):
    fixture_file = FIXTURES_DIR / "ethplorer_account_no_tokens.json"
    mock_response = get_json(fixture_file)

    mock_fetch_token = mocker.patch.object(EthplorerBalanceChecker, "fetch_token_data")
    mock_fetch_token.return_value = mock_response

    balance = await EthplorerBalanceChecker(api_key="fake-key").get_balance(
        "0x27702a26126e0b3702af63ee09ac4d1a084ef628"
    )
    assert balance == 0
