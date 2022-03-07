import logging

import aiohttp

from .balance_checker import ALEPH_CONTRACT_ID, BalanceChecker

LOGGER = logging.getLogger("balance.ethplorer")

API_URL_TEMPLATE = "https://api.ethplorer.io/getAddressInfo/{address}?apiKey={api_key}"


class EthplorerBalanceChecker(BalanceChecker):
    def __init__(self, api_key: str):
        self.api_key = api_key

    async def fetch_token_data(self, address: str):
        api_url = API_URL_TEMPLATE.format(address=address, api_key=self.api_key)

        # TODO: Check if using the freekey api key is acceptable
        async with aiohttp.ClientSession() as client:
            async with client.get(api_url) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def get_balance(self, address: str) -> float:
        result = await self.fetch_token_data(address)

        for token in result.get("tokens", ()):
            if token["tokenInfo"]["address"] == ALEPH_CONTRACT_ID:
                break
        else:
            return 0

        if symbol := token["tokenInfo"]["symbol"] != "ALEPH":
            raise ValueError(f"Expected a balance in ALEPH, got '{symbol}' instead.")

        balance = int(token["rawBalance"])
        decimals = token["tokenInfo"]["decimals"]
        decimals = int(decimals)
        # Check value consistency:
        if balance < 0:
            raise ValueError("Balance cannot be negative")
        if decimals != int(decimals):
            raise ValueError("Decimals must be an integer")
        if decimals < 1:
            raise ValueError("Decimals must be greater than 1")
        return balance / 10 ** decimals
