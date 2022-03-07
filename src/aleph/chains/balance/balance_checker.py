import abc


ALEPH_CONTRACT_ID = "0x27702a26126e0b3702af63ee09ac4d1a084ef628"


class BalanceChecker(abc.ABC):
    @abc.abstractmethod
    async def get_balance(self, address: str) -> float:
        ...
