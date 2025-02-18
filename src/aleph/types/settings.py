from typing import List, Union

from pydantic import BaseModel

from aleph.db.models import AggregateDb


class CompatibleGPU(BaseModel):
    name: str
    model: str
    vendor: str
    device_id: str


class Settings(BaseModel):
    compatible_gpus: List[CompatibleGPU]
    community_wallet_address: str
    community_wallet_timestamp: int

    @staticmethod
    def from_aggregate(aggregate: Union[AggregateDb, dict]):
        content = aggregate.content if isinstance(aggregate, AggregateDb) else aggregate

        community_wallet_address = content.get("community_wallet_address", "")
        community_wallet_timestamp = content.get("community_wallet_timestamp", 0)
        compatible_gpus = content.get("compatible_gpus", [])

        settings = Settings(
            community_wallet_address=community_wallet_address,
            community_wallet_timestamp=community_wallet_timestamp,
            compatible_gpus=compatible_gpus,
        )

        return settings
