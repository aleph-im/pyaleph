from typing import List

from pydantic import BaseModel

from aleph.db.models import AggregateDb
from aleph.toolkit.constants import DEFAULT_SETTINGS_AGGREGATE


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
    def from_aggregate(aggregate: AggregateDb):
        content = aggregate.content

        community_wallet_address = content.get("community_wallet_address", "")
        community_wallet_timestamp = content.get("community_wallet_timestamp", 0)
        compatible_gpus = content.get("compatible_gpus", [])

        settings = Settings(
            community_wallet_address=community_wallet_address,
            community_wallet_timestamp=community_wallet_timestamp,
            compatible_gpus=compatible_gpus,
        )

        return settings

    @classmethod
    def default(cls):
        community_wallet_address = DEFAULT_SETTINGS_AGGREGATE.get("community_wallet_address", "")
        community_wallet_timestamp = DEFAULT_SETTINGS_AGGREGATE.get(
            "community_wallet_timestamp", 0
        )
        compatible_gpus = DEFAULT_SETTINGS_AGGREGATE.get("compatible_gpus", [])

        settings = Settings(
            community_wallet_address=community_wallet_address,
            community_wallet_timestamp=community_wallet_timestamp,
            compatible_gpus=compatible_gpus,
        )

        return settings
