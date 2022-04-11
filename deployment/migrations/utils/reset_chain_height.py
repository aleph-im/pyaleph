from configmanager import Config
from aleph.model.chains import Chain


async def reset_chain_height(config: Config):
    start_height = config.ethereum.start_height.value
    await Chain.set_last_height("ETH", start_height)
