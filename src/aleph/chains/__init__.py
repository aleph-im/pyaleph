import asyncio
from aleph.chains.nuls import nuls_incoming_worker

async def start_connector(config):
    loop = asyncio.get_event_loop()
    loop.create_task(incoming_worker(config))
