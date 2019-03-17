import asyncio
from aleph.chains.nuls import nuls_incoming_worker


def start_connector(config):
    loop = asyncio.get_event_loop()
    loop.create_task(nuls_incoming_worker(config))
