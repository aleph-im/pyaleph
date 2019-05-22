import asyncio
from aleph.chains.register import OUTGOING_WORKERS, INCOMING_WORKERS
from aleph.chains import nuls, ethereum, binance


def start_connector(config):
    loop = asyncio.get_event_loop()

    for worker in INCOMING_WORKERS.values():
        loop.create_task(worker(config))

    for worker in OUTGOING_WORKERS.values():
        loop.create_task(worker(config))
