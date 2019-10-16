import asyncio
from aleph.chains.register import OUTGOING_WORKERS, INCOMING_WORKERS
try:
    from aleph.chains import nuls
except:
    print("Can't load NULS")
try:
    from aleph.chains import nuls2
except:
    print("Can't load NULS2")
try:
    from aleph.chains import ethereum
except:
    print("Can't load ETH")
try:
    from aleph.chains import binance
except:
    print("Can't load BNB")

def start_connector(config, outgoing=True):
    loop = asyncio.get_event_loop()

    for worker in INCOMING_WORKERS.values():
        loop.create_task(worker(config))

    if outgoing:
        for worker in OUTGOING_WORKERS.values():
            loop.create_task(worker(config))
