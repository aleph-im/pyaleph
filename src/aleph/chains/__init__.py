import asyncio
import traceback
from aleph.chains.register import OUTGOING_WORKERS, INCOMING_WORKERS
try:
    from aleph.chains import nuls
except:
    print("Can't load NULS")
    traceback.print_exc()
try:
    from aleph.chains import nuls2
except:
    print("Can't load NULS2")
    traceback.print_exc()
try:
    from aleph.chains import ethereum
except:
    print("Can't load ETH")
    traceback.print_exc()
try:
    from aleph.chains import binance
except:
    print("Can't load BNB")
    traceback.print_exc()
try:
    from aleph.chains import neo
except:
    print("Can't load NEO")
    traceback.print_exc()
try:
    from aleph.chains import substrate
except:
    print("Can't load DOT")
    traceback.print_exc()

def start_connector(config, outgoing=True):
    loop = asyncio.get_event_loop()

    for worker in INCOMING_WORKERS.values():
        loop.create_task(worker(config))

    if outgoing:
        for worker in OUTGOING_WORKERS.values():
            loop.create_task(worker(config))
