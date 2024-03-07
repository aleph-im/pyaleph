import requests
import json
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

initial_messages_list = [
    # Diagnostic VMs
    "cad11970efe9b7478300fd04d7cc91c646ca0a792b9cc718650f86e1ccfac73e",  # Initial program
    "3fc0aa9569da840c43e7bd2033c3c580abb46b007527d6d20f2d4e98e867f7af",  # DiagVM
    "67705389842a0a1b95eaa408b009741027964edc805997475e95c505d642edd8",  # Legacy Diag VM
    # Volumes like runtimes, data, code, etc
    "6b8618f5b8913c0f582f1a771a154a556ee3fa3437ef3cf91097819910cf383b",  # Diag VM code volume
    "f873715dc2feec3833074bd4b8745363a0e0093746b987b4c8191268883b2463",  # Diag VM runtime volume
    "a92c81992e885d7a554fa78e255a5802404b7fdde5fbff20a443ccd13020d139",  # Legacy Diag VM code volume
    "bd79839bf96e595a06da5ac0b6ba51dea6f7e2591bb913deccded04d831d29f4",  # Legacy Diag VM runtime volume
]

FROM_CCN = "http://api1.aleph.im"
TO_CCN = "http://api2.aleph.im"
PUB_SUB_TOPIC = "ALEPH-TEST"
item_hashes_to_sync = ",".join(initial_messages_list)

logger.debug(f"Fetching messages from {FROM_CCN}...")
m1 = requests.get(f"{FROM_CCN}/api/v0/messages.json?pagination=50000&refs={item_hashes_to_sync}")
m1 = m1.json()['messages']
logger.debug(f"Fetched {len(m1)} messages from {FROM_CCN}")

logger.debug(f"Fetching messages from {TO_CCN}")
m2 = requests.get(f"{TO_CCN}/api/v0/messages.json?pagination=50000&refs={item_hashes_to_sync}")
m2 = m2.json()['messages']
logger.debug(f"Fetched {len(m2)} messages from {TO_CCN}")

m1_hashes = set(m["item_hash"] for m in m1)
m2_hashes = set(m["item_hash"] for m in m2)
hashes_to_sync = m1_hashes - m2_hashes
messages_to_sync = (m for m in m1 if m["item_hash"] in hashes_to_sync)  # Use a generator to avoid memory issues

logger.info(f"Messages to sync to {TO_CCN}: {len(hashes_to_sync)}")
for message in messages_to_sync:
    requests.post(f"{TO_CCN}/api/v0/ipfs/pubsub/pub",
                  json={"topic": PUB_SUB_TOPIC, "data": json.dumps(message)})
