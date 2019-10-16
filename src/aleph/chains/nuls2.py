import asyncio
import aiohttp
import orjson as json
import time
import struct
import base64
from operator import itemgetter
from aleph.network import check_message
from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata, get_chaindata_messages,
                                 join_tasks)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

from nuls2.model.data import (hash_from_address, public_key_to_hash,
                              recover_message_address)

import logging
LOGGER = logging.getLogger('chains.nuls2')
CHAIN_NAME = 'NULS2'

async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    loop = asyncio.get_event_loop()
    sig_raw = base64.b64decode(message['signature'])
    
    sender_hash = hash_from_address(message['sender'])
    (sender_chain_id,) = struct.unpack('h', sender_hash[:2])
    verification = await get_verification_buffer(message)
    print(verification)
    try:
        address = recover_message_address(sig_raw, verification,
                                          chain_id=sender_chain_id)
    except Exception:
        LOGGER.exception("NULS Signature verification error")
        return False
    
    if address != message['sender']:
        LOGGER.warning('Received bad signature from %s for %s'
                       % (address, message['sender']))
        return False
    else:
        return True

register_verifier(CHAIN_NAME, verify_signature)