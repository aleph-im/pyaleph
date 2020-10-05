import asyncio
import aiohttp
import orjson as json
import time
import struct
from aleph.chains.common import (get_verification_buffer)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

from substrateinterface import Keypair

import logging
LOGGER = logging.getLogger('chains.substrate')
CHAIN_NAME = 'DOT'

async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    
    try:
        signature = json.loads(message['signature'])
    except Exception:
        LOGGER.exception("Substrate signature deserialization error")
        return False
    
    try:
        if signature.get('curve', 'sr25519') != 'sr25519':
            LOGGER.warning('Unsupported curve %s' % signature.get('curve'))
    except Exception:
        LOGGER.exception("Substrate signature Key error")
        return False
    
    
    try:
        keypair = Keypair(ss58_address=message['sender'])
        verif = (await get_verification_buffer(message)).decode('utf-8')
        result = keypair.verify(verif, signature['data'])
    except Exception:
        LOGGER.exception("Substrate Signature verification error")
        result = False
        
    return result

register_verifier(CHAIN_NAME, verify_signature)

