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

from neo.Core.Cryptography.Crypto import Crypto

import logging
LOGGER = logging.getLogger('chains.neo')
CHAIN_NAME = 'NEO'

def num2VarInt(num):
    if num < 0xfd:
        return f'{num:02x}'
    elif (num <= 0xffff):
        # uint16
        return f'fd{num:04x}'
    elif (num <= 0xffffffff):
        # uint32
        return f'fe{num:08x}'
    else:
        # uint64
        return f'ff{num:16x}'

async def buildNEOVerification(message, salt):
    base_verification = await get_verification_buffer(message)
    verification = (salt.encode('utf-8') + base_verification).hex()
    verification = num2VarInt(int(len(verification)/2)) + verification
    verification = '010001f0' + verification + '0000'
    return verification

async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    Crypto.SetupSignatureCurve()
    
    try:
        signature = json.loads(message['signature'])
    except Exception:
        LOGGER.exception("NEO Signature deserialization error")
        return False
    
    try:
        script_hash = Crypto.ToScriptHash(
            "21" + signature['publicKey'] + "ac"
        )
        address = Crypto.ToAddress(script_hash)
    except Exception:
        LOGGER.exception("NEO Signature Key error")
        return False
    
    if address != message['sender']:
        LOGGER.warning('Received bad signature from %s for %s'
                       % (address, message['sender']))
        return False
    
    
    try:
        verification = await buildNEOVerification(
            message, signature['salt'])
    
        result = Crypto.VerifySignature(
            verification,
            bytes.fromhex(signature['data']),
            bytes.fromhex(signature['publicKey']),
            unhex=True)
    except Exception:
        LOGGER.exception("NULS Signature verification error")
        result = False
        
    return result

register_verifier(CHAIN_NAME, verify_signature)

