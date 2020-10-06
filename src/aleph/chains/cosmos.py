import asyncio
import aiohttp
import json
import time
import struct
import base64
import ecdsa
import hashlib
from aleph.chains.common import (get_verification_buffer)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

from cosmospy._wallet import privkey_to_address, privkey_to_pubkey, pubkey_to_address
from cosmospy.typing import SyncMode
import ecdsa

import logging
LOGGER = logging.getLogger('chains.cosmos')
CHAIN_NAME = 'CSDK'


async def get_signable_message(message):
    signable = (await get_verification_buffer(message)).decode('utf-8')
    content_message = {
        "type": "signutil/MsgSignText",
        "value": {
            "message": signable,
            "signer": message['sender'],
        },
    }
    
    return {
        "chain_id": "signed-message-v1",
        "account_number": str(0),
        "fee": {
            "amount": [],
            "gas": str(0),
        },
        "memo": "",
        "sequence": str(0),
        "msgs": [content_message,],
    }
    
    
async def get_verification_string(message):
    value = await get_signable_message(message)
    print(value)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


async def get_hrp(address):
    hrp, rest = address.split("1", 1)
    return hrp


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    
    try:
        signature = json.loads(message['signature'])
    except Exception:
        LOGGER.exception("Cosmos signature deserialization error")
        return False
    
    try:
        if signature.get('pub_key').get('type') != 'tendermint/PubKeySecp256k1':
            LOGGER.warning('Unsupported curve %s' % signature.get('pub_key').get('type'))
    except Exception:
        LOGGER.exception("Cosmos signature Key error")
        return False
    
    
    try:
        pub_key = base64.b64decode(signature.get('pub_key').get('value'))
        hrp = await get_hrp(message['sender'])
    except Exception:
        LOGGER.exception("Cosmos key verification error")
        result = False
        
    try:
        sig_compact = base64.b64decode(signature.get('signature'))
    except Exception:
        LOGGER.exception("Cosmos signature deserialization error")
        result = False
    
    
    try:
        address = pubkey_to_address(pub_key, hrp=hrp)
        if address != message['sender']:
            LOGGER.warning('Signature for bad address %s instead of %s' % (address, message['sender']))
            return False
        
        verif = await get_verification_string(message)
        vk = ecdsa.VerifyingKey.from_string(pub_key, curve=ecdsa.SECP256k1)
        verified = vk.verify(sig_compact, verif.encode("utf-8"), hashfunc=hashlib.sha256)
        return verified
    
    except Exception:
        LOGGER.exception("Substrate Signature verification error")
        result = False
    
    return result

register_verifier(CHAIN_NAME, verify_signature)

