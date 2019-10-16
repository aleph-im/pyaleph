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
from aleph.model.pending import pending_messages_count, pending_txs_count

from nuls2.model.data import (hash_from_address, public_key_to_hash,
                              recover_message_address, get_address,
                              address_from_hash, CHEAP_UNIT_FEE, b58_decode)
from nuls2.api.server import get_server
from nuls2.model.transaction import Transaction

from coincurve import PrivateKey

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

# def broadcast_content(config, contract, web3, account,
#                       gas_price, nonce, content):
#     # content = json.dumps(content)
#     tx = contract.functions.doEmit(content).buildTransaction({
#             'chainId': config.ethereum.chain_id.value,
#             'gasPrice': gas_price,
#             'nonce': nonce,
#             })
#     signed_tx = account.signTransaction(tx)
#     return web3.eth.sendRawTransaction(signed_tx.rawTransaction)




async def broadcast(server, tx_hex, chain_id=1):
    return await server.broadcastTx(chain_id, tx_hex)

async def get_balance(server, address, chain_id, asset_id):
    return await server.getAccountBalance(chain_id, chain_id,
                                          asset_id, address)
    
async def prepare_transfer_tx(address, targets, nonce, chain_id=1,
                              asset_id=1, remark=""):
    """ Targets are tuples: address and value.
    """
    outputs = [
        {"address": add,
         "amount": val,
         "lockTime": 0,
         "assetsChainId": chain_id,
         "assetsId": asset_id} for add, val in targets
    ]
    
    tx = await Transaction.from_dict({
        "type": 2,
        "time": int(time.time()),
        "remark": remark.encode('utf-8'),
        "coinFroms": [
            {
                'address': address,
                'assetsChainId': chain_id,
                'assetsId': asset_id,
                'amount': 0,
                'nonce': nonce,
                'locked': 0
            }
        ],
        "coinTos": outputs
    })
    tx.inputs[0]['amount'] = (
        (await tx.calculate_fee())
        + sum([o['amount'] for o in outputs]))
    return tx

async def get_nonce(server, account_address, chain_id, asset_id=1):
    balance_info = await get_balance(server,
                                     account_address,
                                     chain_id, asset_id)
    return balance_info['nonce']

async def nuls2_packer(config):
    loop = asyncio.get_event_loop()
    server = get_server(config.nuls2.api_url.value)

    pri_key = bytes.fromhex(config.nuls2.private_key.value)
    privkey = PrivateKey(pri_key)
    pub_key = privkey.public_key.format()
    chain_id = config.nuls.chain_id.value
    address = get_address(pub_key, config.nuls.chain_id.value)
    
    LOGGER.info("NULS2 Connector set up with address %s" % address)
    # utxo = await get_utxo(config, address)
    i = 0
    nonce = await get_nonce(server, address, chain_id)
    
    while True:
        if (await pending_txs_count(chain=CHAIN_NAME)) \
           or (await pending_messages_count(source_chain=CHAIN_NAME)):
            await asyncio.sleep(30)
            continue
        
        if i >= 100:
            await asyncio.sleep(30)  # wait three (!!) blocks
            nonce = await get_nonce(server, address, chain_id)
            # utxo = await get_utxo(config, address)
            i = 0
            
        messages = [message async for message
                    in (await Message.get_unconfirmed_raw(
                            limit=100000,
                            for_chain=CHAIN_NAME))]
        
        if len(messages):
            content = await get_chaindata(messages)
        
            tx = await prepare_transfer_tx(address, [(address, CHEAP_UNIT_FEE)], nonce,
                                           chain_id=chain_id, asset_id=1,
                                           remark=content)
            await tx.sign_tx(pri_key)
            tx_hex = (await tx.serialize()).hex()
            ret = await broadcast(server, tx_hex, chain_id=chain_id)
            LOGGER.info("Broadcasted %r on %s" % (ret['hash'], CHAIN_NAME))
            nonce = ret['hash'][-16:]

        await asyncio.sleep(config.ethereum.commit_delay.value)
        i += 1


async def nuls2_outgoing_worker(config):
    if config.nuls2.packing_node.value:
        while True:
            try:
                await nuls2_packer(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching outgoing in 10 seconds")
                await asyncio.sleep(10)

register_outgoing_worker(CHAIN_NAME, nuls2_outgoing_worker)