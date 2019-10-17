import asyncio
import aiohttp
import orjson as json
import time
import struct
import base64
import math
from operator import itemgetter
from aleph.network import check_message
from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata, get_chaindata_messages,
                                 join_tasks, incoming_chaindata)
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
PAGINATION = 500

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

async def get_last_height():
    """ Returns the last height for which we already have the nuls data.
    """
    last_height = await Chain.get_last_height(CHAIN_NAME)

    if last_height is None:
        last_height = -1

    return last_height

# async def get_transactions(config, server, chain_id,
#                            target_addr, start_height,
#                            end_height=None):

#     if end_height is None:
#         end_height = -1
#     result = await server.getAccountTxs(
#         chain_id, 1, PAGINATION,
#         target_addr, 2, start_height, end_height)
    
#     seen_hashes = list()

#     if result['totalCount'] >= PAGINATION:
#         if end_height is None:
#             # If we have no end_time, we ensure we have a non-slipping range
#             # while we iterate.
#             end_height = result['list'][0]['height']

#         for i in reversed(range(math.ceil(result['total']/PAGINATION))):
#             nresult = await server.getAccountTxs(
#                         chain_id, i, PAGINATION,
#                         target_addr, 2, start_height, end_height)
#             for tx in sorted(nresult['list'], key=itemgetter('height')):
#                 if tx['txHash'] not in seen_hashes:
#                     yield tx
#                     seen_hashes.append(tx['txHash'])
#     else:
#         for tx in sorted(result['list'], key=itemgetter('height')):
#             if tx['txHash'] not in seen_hashes:
#                 yield tx
#                 seen_hashes.append(tx['txHash'])
                
async def get_base_url(config):
    return config.nuls2.explorer_url.value

async def get_transactions(config, session, chain_id,
                           target_addr, start_height,
                           end_height=None, remark=None):
    check_url = '{}transactions.json'.format(await get_base_url(config))

    async with session.get(check_url, params={
        'address': target_addr,
        'sort_order': 1,
        'startHeight': start_height+1,
        'pagination': 500
    }) as resp:
        jres = await resp.json()
        for tx in sorted(jres['transactions'], key=itemgetter('height')):
            if remark is not None and tx['remark'] != remark:
                continue
            
            yield tx

async def request_transactions(config, session,
                               start_height):
    """ Continuously request data from the NULS blockchain.
    """
    target_addr = config.nuls2.sync_address.value
    remark = config.nuls2.remark.value
    chain_id = config.nuls2.chain_id.value

    last_height = None
    async for tx in get_transactions(config, session, chain_id,
                                     target_addr, start_height, remark=remark):
        ldata = tx['txDataHex']
        LOGGER.info('Handling TX in block %s' % tx['height'])
        try:
            ddata = bytes.fromhex(ldata).decode('utf-8')
            last_height = tx['height']
            jdata = json.loads(ddata)
            
            context = {"chain_name": CHAIN_NAME,
                       "tx_hash": tx['hash'],
                       "height": tx['height'],
                       "time": tx['createTime'],
                       "publisher": tx["coinFroms"][0]['address']}
            yield (jdata, context)

        except json.JSONDecodeError:
            # if it's not valid json, just ignore it...
            LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
                        % ldata)

    if last_height:
        await Chain.set_last_height(CHAIN_NAME, last_height)



async def check_incoming(config):
    last_stored_height = await get_last_height()

    LOGGER.info("Last block is #%d" % last_stored_height)
    async with aiohttp.ClientSession() as session:
        while True:
            last_stored_height = await get_last_height()
            async for jdata, context in request_transactions(config, session,
                                                last_stored_height+1):
                await incoming_chaindata(jdata, context)
            await asyncio.sleep(10)


async def nuls_incoming_worker(config):
    if config.nuls2.enabled.value:
        while True:
            try:
                await check_incoming(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
                await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, nuls_incoming_worker)


async def broadcast(server, tx_hex, chain_id=1):
    return await server.broadcastTx(chain_id, tx_hex)

async def get_balance(server, address, chain_id, asset_id):
    return await server.getAccountBalance(chain_id, chain_id,
                                          asset_id, address)
    
async def prepare_transfer_tx(address, targets, nonce, chain_id=1,
                              asset_id=1, remark=b"", raw_tx_data=None):
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
        "remark": remark,
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
    print(await tx.calculate_fee())
    tx.inputs[0]['amount'] = (
        (await tx.calculate_fee())
        + sum([o['amount'] for o in outputs]))
    
    if raw_tx_data is not None:
        tx.raw_tx_data = raw_tx_data
        
    return tx

async def get_nonce(server, account_address, chain_id, asset_id=1):
    balance_info = await get_balance(server,
                                     account_address,
                                     chain_id, asset_id)
    return balance_info['nonce']

async def nuls2_packer(config):
    loop = asyncio.get_event_loop()
    server = get_server(config.nuls2.api_url.value)
    target_addr = config.nuls2.sync_address.value
    remark = config.nuls2.remark.value.encode('utf-8')

    pri_key = bytes.fromhex(config.nuls2.private_key.value)
    privkey = PrivateKey(pri_key)
    pub_key = privkey.public_key.format()
    chain_id = config.nuls2.chain_id.value
    address = get_address(pub_key, config.nuls2.chain_id.value)
    
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
        
            tx = await prepare_transfer_tx(address, [(target_addr, CHEAP_UNIT_FEE)], nonce,
                                           chain_id=chain_id, asset_id=1,
                                           raw_tx_data=content, remark=remark)
            await tx.sign_tx(pri_key)
            tx_hex = (await tx.serialize(update_data=False)).hex()
            ret = await broadcast(server, tx_hex, chain_id=chain_id)
            LOGGER.info("Broadcasted %r on %s" % (ret['hash'], CHAIN_NAME))
            nonce = ret['hash'][-16:]

        await asyncio.sleep(config.nuls2.commit_delay.value)
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