import asyncio
import aiohttp
import orjson as json
import time
import struct
from operator import itemgetter
from aleph.network import check_message
from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata, get_chaindata_messages,
                                 join_tasks)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message


# TODO: move this to another project
# from nulsexplorer.protocol.data import (
#     NulsSignature, public_key_to_hash, address_from_hash, hash_from_address,
#     CHEAP_UNIT_FEE)
# from nulsexplorer.protocol.transaction import Transaction

import logging
LOGGER = logging.getLogger('chains.nuls')
CHAIN_NAME = 'NULS'


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    loop = asyncio.get_event_loop()

    sig_raw = bytes(bytearray.fromhex(message['signature']))
    sig = NulsSignature(sig_raw)
    
    sender_hash = hash_from_address(message['sender'])
    (sender_chain_id,) = struct.unpack('h', sender_hash[:2])
    
    hash = public_key_to_hash(sig.pub_key, sender_chain_id)
    
    address = address_from_hash(hash)
    if address != message['sender']:
        LOGGER.warning('Received bad signature from %s for %s'
                       % (address, message['sender']))
        return False

    verification = await get_verification_buffer(message)
    try:
        result = await loop.run_in_executor(
            None, sig.verify, verification)
    except Exception:
        LOGGER.exception("NULS Signature verification error")
        result = False
    # result = sig.verify(verification)
    return result

register_verifier(CHAIN_NAME, verify_signature)


# async def get_base_url(config):
#     return config.nulsexplorer.url.value


# async def get_last_height():
#     """ Returns the last height for which we already have the nuls data.
#     """
#     last_height = await Chain.get_last_height(CHAIN_NAME)

#     if last_height is None:
#         last_height = -1

#     return last_height


# async def request_transactions(config, session, start_height):
#     """ Continuously request data from the NULS blockchain.
#     TODO: setup a websocket and push system.
#     """
#     check_url = '{}/transactions.json'.format(await get_base_url(config))

#     async with session.get(check_url, params={
#         'business_ipfs': 1,
#         'sort_order': 1,
#         'startHeight': start_height+1,
#         'pagination': 500
#     }) as resp:
#         jres = await resp.json()
#         last_height = 0
#         for tx in sorted(jres['transactions'], key=itemgetter('blockHeight')):
#             last_height = tx['blockHeight']
#             LOGGER.info('Handling TX in block %s' % tx['blockHeight'])
#             if tx['info'].get('type', False) == 'ipfs':
#                 # Legacy remark-based message
#                 parts = tx['remark'].split(';')
#                 message = {}
#                 message["chain"] = CHAIN_NAME
#                 message["signature"] = tx["scriptSig"]
#                 message["tx_hash"] = tx["hash"]
#                 message["sender"] = tx["inputs"][0]["address"]
#                 if parts[1] == "A":
#                     # Ok, we have an aggregate.
#                     # Maybe check object size to avoid ddos attack ?
#                     message["type"] = "AGGREGATE"
#                     message["item_hash"] = parts[2]
#                 elif parts[1] == "P":
#                     message["type"] = "POST"
#                     message["item_hash"] = parts[2]
#                 else:
#                     LOGGER.info('Got unknown extended object in tx %s'
#                                 % tx['hash'])
#                     continue

#                 yield dict(type="native-single", time=tx['time']/1000,
#                            tx_hash=tx['hash'], height=tx['blockHeight'],
#                            messages=[message])

#             else:
#                 ldata = tx['info'].get('logicData')
#                 try:
#                     ddata = bytes.fromhex(ldata).decode('utf-8')
#                     jdata = json.loads(ddata)
#                     # if jdata.get('protocol', None) != 'aleph':
#                     #     LOGGER.info('Got unknown protocol object in tx %s'
#                     #                 % tx['hash'])
#                     #     continue
#                     # if jdata.get('version', None) != 1:
#                     #     LOGGER.info(
#                     #         'Got an unsupported version object in tx %s'
#                     #         % tx['hash'])
#                     #     continue  # unsupported protocol version

#                     messages = await get_chaindata_messages(jdata, context={
#                         "tx_hash": tx['hash'],
#                         "height": tx['blockHeight'],
#                         "time": tx['time']/1000,
#                         "publisher": tx["inputs"][0]["address"]
#                     })

#                     if messages is not None:
#                         yield dict(type="aleph", time=tx['time']/1000,
#                                    tx_hash=tx['hash'],
#                                    height=tx['blockHeight'],
#                                    publisher=tx["inputs"][0]["address"],
#                                    messages=messages)

#                 except json.JSONDecodeError:
#                     # if it's not valid json, just ignore it...
#                     LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
#                                 % ldata)

#                 except Exception:
#                     LOGGER.exception("Can't decode incoming logic data %r"
#                                      % ldata)

#         # Since we got no critical exception, save last received object
#         # block height to do next requests from there.
#         if last_height:
#             await Chain.set_last_height(CHAIN_NAME, last_height)


# async def check_incoming(config):
#     last_stored_height = await get_last_height()

#     LOGGER.info("Last block is #%d" % last_stored_height)
#     loop = asyncio.get_event_loop()

#     async with aiohttp.ClientSession() as session:
#         while True:
#             last_stored_height = await get_last_height()
#             i = 0
#             j = 0

#             tasks = []
#             seen_ids = []
#             async for txi in request_transactions(config, session,
#                                                   last_stored_height):
#                 i += 1
#                 # TODO: handle big message list stored in IPFS case
#                 # (if too much messages, an ipfs hash is stored here).
#                 for message in txi['messages']:
#                     j += 1
#                     # message = await check_message(
#                     #     message, from_chain=True,
#                     #     trusted=(txi['type'] == 'native-single'))
#                     # if message is None:
#                     #     # message got discarded at check stage.
#                     #     continue

#                     message['time'] = txi['time']

#                     # running those separately... a good/bad thing?
#                     # shouldn't do that for VMs.
#                     tasks.append(
#                         incoming(
#                             message, chain_name=CHAIN_NAME,
#                             seen_ids=seen_ids,
#                             tx_hash=txi['tx_hash'],
#                             height=txi['height'],
#                             check_message=(txi['type'] != 'native-single')))

#                     # let's join every 500 messages...
#                     if (j > 200):
#                         await join_tasks(tasks, seen_ids)
#                         j = 0
#                         # seen_ids = []

#             await join_tasks(tasks, seen_ids)

#             if (i < 10):  # if there was less than 10 items, not a busy time
#                 # wait 5 seconds (half of typical time between 2 blocks)
#                 await asyncio.sleep(5)


# async def nuls_incoming_worker(config):
#     if config.nuls.enabled.value:
#         while True:
#             try:
#                 await check_incoming(config)

#             except Exception:
#                 LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
#                 await asyncio.sleep(10)

# register_incoming_worker(CHAIN_NAME, nuls_incoming_worker)


# async def prepare_businessdata_tx(address, utxo, content):
#     # we take the first 10, hoping it's enough... bad, bad, bad!
#     # TODO: do a real utxo management here
#     # tx = transaction.Transaction()
#     tx = await Transaction.from_dict({
#       "type": 10,
#       "time": int(time.time() * 1000),
#       "blockHeight": None,
#       "fee": 0,
#       "remark": b"",
#       "scriptSig": b"",
#       "info": {
#           "logicData": content.hex()
#       },
#       "inputs": [{'fromHash': inp['hash'],
#                   'fromIndex': inp['idx'],
#                   'value': inp['value'],
#                   'lockTime': inp['lockTime']} for inp in utxo],
#       "outputs": [
#           {"address": hash_from_address(address),
#            "value": sum([inp['value'] for inp in utxo]),
#            "lockTime": 0}
#       ]
#     })
#     tx.coin_data.outputs[0].na = (
#         sum([inp['value'] for inp in utxo])
#         - (await tx.calculate_fee()))
#     return tx


# async def nuls_packer(config):
#     from secp256k1 import PrivateKey

#     pri_key = bytes.fromhex(config.nuls.private_key.value)
#     privkey = PrivateKey(pri_key, raw=True)
#     pub_key = privkey.pubkey.serialize()
#     address = await get_address(pub_key, config.nuls.chain_id.value)
#     LOGGER.info("NULS Connector set up with address %s" % address)
#     utxo = await get_utxo(config, address)
#     i = 0
#     while True:
#         if (i >= 100) or (utxo[0]['value'] < CHEAP_UNIT_FEE):
#             await asyncio.sleep(30)  # wait three (!!) blocks
#             # utxo = await get_utxo(config, address)
#             i = 0

#         utxo = await get_utxo(config, address)
#         selected_utxo = utxo[:10]
#         messages = [message async for message
#                     in (await Message.get_unconfirmed_raw(
#                             limit=5000, for_chain=CHAIN_NAME))]
#         if len(messages):
#             content = await get_chaindata(messages, bulk_threshold=10000)
#             # content = json.dumps(content)
#             tx = await prepare_businessdata_tx(address, selected_utxo,
#                                                content.encode('UTF-8'))
#             await tx.sign_tx(pri_key)
#             tx_hex = (await tx.serialize()).hex()
#             # tx_hash = await tx.get_hash()
#             LOGGER.info("Broadcasting TX")
#             tx_hash = await broadcast(config, tx_hex)
#             utxo = [{
#                 'hash': tx_hash,
#                 'idx': 0,
#                 'lockTime': 0,
#                 'value': tx.coin_data.outputs[0].na
#             }]

#         await asyncio.sleep(config.nuls.commit_delay.value)

#         i += 1


# async def nuls_outgoing_worker(config):
#     if config.nuls.packing_node.value:
#         while True:
#             try:
#                 await nuls_packer(config)

#             except Exception:
#                 LOGGER.exception("ERROR, relaunching outgoing in 10 seconds")
#                 await asyncio.sleep(10)

# register_outgoing_worker(CHAIN_NAME, nuls_outgoing_worker)


# async def get_address(pubkey, chain_id):
#     phash = public_key_to_hash(pubkey, chain_id=chain_id)
#     address = address_from_hash(phash)
#     return address


# async def broadcast(config, tx_hex):
#     broadcast_url = '{}/broadcast'.format(
#         await get_base_url(config))
#     data = {'txHex': tx_hex}

#     async with aiohttp.ClientSession() as session:
#         async with session.post(broadcast_url, json=data) as resp:
#             jres = (await resp.json())['value']
#             return jres


# async def get_utxo(config, address):
#     check_url = '{}/addresses/outputs/{}.json'.format(
#         await get_base_url(config), address)

#     async with aiohttp.ClientSession() as session:
#         async with session.get(check_url) as resp:
#             jres = await resp.json()
#             return jres['outputs']
