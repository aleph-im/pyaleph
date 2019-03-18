import binascii
import asyncio
import aiohttp
import json
from operator import itemgetter
from aleph.network import check_message
from aleph.chains.common import incoming, get_verification_buffer
from aleph.chains.register import register_verifier, register_incoming_worker


# TODO: move this to another project
from nulsexplorer.protocol.data import NulsSignature

import logging
LOGGER = logging.getLogger('chains.nuls')
CHAIN_NAME = 'NULS'


async def verify_signature(message):
    """ Verifies a signature of a hash and returns the address that signed it.
    """
    sig_raw = bytes(bytearray.fromhex(message['signature']))
    sig = NulsSignature(sig_raw)
    verification = await get_verification_buffer(message)
    return sig.verify(verification)

register_verifier(CHAIN_NAME, verify_signature)


async def get_base_url(config):
    return config.nulsexplorer.url.value


async def get_last_height():
    """ Returns the last height for which we already have the nuls data.
    """
    return 0  # for now, request everything (bad!) TODO: Change that!


async def request_transactions(config, session, start_height):
    """ Continuously request data from the NULS blockchain.
    TODO: setup a websocket and push system.
    """
    check_url = '{}/transactions.json'.format(await get_base_url(config))

    async with session.get(check_url, params={
        'business_ipfs': 1,
        'sort_order': 1,
        'startHeight': start_height+1,
        'pagination': 100000  # TODO: handle pagination correctly!
    }) as resp:
        jres = await resp.json()
        for tx in sorted(jres['transactions'], key=itemgetter('blockHeight')):
            if tx['info'].get('type', False) == 'ipfs':
                # Legacy remark-based message
                parts = tx['remark'].split(';')
                message = {}
                message["chain"] = CHAIN_NAME
                message["signature"] = tx["scriptSig"]
                message["tx_hash"] = tx["hash"]
                message["sender"] = tx["inputs"][0]["address"]
                if parts[1] == "A":
                    # Ok, we have an aggregate.
                    # Maybe check object size to avoid ddos attack ?
                    message["type"] = "AGGREGATE"
                    message["item_hash"] = parts[2]
                elif parts[1] == "P":
                    message["type"] = "POST"
                    message["item_hash"] = parts[2]
                else:
                    LOGGER.info('Got unknown extended object in tx %s'
                                % tx['hash'])
                    continue

                yield dict(type="native-single", time=tx['time']/1000,
                           tx_hash=tx['hash'], height=tx['blockHeight'],
                           messages=[message])

            else:
                ldata = tx['info'].get('logicData')
                try:
                    ddata = binascii.unhexlify(ldata).decode('utf-8')
                    jdata = json.loads(ddata)
                    if jdata.get('protocol', None) != 'aleph':
                        LOGGER.info('Got unknown protocol object in tx %s'
                                    % tx['hash'])
                        continue
                    if jdata.get('version', None) != 1:
                        LOGGER.info(
                            'Got an unsupported version object in tx %s'
                            % tx['hash'])
                        continue  # unsupported protocol version

                    yield dict(type="aleph", time=tx['time']/1000,
                               tx_hash=tx['hash'], height=tx['blockHeight'],
                               messages=jdata['content']['messages'])

                except json.JSONDecodeError:
                    # if it's not valid json, just ignore it...
                    LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
                                % ldata)

                except Exception:
                    LOGGER.exception("Can't decode incoming logic data %r"
                                     % ldata)


async def check_incoming(config):
    last_stored_height = await get_last_height()
    # last_height = -1
    if last_stored_height is None:
        last_stored_height = -1

    LOGGER.info("Last block is #%d" % last_stored_height)

    async with aiohttp.ClientSession() as session:
        while True:
            async for txi in request_transactions(config, session,
                                                  last_stored_height):
                # TODO: handle big message list stored in IPFS case
                # (if too much messages, an ipfs hash is stored here).
                for message in txi['messages']:
                    message = await check_message(
                        message, from_chain=True,
                        trusted=(txi['type'] == 'native-single'))
                    if message is None:
                        # message got discarded at check stage.
                        continue

                    message['time'] = txi['time']
                    # TODO: handle other chain signatures here
                    # now handled in check_message
                    # signed = await verify_signature(message, tx=txi)

                    await incoming(message, chain_name=CHAIN_NAME,
                                   tx_hash=txi['tx_hash'],
                                   height=txi['height'])

                if txi['height'] > last_stored_height:
                    last_stored_height = txi['height']

            # wait 10 seconds (typical time between 2 blocks)
            await asyncio.sleep(10)


async def nuls_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)

        except Exception:
            LOGGER.exception("ERROR, relaunching in 10 seconds")
            await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, nuls_incoming_worker)
