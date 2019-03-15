import binascii
import asyncio
import aiohttp
import time
import json
from operator import itemgetter
from aleph.chains.common import incoming, invalidate, get_verification_buffer


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

async def get_base_url(config):
    return config.nulsexplorer.url.value

async def get_last_height():
    """ Returns the last height for which we already have the nuls data.
    """
    return 0 # for now, request everything (bad!) TODO: Change that!

async def request_transactions(config, session, start_height):
    """ Continuously request data from the NULS blockchain.
    TODO: setup a websocket and push system.
    """
    check_url = '{}/transactions.json'.format(await get_base_url(config))

    async with session.get(check_url, params={
        'type': '10',
        'startHeight': start_height,
        'pagination': 100000 # TODO: handle pagination correctly!
    }) as resp:
        jres = await resp.json()
        for tx in sorted(jres['transactions'], key=itemgetter('blockHeight')):
            ldata = tx['info'].get('logicData')
            try:
                ddata = binascii.unhexlify(ldata).decode('utf-8')
                jdata = json.loads(ddata)
                if jdata.get('protocol', None) != 'aleph':
                    LOGGER.info('Got unknown protocol object in tx %s' % tx['hash'])
                    continue
                if jdata.get('version', None) != 1:
                    LOGGER.info('Got an unsupported version object in tx %s' % tx['hash'])
                    continue # unsupported protocol version

                yield dict(height=tx['blockHeight'], messages=jdata['content']['messages'])

            except Exception as exc:
                LOGGER.exception("Can't decode incoming logic data %r" % ldata)

async def check_incoming(config):
    last_stored_height = await get_last_height()
    last_height = -1
    if last_stored_height is None:
        last_stored_height = -1

    big_batch = False
    LOGGER.info("Last block is #%d" % last_stored_height)

    async with aiohttp.ClientSession() as session:
        while True:
            async for tx in request_transactions(config, session, last_stored_height):
                # TODO: handle big message list stored in IPFS case (if too much messages, an ipfs hash is stored here).
                for message in tx['messages']:
                    # TODO: handle other chain signatures here
                    signed = await verify_signature(message)
                    if signed:
                        await incoming(CHAIN_NAME, message)

                if tx['height'] > last_stored_height:
                    last_stored_height = tx['height']

            await asyncio.sleep(10) # wait 10 seconds (typical time between 2 blocks)

async def nuls_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)
        except:
            LOGGER.exception("ERROR, relaunching in 10 seconds")
            await asyncio.sleep(10)
