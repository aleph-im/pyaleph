import asyncio
import json
import math
import pytz
import dateutil
import dateutil.parser
from datetime import datetime, timezone, timedelta
from operator import itemgetter

from binance_chain.wallet import Wallet
from binance_chain.environment import BinanceEnvironment
from binance_chain.http import AsyncHttpApiClient
from binance_chain.messages import TransferMsg
from binance_chain.websockets import BinanceChainSocketManager

from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata, get_chaindata_messages,
                                 join_tasks, incoming_chaindata)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

import logging
LOGGER = logging.getLogger('chains.binance')
CHAIN_NAME = 'BNB'
PAGINATION = 500


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified
    """
    raise NotImplementedError("BNB Signature not implemented yet!")

register_verifier(CHAIN_NAME, verify_signature)


async def get_last_height():
    """ Returns the last height for which we already have the nuls data.
    """
    last_height = await Chain.get_last_height(CHAIN_NAME)

    if last_height is None:
        last_height = -1

    return last_height


async def prepare_timestamp(dt):
    if dt is None:
        return None

    return int(datetime.timestamp(dt)*1000)


async def get_transactions(config, client, target_addr,
                           start_time, end_time=None):
    if start_time is None:
        start_time = config.binancechain.start_time.value
        start_time = dateutil.parser.parse(start_time)

    if end_time is None:
        if (datetime.now(timezone.utc) - start_time) \
                > timedelta(days=85):
            end_time = start_time + timedelta(days=85)

    result = await client.get_transactions(
        target_addr, limit=PAGINATION,
        start_time=(await prepare_timestamp(start_time))+1,
        end_time=await prepare_timestamp(end_time))

    if result['total'] >= PAGINATION:
        if end_time is None:
            # If we have no end_time, we ensure we have a non-slipping range
            # while we iterate.
            end_time = dateutil.parser.parse(result['tx'][0]['timeStamp'])

        for i in reversed(range(math.ceil(result['total']/PAGINATION))):
            nresult = await client.get_transactions(
                target_addr, limit=PAGINATION, offset=i*PAGINATION,
                start_time=(await prepare_timestamp(start_time))+1,
                end_time=await prepare_timestamp(end_time))
            for tx in sorted(nresult['tx'], key=itemgetter('blockHeight')):
                yield tx
    else:
        for tx in sorted(result['tx'], key=itemgetter('blockHeight')):
            yield tx

    # if result['total'] >= PAGINATION:
    #     for i in range(1, math.ceil(result['total']/PAGINATION)):
    #         nresult = await client.get_transactions(
    #             target_addr, limit=PAGINATION, offset=i*PAGINATION,
    #             start_time=(await prepare_timestamp(start_time))+1,
    #             end_time=(end_time is not None and (await prepare_timestamp(end_time))
    #                       or None))
    #         for tx in nresult['tx']:
    #             yield tx


async def request_transactions(config, client, start_time):
    loop = asyncio.get_event_loop()
    # TODO: testnet perhaps? When we get testnet coins.

    target_addr = config.binancechain.sync_address.value

    last_time = None
    async for tx in get_transactions(config, client,
                                     target_addr, start_time):
        ldata = tx['memo']
        LOGGER.info('Handling TX in block %s' % tx['blockHeight'])
        try:
            tx_time = dateutil.parser.parse(tx['timeStamp']).timestamp()
            last_time = dateutil.parser.parse(tx['timeStamp'])
            jdata = json.loads(ldata)
            
            context = {"chain_name": CHAIN_NAME,
                       "tx_hash": tx['txHash'],
                       "height": tx['blockHeight'],
                       "time": tx_time,
                       "publisher": tx["fromAddr"]}
            yield (jdata, context)

        except json.JSONDecodeError:
            # if it's not valid json, just ignore it...
            LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
                        % ldata)

    if last_time:
        await Chain.set_last_time(CHAIN_NAME, last_time)

async def check_incoming(config):
    last_stored_time = await Chain.get_last_time(CHAIN_NAME)

    LOGGER.info("Last time is %s" % last_stored_time)
    loop = asyncio.get_event_loop()
    env = BinanceEnvironment.get_production_env()
    client = AsyncHttpApiClient(env=env)

    while True:
        last_stored_time = await Chain.get_last_time(CHAIN_NAME)
        i = 0
        j = 0
        
        async for jdata, context in request_transactions(config, client,
                                              last_stored_time):
            
            await incoming_chaindata(jdata, context)
            await Chain.set_last_time(
                CHAIN_NAME,
                datetime.fromtimestamp(context['time'], tz=pytz.utc))
            
        # print(i)
        if (i < 10):  # if there was less than 10 items, not a busy time
            await asyncio.sleep(2)


async def binance_incoming_worker(config):
    if config.binancechain.enabled.value:
        while True:
            try:
                await check_incoming(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
                await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, binance_incoming_worker)


def prepare_transfer_tx(wallet, target_addr, memo_bytes):
    tx = TransferMsg(
        wallet=wallet,
        symbol='BNB',
        amount=0.0001,
        to_address=target_addr,  # send to target
        memo=memo_bytes
    )
    return tx

async def binance_packer(config):
    loop = asyncio.get_event_loop()
    # TODO: testnet perhaps? When we get testnet coins.
    env = BinanceEnvironment.get_production_env()
    target_addr = config.binancechain.sync_address.value

    client = AsyncHttpApiClient(env=env)
    wallet = Wallet(config.binancechain.private_key.value, env=env)
    LOGGER.info("BNB Connector set up with address %s" % wallet.address)
    try:
        await loop.run_in_executor(None, wallet.reload_account_sequence)
    except KeyError:
        pass

    i = 0
    while True:
        if (i >= 100):
            try:
                await loop.run_in_executor(None, wallet.reload_account_sequence)
            except KeyError:
                pass
            # utxo = await get_utxo(config, address)
            i = 0

        messages = [message async for message
                    in (await Message.get_unconfirmed_raw(
                            limit=100000, for_chain=CHAIN_NAME))]
        if len(messages):
            content = await get_chaindata(messages, bulk_threshold=0)
            # content = json.dumps(content)
            tx = await loop.run_in_executor(None, prepare_transfer_tx,
                                            wallet, target_addr,
                                            content)
            # tx_hash = await tx.get_hash()
            LOGGER.info("Broadcasting TX")
            await client.broadcast_msg(tx, sync=True)

        await asyncio.sleep(config.binancechain.commit_delay.value)

        i += 1


async def binance_outgoing_worker(config):
    if config.binancechain.packing_node.value:
        while True:
            try:
                await binance_packer(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching outgoing in 10 seconds")
                await asyncio.sleep(10)

register_outgoing_worker(CHAIN_NAME, binance_outgoing_worker)
