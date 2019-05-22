import asyncio
import json
import math
from datetime.datetime import timestamp
import dateutil

from binance_chain_python.Client import Client
from binance_chain.wallet import Wallet
from binance_chain.environment import BinanceEnvironment
from binance_chain.http import AsyncHttpApiClient
from binance_chain.messages import TransferMsg
from binance_chain.websockets import BinanceChainSocketManager

from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata, get_chaindata_messages)
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


async def get_transactions(client, start_time):
    stime = int(timestamp(dateutil.parser.parse(start_time))*1000+1)
    result = await client.get_transactions(
        target_addr, limit=PAGINATION,
        start_time=stime)

    for tx in result['tx']:
        yield tx

    if result['total'] >= PAGINATION:
        for i in range(1, math.ceil(result['total']/PAGINATION)):
            nresult = await client.get_transactions(target_addr,
                                                    limit=PAGINATION,
                                                    offset=i*PAGINATION,
                                                    start_time=stime)
            for tx in nresult['tx']:
                yield tx



async def request_transactions(config, start_time):
    loop = asyncio.get_event_loop()
    # TODO: testnet perhaps? When we get testnet coins.
    env = BinanceEnvironment.get_production_env()

    target_addr = config.binancechain.sync_address.value
    client = AsyncHttpApiClient(env=env)

    while True:
        last_time = 0
        i = 0
        async for tx in get_transactions(client, start_time):
            if i == 0:
                last_time = tx['timeStamp']


            i += 1

        if last_time:
            await Chain.set_last_time(CHAIN_NAME, last_time)



async def check_incoming(config):
    last_stored_time = await Chain.get_last_time(CHAIN_NAME)

    LOGGER.info("Last block is #%d" % last_stored_height)
    loop = asyncio.get_event_loop()

    while True:
        last_stored_time = await Chain.get_last_time(CHAIN_NAME)
        i = 0
        j = 0

        tasks = []
        seen_ids = []
        async for txi in request_transactions(config,
                                              last_stored_time):
            i += 1
            # TODO: handle big message list stored in IPFS case
            # (if too much messages, an ipfs hash is stored here).
            for message in txi['messages']:
                j += 1
                # message = await check_message(
                #     message, from_chain=True,
                #     trusted=(txi['type'] == 'native-single'))
                # if message is None:
                #     # message got discarded at check stage.
                #     continue

                message['time'] = txi['time']

                # running those separately... a good/bad thing?
                # shouldn't do that for VMs.
                tasks.append(
                    loop.create_task(incoming(
                        message, chain_name=CHAIN_NAME,
                        seen_ids=seen_ids,
                        tx_hash=txi['tx_hash'],
                        height=txi['height'],
                        check_message=(txi['type'] != 'native-single'))))

                # let's join every 500 messages...
                if (j > 500):
                    for task in tasks:
                        try:
                            await task
                        except Exception:
                            LOGGER.exception("error in incoming task")
                    j = 0
                    seen_ids = []
                    tasks = []

        for task in tasks:
            try:
                await task # let's wait for all tasks to end.
            except Exception:
                LOGGER.exception("error in incoming task")

        if (i < 10):  # if there was less than 10 items, not a busy time
            # wait 5 seconds (half of typical time between 2 blocks)
            await asyncio.sleep(5)


async def nuls_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)

        except Exception:
            LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
            await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, nuls_incoming_worker)


def prepare_transfer_tx(wallet, memo_bytes):
    tx = TransferMsg(
        wallet=wallet,
        symbol='BNB',
        amount=0.0001,
        to_address=wallet.address,  # send to ourselves
        memo=memo_bytes
    )
    return tx

async def binance_packer(config):
    loop = asyncio.get_event_loop()
    # TODO: testnet perhaps? When we get testnet coins.
    env = BinanceEnvironment.get_production_env()

    client = AsyncHttpApiClient(env=env)
    wallet = Wallet(config.binancechain.private_key.value, env=env)
    LOGGER.info("BNB Connector set up with address %s" % wallet.address)
    await loop.run_in_executor(None, wallet.reload_account_sequence)
    i = 0
    while True:
        if (i >= 100):
            await loop.run_in_executor(None, wallet.reload_account_sequence)
            # utxo = await get_utxo(config, address)
            i = 0

        messages = [message async for message
                    in (await Message.get_unconfirmed_raw(
                            limit=10, for_chain=CHAIN_NAME))]
        if len(messages):
            content = await get_chaindata(messages, bulk_threshold=0)
            content = json.dumps(content)
            tx = await loop.run_in_executor(None, prepare_transfer_tx,
                                            wallet, content.encode('utf-8'))
            # tx_hash = await tx.get_hash()
            LOGGER.info("Broadcasting TX")
            await client.broadcast_msg(tx, sync=True)

        await asyncio.sleep(35)

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
