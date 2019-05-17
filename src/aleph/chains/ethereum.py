import asyncio
import json
import pkg_resources
from aleph.network import check_message
from aleph.chains.common import (incoming, get_verification_buffer,
                                 get_chaindata,
                                 get_chaindata_messages)
from aleph.chains.register import (
    register_verifier, register_incoming_worker, register_outgoing_worker)
from aleph.model.chains import Chain
from aleph.model.messages import Message

from web3 import Web3
from web3.middleware import geth_poa_middleware, local_filter_middleware
from web3.contract import get_event_data
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from eth_account.messages import defunct_hash_message
from eth_account import Account
from hexbytes import HexBytes
import functools

import logging
LOGGER = logging.getLogger('chains.ethereum')
CHAIN_NAME = 'ETH'


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    loop = asyncio.get_event_loop()
    from aleph.web import app
    config = app.config
    w3 = await get_web3(config)

    verification = await get_verification_buffer(message)

    message_hash = defunct_hash_message(text=verification.decode('utf-8'))

    verified = False
    try:
        # we assume the signature is a valid string
        address = await loop.run_in_executor(
            None,
            functools.partial(w3.eth.account.recoverHash, message_hash,
                              signature=message['signature']))
        # address = w3.eth.account.recoverHash(message_hash,
        #                                      signature=message['signature'])
        if address == message['sender']:
            verified = True
        else:
            LOGGER.warning('Received bad signature from %s for %s'
                           % (address, message['sender']))
            return False

    except Exception as e:
        LOGGER.exception('Error processing signature from %s for %s'
                         % (address, message['sender']))
        verified = False

    return verified

register_verifier(CHAIN_NAME, verify_signature)


async def get_last_height():
    """ Returns the last height for which we already have the ethereum data.
    """
    last_height = await Chain.get_last_height(CHAIN_NAME)

    if last_height is None:
        last_height = -1

    return last_height


async def get_web3(config):
    web3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
    if config.ethereum.chain_id.value == 4:  # rinkeby
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    web3.middleware_onion.add(local_filter_middleware)
    web3.eth.setGasPriceStrategy(rpc_gas_price_strategy)

    return web3


async def get_contract_abi():
    return json.loads(pkg_resources.resource_string(
        'aleph.chains',
        'assets/ethereum_sc_abi.json').decode('utf-8'))


async def get_contract(config, web3):
    return web3.eth.contract(config.ethereum.sync_contract.value,
                             abi=await get_contract_abi())


async def get_logs_query(web3, contract, start_height, end_height):
    loop = asyncio.get_event_loop()
    logs = await loop.run_in_executor(None, web3.eth.getLogs,
                                      {'address': contract.address,
                                       'fromBlock': start_height,
                                       'toBlock': end_height})
    for log in logs:
        yield log


async def get_logs(config, web3, contract, start_height):
    try:
        logs = get_logs_query(web3, contract,
                              start_height+1, 'latest')
        async for log in logs:
            yield log
    except ValueError as e:
        # we got an error, let's try the pagination aware version.
        if e.args[0]['code'] != -32005:
            return

        last_block = web3.eth.blockNumber
        if (start_height < config.ethereum.start_height.value):
            start_height = config.ethereum.start_height.value

        end_height = start_height + 1000

        while True:
            try:
                logs = get_logs_query(web3, contract,
                                      start_height, end_height)
                async for log in logs:
                    yield log

                start_height = end_height + 1
                end_height = start_height + 1000

                if start_height > last_block:
                    LOGGER.info("Ending big batch sync")
                    break

            except ValueError as e:
                if e.args[0]['code'] == -32005:
                    end_height = start_height + 100
                else:
                    raise


async def request_transactions(config, web3, contract, start_height):
    """ Continuously request data from the Ethereum blockchain.
    TODO: support websocket API.
    """

    last_height = 0
    seen_ids = []

    logs = get_logs(config, web3, contract, start_height+1)

    async for log in logs:
        event_data = get_event_data(contract.events.SyncEvent._get_event_abi(),
                                    log)
        LOGGER.info('Handling TX in block %s' % event_data.blockNumber)
        publisher = event_data.args.addr  # TODO: verify rights.

        last_height = event_data.blockNumber

        message = event_data.args.message
        try:
            jdata = json.loads(message)

            messages = await get_chaindata_messages(
                jdata, seen_ids=seen_ids, context={
                    "tx_hash": event_data.transactionHash,
                    "height": event_data.blockNumber,
                    "publisher": publisher
                })

            if messages is not None:
                yield dict(type="aleph",
                           tx_hash=event_data.transactionHash,
                           height=event_data.blockNumber,
                           publisher=publisher,
                           messages=messages)

        except json.JSONDecodeError:
            # if it's not valid json, just ignore it...
            LOGGER.info("Incoming logic data is not JSON, ignoring. %r"
                        % message)

        except Exception:
            LOGGER.exception("Can't decode incoming logic data %r"
                             % message)

        # Since we got no critical exception, save last received object
        # block height to do next requests from there.
        if last_height:
            await Chain.set_last_height(CHAIN_NAME, last_height)


async def check_incoming(config):
    last_stored_height = await get_last_height()

    LOGGER.info("Last block is #%d" % last_stored_height)
    loop = asyncio.get_event_loop()

    web3 = await get_web3(config)
    contract = await get_contract(config, web3)

    while True:
        last_stored_height = await get_last_height()
        i = 0
        j = 0

        tasks = []
        seen_ids = []
        async for txi in request_transactions(config, web3, contract,
                                              last_stored_height):
            i += 1
            # TODO: handle big message list stored in IPFS case
            # (if too much messages, an ipfs hash is stored here).
            for message in txi['messages']:
                j += 1
                # message = await check_message(message, from_chain=True)
                # if message is None:
                #     # message got discarded at check stage.
                #     continue

                # message['time'] = txi['time']

                # running those separately... a good/bad thing?
                # shouldn't do that for VMs.
                tasks.append(
                    loop.create_task(incoming(
                        message, chain_name=CHAIN_NAME,
                        seen_ids=seen_ids,
                        tx_hash=txi['tx_hash'],
                        height=txi['height'],
                        check_message=True)))

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
                await task  # let's wait for all tasks to end.
            except Exception:
                LOGGER.exception("error in incoming task")

        if (i < 10):  # if there was less than 10 items, not a busy time
            # wait 5 seconds (half of typical time between 2 blocks)
            await asyncio.sleep(5)


async def ethereum_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)

        except Exception:
            LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
            await asyncio.sleep(10)

register_incoming_worker(CHAIN_NAME, ethereum_incoming_worker)


def broadcast_content(config, contract, web3, account,
                      gas_price, nonce, content):
    content = json.dumps(content)
    tx = contract.functions.doEmit(content).buildTransaction({
            'chainId': config.ethereum.chain_id.value,
            'gasPrice': gas_price,
            'nonce': nonce,
            })
    signed_tx = account.signTransaction(tx)
    return web3.eth.sendRawTransaction(signed_tx.rawTransaction)


async def ethereum_packer(config):
    web3 = await get_web3(config)
    contract = await get_contract(config, web3)
    loop = asyncio.get_event_loop()

    pri_key = HexBytes(config.ethereum.private_key.value)
    account = Account.privateKeyToAccount(pri_key)
    address = account.address

    LOGGER.info("Ethereum Connector set up with address %s" % address)
    i = 0
    gas_price = web3.eth.generateGasPrice()
    while True:
        if i >= 100:
            await asyncio.sleep(30)  # wait three (!!) blocks
            gas_price = web3.eth.generateGasPrice()
            # utxo = await get_utxo(config, address)
            i = 0

        nonce = web3.eth.getTransactionCount(account.address)

        messages = [message async for message
                    in (await Message.get_unconfirmed_raw(
                            limit=5000,
                            for_chain=CHAIN_NAME))]

        if len(messages):
            content = await get_chaindata(messages)
            response = await loop.run_in_executor(None, broadcast_content,
                                                  config, contract, web3,
                                                  account, gas_price, nonce,
                                                  content)
            LOGGER.info("Broadcasted %r on %s" % (response, CHAIN_NAME))

        await asyncio.sleep(35)
        i += 1


async def ethereum_outgoing_worker(config):
    if config.ethereum.packing_node.value:
        while True:
            try:
                await ethereum_packer(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching outgoing in 10 seconds")
                await asyncio.sleep(10)

register_outgoing_worker(CHAIN_NAME, ethereum_outgoing_worker)
