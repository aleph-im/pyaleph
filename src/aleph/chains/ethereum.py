import asyncio
import functools
import json
import logging
from typing import AsyncIterator, Dict, Tuple

import pkg_resources
from eth_account import Account
from eth_account.messages import encode_defunct
from hexbytes import HexBytes
from web3 import Web3
from web3._utils.events import get_event_data
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.middleware.filter import local_filter_middleware
from web3.middleware.geth_poa import geth_poa_middleware

from aleph.chains.common import (
    get_verification_buffer,
    get_chaindata,
    incoming_chaindata,
)
from aleph.model.chains import Chain
from aleph.model.messages import Message
from aleph.model.pending import pending_messages_count, pending_txs_count
from aleph.register_chain import (
    register_verifier,
    register_incoming_worker,
    register_outgoing_worker,
)
from aleph.utils import run_in_executor
from .tx_context import TxContext
from ..schemas.pending_messages import BasePendingMessage

LOGGER = logging.getLogger("chains.ethereum")
CHAIN_NAME = "ETH"


async def verify_signature(message: BasePendingMessage) -> bool:
    """Verifies a signature of a message, return True if verified, false if not"""

    # w3 = await loop.run_in_executor(None, get_web3, config)

    verification = await get_verification_buffer(message)

    message_hash = await run_in_executor(
        None, functools.partial(encode_defunct, text=verification.decode("utf-8"))
    )
    # message_hash = encode_defunct(text=verification.decode('utf-8'))
    # await asyncio.sleep(0)
    verified = False
    try:
        # we assume the signature is a valid string
        address = await run_in_executor(
            None,
            functools.partial(
                Account.recover_message, message_hash, signature=message.signature
            ),
        )
        # address = Account.recover_message(message_hash, signature=message['signature'])
        # await asyncio.sleep(0)
        # address = await loop.run_in_executor(
        #     None,
        #     functools.partial(w3.eth.account.recoverHash, message_hash,
        #                       signature=message['signature']))
        # address = w3.eth.account.recoverHash(message_hash,
        #                                      signature=message['signature'])
        if address == message.sender:
            verified = True
        else:
            LOGGER.warning(
                "Received bad signature from %s for %s" % (address, message.sender)
            )
            return False

    except Exception as e:
        LOGGER.exception("Error processing signature for %s" % message.sender)
        verified = False

    return verified


register_verifier(CHAIN_NAME, verify_signature)


async def get_last_height():
    """Returns the last height for which we already have the ethereum data."""
    last_height = await Chain.get_last_height(CHAIN_NAME)

    if last_height is None:
        last_height = -1

    return last_height


def get_web3(config) -> Web3:
    web3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
    if config.ethereum.chain_id.value == 4:  # rinkeby
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    web3.middleware_onion.add(local_filter_middleware)
    web3.eth.setGasPriceStrategy(rpc_gas_price_strategy)

    return web3


async def get_contract_abi():
    return json.loads(
        pkg_resources.resource_string(
            "aleph.chains", "assets/ethereum_sc_abi.json"
        ).decode("utf-8")
    )


async def get_contract(config, web3: Web3):
    return web3.eth.contract(
        config.ethereum.sync_contract.value, abi=await get_contract_abi()
    )


async def get_logs_query(web3: Web3, contract, start_height, end_height):
    logs = await run_in_executor(
        None,
        web3.eth.getLogs,
        {"address": contract.address, "fromBlock": start_height, "toBlock": end_height},
    )
    for log in logs:
        yield log


async def get_logs(config, web3: Web3, contract, start_height):
    try:
        logs = get_logs_query(web3, contract, start_height + 1, "latest")
        async for log in logs:
            yield log
    except ValueError as e:
        # we got an error, let's try the pagination aware version.
        if e.args[0]["code"] != -32005:
            return

        last_block = await asyncio.get_event_loop().run_in_executor(
            None, web3.eth.get_block_number
        )
        if start_height < config.ethereum.start_height.value:
            start_height = config.ethereum.start_height.value

        end_height = start_height + 1000

        while True:
            try:
                logs = get_logs_query(web3, contract, start_height, end_height)
                async for log in logs:
                    yield log

                start_height = end_height + 1
                end_height = start_height + 1000

                if start_height > last_block:
                    LOGGER.info("Ending big batch sync")
                    break

            except ValueError as e:
                if e.args[0]["code"] == -32005:
                    end_height = start_height + 100
                else:
                    raise


async def request_transactions(
    config, web3: Web3, contract, abi, start_height
) -> AsyncIterator[Tuple[Dict, TxContext]]:
    """Continuously request data from the Ethereum blockchain.
    TODO: support websocket API.
    """

    logs = get_logs(config, web3, contract, start_height + 1)

    async for log in logs:
        event_data = await run_in_executor(None, get_event_data, web3.codec, abi, log)
        # event_data = get_event_data(web3.codec,
        #                             contract.events.SyncEvent._get_event_abi(),
        #                             log)
        LOGGER.info("Handling TX in block %s" % event_data.blockNumber)
        publisher = event_data.args.addr
        timestamp = event_data.args.timestamp

        if publisher not in config.ethereum.authorized_emitters.value:
            LOGGER.info(
                "TX with unauthorized emitter %s in block %s"
                % (publisher, event_data.blockNumber)
            )
            continue

        last_height = event_data.blockNumber
        # block = await loop.run_in_executor(None, web3.eth.getBlock, event_data.blockNumber)
        # timestamp = block.timestamp

        message = event_data.args.message
        try:
            jdata = json.loads(message)
            context = TxContext(
                chain_name=CHAIN_NAME,
                tx_hash=event_data.transactionHash.hex(),
                time=timestamp,
                height=event_data.blockNumber,
                publisher=publisher,
            )
            yield jdata, context

        except json.JSONDecodeError:
            # if it's not valid json, just ignore it...
            LOGGER.info("Incoming logic data is not JSON, ignoring. %r" % message)

        except Exception:
            LOGGER.exception("Can't decode incoming logic data %r" % message)

        # Since we got no critical exception, save last received object
        # block height to do next requests from there.
        if last_height:
            await Chain.set_last_height(CHAIN_NAME, last_height)


async def check_incoming(config):
    last_stored_height = await get_last_height()

    LOGGER.info("Last block is #%d" % last_stored_height)

    web3 = await run_in_executor(None, get_web3, config)
    contract = await get_contract(config, web3)
    abi = contract.events.SyncEvent._get_event_abi()

    while True:
        last_stored_height = await get_last_height()
        async for jdata, context in request_transactions(
            config, web3, contract, abi, last_stored_height
        ):
            await incoming_chaindata(jdata, context)
        await asyncio.sleep(10)


async def ethereum_incoming_worker(config):
    if config.ethereum.enabled.value:
        while True:
            try:
                await check_incoming(config)

            except Exception:
                LOGGER.exception("ERROR, relaunching incoming in 10 seconds")
                await asyncio.sleep(10)


register_incoming_worker(CHAIN_NAME, ethereum_incoming_worker)


def broadcast_content(config, contract, web3: Web3, account, gas_price, nonce, content):
    # content = json.dumps(content)
    tx = contract.functions.doEmit(content).buildTransaction(
        {
            "chainId": config.ethereum.chain_id.value,
            "gasPrice": gas_price,
            "nonce": nonce,
        }
    )
    signed_tx = account.signTransaction(tx)
    return web3.eth.sendRawTransaction(signed_tx.rawTransaction)


async def ethereum_packer(config):
    web3 = await run_in_executor(None, get_web3, config)
    contract = await get_contract(config, web3)

    pri_key = HexBytes(config.ethereum.private_key.value)
    account = Account.privateKeyToAccount(pri_key)
    address = account.address

    LOGGER.info("Ethereum Connector set up with address %s" % address)
    i = 0
    gas_price = web3.eth.generateGasPrice()
    while True:
        if (await pending_txs_count(chain=CHAIN_NAME)) or (
            await pending_messages_count(source_chain=CHAIN_NAME)
        ) > 1000:
            await asyncio.sleep(30)
            continue
        gas_price = web3.eth.generateGasPrice()

        if i >= 100:
            await asyncio.sleep(30)  # wait three (!!) blocks
            gas_price = web3.eth.generateGasPrice()
            # utxo = await get_utxo(config, address)
            i = 0

        if gas_price > config.ethereum.max_gas_price.value:
            # gas price too high, wait a bit and retry.
            await asyncio.sleep(60)
            continue

        nonce = web3.eth.getTransactionCount(account.address)

        messages = [
            message
            async for message in (
                await Message.get_unconfirmed_raw(limit=10000, for_chain=CHAIN_NAME)
            )
        ]

        if len(messages):
            content = await get_chaindata(messages, bulk_threshold=200)
            response = await run_in_executor(
                None,
                broadcast_content,
                config,
                contract,
                web3,
                account,
                int(gas_price * 1.1),
                nonce,
                content,
            )
            LOGGER.info("Broadcasted %r on %s" % (response, CHAIN_NAME))

        await asyncio.sleep(config.ethereum.commit_delay.value)
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


async def get_token_contract(config, web3: Web3):
    return web3.eth.contract(
        config.ethereum.sync_contract.value, abi=await get_contract_abi()
    )
