from aleph.chains.common import incoming, validates
import binascii

LOGGER = logging.getLogger('chains.nuls')
CHAIN_NAME = 'NULS'

async def get_base_url(config):
    return config.nulsexplorer.url.value

async def get_last_height():
    """ Returns the last height for which we already have the nuls data.
    """
    return 0 # for now, request everything (bad!) TODO: Change that!

async def request_transactions(session, start_height):
    """ Continuously request data from the NULS blockchain.
    TODO: setup a websocket and push system.
    """
    check_url = '{}/transactions.json'.format(await get_base_url())

    async with session.get(check_url, params={
        'type': '10',
        'startHeight': start_height,
        'pagination': 100000 # TODO: handle pagination correctly!
    }) as resp:
        jres = await resp.json()
        for tx in jres['transactions']:
            ldata = tx['info'].get('logicData')
            try:
                ddata = binascii.unhexlify(ldata).decode('utf-8')
                jdata = json.loads(ddata)
                if jdata.get('protocol', None) != 'aleph':
                    continue
                if jdata.get('version', None) != 1:
                    continue # unsupported protocol version

                yield dict(height=tx['blockHeight'], hashes=jdata['content']['hashes'])

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
            async for tx in request_transactions(session, last_stored_height):
                for hash in tx['hashes']:
                    await incoming(CHAIN_NAME, hash)

                if tx['height'] > last_stored_height:
                    last_stored_height = tx['height']

            time.sleep(10) # wait 10 seconds (typical time between 2 blocks)



async def nuls_incoming_worker(config):
    while True:
        try:
            await check_incoming(config)
        except:
            LOGGER.exception("ERROR, relaunching in 10 seconds")
            await asyncio.sleep(10)
