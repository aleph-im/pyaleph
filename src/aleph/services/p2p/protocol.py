import logging
import asyncio
from libp2p.typing import TProtocol
from libp2p.network.stream.net_stream_interface import INetStream
from .pubsub import sub
from aleph.network import incoming_check
from aleph.chains.common import incoming
from aleph.services.filestore import get_value
import orjson as json
import base64

PROTOCOL_ID = TProtocol("/aleph/p2p/0.1.0")
MAX_READ_LEN = 2 ** 32 - 1

LOGGER = logging.getLogger('P2P.protocol')

async def incoming_channel(config, topic):
    from aleph.chains.common import incoming
    loop = asyncio.get_event_loop()
    while True:
        try:
            i = 0
            tasks = []
            async for mvalue in sub(topic):
                try:
                    message = json.loads(mvalue['data'])

                    # we should check the sender here to avoid spam
                    # and such things...
                    message = await incoming_check(mvalue)
                    if message is None:
                        continue
                    
                    LOGGER.debug("New message %r" % message)
                    i += 1
                    tasks.append(
                        loop.create_task(incoming(message)))

                    # await incoming(message, seen_ids=seen_ids)
                    if (i > 1000):
                        # every 1000 message we check that all tasks finished
                        # and we reset the seen_ids list.
                        for task in tasks:
                            await task
                        tasks = []
                        i = 0
                except:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")
            
async def read_data(stream: INetStream) -> None:
    while True:
        read_bytes = await stream.read(MAX_READ_LEN)
        if read_bytes is not None:
            result = {'status': 'error',
                      'reason': 'unknown'}
            try:
                read_string = read_bytes.decode('utf-8')
                message_json = json.loads(read_string)
                if message_json['command'] == 'hash_content':
                    value = await get_value(message_json['hash'])
                    if value is not None:
                        result = {'status': 'success',
                                  'content': base64.encodebytes(value).encode('utf-8')}
                    else:
                        result = {'status': 'success',
                                  'content': None}
                else:
                    result = {'status': 'error',
                              'reason': 'unknown command'}
                print(f"received {read_string}")
            except Exception as e:
                result = {'status': 'error',
                          'reason': repr(e)}
            await stream.write(json.dumps(result))
        
async def stream_handler(stream: INetStream) -> None:
    asyncio.ensure_future(read_data(stream))
#             asyncio.ensure_future(write_data(stream))
    