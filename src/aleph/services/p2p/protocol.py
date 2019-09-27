import logging
import asyncio
from libp2p.typing import TProtocol
from libp2p.network.stream.net_stream_interface import INetStream
from libp2p.network.stream.exceptions import StreamError
from .pubsub import sub
from aleph.network import incoming_check
from aleph.services.filestore import get_value
from concurrent import futures
from . import singleton
from . import peers
import orjson as json
import base64
import random

PROTOCOL_ID = TProtocol("/aleph/p2p/0.1.0")
MAX_READ_LEN = 2 ** 48 - 1

LOGGER = logging.getLogger('P2P.protocol')

STREAMS = dict()

CONNECT_LOCK = asyncio.Lock()

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
                    value = await get_value(message_json['hash'], in_executor=True)
                    if value is not None:
                        result = {'status': 'success',
                                  'hash': message_json['hash'],
                                  'content': base64.encodebytes(value).decode('utf-8')}
                    else:
                        result = {'status': 'success',
                                  'content': None}
                else:
                    result = {'status': 'error',
                              'reason': 'unknown command'}
                LOGGER.debug(f"received {read_string}")
            except Exception as e:
                result = {'status': 'error',
                          'reason': repr(e)}
            await stream.write(json.dumps(result))
        
        
async def stream_handler(stream: INetStream) -> None:
    asyncio.ensure_future(read_data(stream))
#             asyncio.ensure_future(write_data(stream))
    

async def make_request(request_structure, peer_id, timeout=2,
                       connect_timeout=.2, parallel_count=10):
    global STREAMS
    # global REQUESTS_SEM
    speer = str(peer_id)
    
    async with CONNECT_LOCK:
        streams = STREAMS.get(speer, list())
        try:
            if len(streams) < parallel_count:
                for i in range(parallel_count-len(streams)):
                    streams.append((await asyncio.wait_for(singleton.host.new_stream(peer_id, [PROTOCOL_ID]),
                                                            connect_timeout),
                                    asyncio.Semaphore(1)))
                STREAMS[speer] = streams
        except:
            LOGGER.info(f"Closing connection to {peer_id}")
            await singleton.host.get_network().close_peer(peer_id)
            if speer in STREAMS:
                del STREAMS[speer]
            return
    # except (futures.TimeoutError, StreamError, RuntimeError, OSError):
    #     return
    while True:
        for i, (stream, semaphore) in enumerate(streams):
            if not semaphore.locked():
                async with semaphore:
                    try:
                        # stream = await asyncio.wait_for(singleton.host.new_stream(peer_id, [PROTOCOL_ID]), connect_timeout)
                        await stream.write(json.dumps(request_structure))
                        value = await asyncio.wait_for(stream.read(MAX_READ_LEN), timeout)
                        # # await stream.close()
                        return json.loads(value)
                    except (StreamError, RuntimeError, OSError):
                        # let's delete this stream so it gets recreated next time
                        # await stream.close()
                        try:
                            STREAMS[speer].remove((stream, semaphore))
                        except ValueError:
                            pass # already removed
                        except KeyError:
                            return # all this peer gone bad
                        # STREAMS[speer].pop(i)
            await asyncio.sleep(0)
            
        if not len(streams):
            return
        


async def request_hash(item_hash, timeout=2,
                       connect_timeout=2, retries=2,
                       total_streams=100, max_per_host=5):
    # this should be done better, finding best peers to query from.
    query = {
        'command': 'hash_content',
        'hash': item_hash
    }
    qpeers = await peers.get_peers()
    random.shuffle(qpeers)
    qpeers = qpeers[:total_streams]
    for i in range(retries):
        for peer in qpeers:
            try:
                item = await make_request(query, peer,
                                          timeout=timeout, connect_timeout=connect_timeout,
                                          parallel_count=min(int(total_streams/len(qpeers)), max_per_host))
                if item is not None and item['status'] == 'success' and item['content'] is not None:
                    # TODO: IMPORTANT /!\ verify the hash of received data!
                    return base64.decodebytes(item['content'].encode('utf-8'))
                else:
                    LOGGER.debug(f"can't get hash {item_hash} from {peer}")
            except futures.TimeoutError:
                LOGGER.debug(f"can't get hash {item_hash} from {peer}")
                continue
            except:
                # Catch all with more info in case of weird error
                LOGGER.exception(f"can't get hash {item_hash} from {peer}")
                continue