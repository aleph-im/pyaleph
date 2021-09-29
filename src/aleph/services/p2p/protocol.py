import asyncio
import base64
import json
import logging
import random
from typing import Coroutine, List

from libp2p.network.exceptions import SwarmException
from libp2p.network.notifee_interface import INotifee
from libp2p.network.stream.exceptions import StreamError
from libp2p.network.stream.net_stream_interface import INetStream
from libp2p.typing import TProtocol

from aleph import __version__
from aleph.network import incoming_check
from . import singleton
from .pubsub import sub

PROTOCOL_ID = TProtocol("/aleph/p2p/0.1.0")
MAX_READ_LEN = 2 ** 32 - 1

LOGGER = logging.getLogger('P2P.protocol')

STREAM_COUNT = 5

HELLO_PACKET = {
    'command': 'hello'
}

CONNECT_LOCK = asyncio.Lock()

class AlephProtocol(INotifee):
    def __init__(self, host, streams_per_host=5):
        self.host = host
        self.streams_per_host = streams_per_host
        self.host.get_network().register_notifee(self)
        self.host.set_stream_handler(PROTOCOL_ID, self.stream_handler)
        self.peers = dict()
        
    async def stream_handler(self, stream: INetStream) -> None:
        asyncio.ensure_future(self.read_data(stream))
    
    async def read_data(self, stream: INetStream) -> None:
        from aleph.storage import get_hash_content
        while True:
            read_bytes = await stream.read(MAX_READ_LEN)
            if read_bytes is not None:
                result = {'status': 'error',
                        'reason': 'unknown'}
                try:
                    read_string = read_bytes.decode('utf-8')
                    message_json = json.loads(read_string)
                    if message_json['command'] == 'hash_content':
                        value = await get_hash_content(message_json['hash'], use_network=False, timeout=1)
                        if value is not None and value != -1:
                            result = {'status': 'success',
                                    'hash': message_json['hash'],
                                    'content': base64.encodebytes(value).decode('utf-8')}
                        else:
                            result = {'status': 'success',
                                    'content': None}
                    elif message_json['command'] == 'get_message':
                        result = {'status': 'error',
                                  'reason': 'not implemented'}
                    elif message_json['command'] == 'publish_message':
                        result = {'status': 'error',
                                  'reason': 'not implemented'}
                    elif message_json['command'] == 'hello':
                        result = {'status': 'success',
                                  'content': {
                                      'version': __version__
                                  }}
                    else:
                        result = {'status': 'error',
                                'reason': 'unknown command'}
                    LOGGER.debug(f"received {read_string}")
                except Exception as e:
                    result = {'status': 'error',
                            'reason': repr(e)}
                    LOGGER.exception("Error while reading data")
                await stream.write(json.dumps(result).encode('utf-8'))
                
    async def make_request(self, request_structure):
        streams = [(peer, item) for peer, sublist in self.peers.items() for item in sublist]
        random.shuffle(streams)
        while True:
            for i, (peer, (stream, semaphore)) in enumerate(streams):
                if not semaphore.locked():
                    async with semaphore:
                        try:
                            # stream = await asyncio.wait_for(singleton.host.new_stream(peer_id, [PROTOCOL_ID]), connect_timeout)
                            await stream.write(json.dumps(request_structure).encode('utf-8'))
                            value = await stream.read(MAX_READ_LEN)
                            # # await stream.close()
                            try:
                                value = json.loads(value)
                            except json.JSONDecodeError:
                                value = None
                                continue
                                
                            if value.get('content') is None:
                                # remove all streams from that peer, ask to the others.
                                for speer, info in list(streams):
                                    if speer == peer:
                                        streams.remove((speer, info))
                                break
                                
                            return value
                        except (StreamError):
                            # let's delete this stream so it gets recreated next time
                            # await stream.close()
                            await stream.reset()
                            streams.remove((peer, (stream, semaphore)))
                            try:
                                self.peers[peer].remove((stream, semaphore))
                            except ValueError:
                                pass
                            LOGGER.debug("Can't request hash...")
                await asyncio.sleep(0)
                
            if not len(streams):
                return
    
    async def request_hash(self, item_hash):
        # this should be done better, finding best peers to query from.
        query = {
            'command': 'hash_content',
            'hash': item_hash
        }
        item = await self.make_request(query)
        if item is not None and item['status'] == 'success' and item['content'] is not None:
            # TODO: IMPORTANT /!\ verify the hash of received data!
            return base64.decodebytes(item['content'].encode('utf-8'))
        else:
            LOGGER.debug(f"can't get hash {item_hash}")
                
    async def _handle_new_peer(self, peer_id) -> None:
        await self.create_connections(peer_id)
        LOGGER.debug("added new peer %s", peer_id)
        
    async def create_connections(self, peer_id):
        peer_streams = self.peers.get(peer_id, list())
        for i in range(self.streams_per_host - len(peer_streams)):
            try:
                stream: INetStream = await self.host.new_stream(peer_id, [PROTOCOL_ID])
            except SwarmException as error:
                LOGGER.debug("fail to add new peer %s, error %s", peer_id, error)
                return
            
            try:
                await stream.write(json.dumps(HELLO_PACKET).encode('utf-8'))
                await stream.read(MAX_READ_LEN)
            except Exception as error:
                LOGGER.debug("fail to add new peer %s, error %s", peer_id, error)
                return
            
            peer_streams.append((stream, asyncio.Semaphore(1)))
            # await asyncio.sleep(.1)
        
        self.peers[peer_id] = peer_streams
        
        
    async def opened_stream(self, network, stream) -> None:
        pass

    async def closed_stream(self, network, stream) -> None:
        pass

    async def connected(self, network, conn) -> None:
        """
        Add peer_id to initiator_peers_queue, so that this peer_id can be used to
        create a stream and we only want to have one pubsub stream with each peer.
        :param network: network the connection was opened on
        :param conn: connection that was opened
        """
        #await self.initiator_peers_queue.put(conn.muxed_conn.peer_id)
        peer_id = conn.muxed_conn.peer_id
        asyncio.ensure_future(self._handle_new_peer(peer_id))
        

    async def disconnected(self, network, conn) -> None:
        pass

    async def listen(self, network, multiaddr) -> None:
        pass

    async def listen_close(self, network, multiaddr) -> None:
        pass
    
    async def has_active_streams(self, peer_id):
        if peer_id not in self.peers:
            return False
        return bool(len(self.peers[peer_id]))

async def incoming_channel(config, topic):
    LOGGER.debug("incoming channel started...")
    from aleph.chains.common import delayed_incoming
    while True:
        try:
            async for mvalue in sub(topic):
                LOGGER.debug("Received from P2P:", mvalue)
                try:
                    message = json.loads(mvalue['data'])

                    # we should check the sender here to avoid spam
                    # and such things...
                    message = await incoming_check(mvalue)
                    if message is None:
                        continue
                    
                    LOGGER.debug("New message %r" % message)
                    await delayed_incoming(message)
                except Exception:
                    LOGGER.exception("Can't handle message")

        except Exception:
            LOGGER.exception("Exception in pubsub, reconnecting.")


async def request_hash(item_hash):
    if singleton.streamer is not None:
        return await singleton.streamer.request_hash(item_hash)
    else:
        return None

        

