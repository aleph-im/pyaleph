import asyncio
from aiohttp import web
import socketio
from aleph.web import sio
from aleph.model.messages import Message
import collections
import logging
LOGGER = logging.getLogger("LISTENER-SOCKETIO")

async def broadcast():
    db = Message.collection
    last_ids = collections.deque(maxlen=100)
    while True:
        try:
            i = 0
            async for item in db.find().sort([('$natural', -1)]).limit(10):
                item['_id'] = str(item['_id'])
                if item['_id'] in last_ids:
                    continue

                last_ids.append(item['_id'])
                await sio.emit("message", item, room=item['channel'])
                i+=1
            await asyncio.sleep(.1)

        except Exception:
            LOGGER.exception("Error processing")
            await asyncio.sleep(.1)

@sio.event
async def join(sid, message):
    print("Client %s joined room %s" % (sid, message['room']))
    sio.enter_room(sid, message['room'])


@sio.event
async def leave(sid, message):
    print("Client %s left room %s" % (sid, message['room']))
    sio.leave_room(sid, message['room'])


@sio.event
async def disconnect_request(sid):
    await sio.disconnect(sid)

@sio.event
def disconnect(sid):
    print('Client disconnected')