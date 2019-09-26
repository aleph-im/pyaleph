import logging
import asyncio
from .pubsub import sub
from aleph.network import incoming_check
from aleph.chains.common import incoming
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