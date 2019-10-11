""" While our own streamer libp2p protocol is still unstable, use direct
HTTP connection to standard rest API.
"""

import aiohttp

async def api_get_request(base_uri, method, timeout=1):
    async with aiohttp.ClientSession(read_timeout=timeout) as session:
        uri = f"{base_uri}/api/v0/{method}"
        try:
            async with session.get(uri) as resp:
                if resp.status != 200:
                    result = None
                else:
                    result = await resp.json()
        except:
            result = None
        return result
