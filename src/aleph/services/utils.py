import socket

import aiohttp


async def get_IP():
    ip = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://v4.ident.me/") as resp:
                ip = await resp.text()
    except:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()

    return ip
