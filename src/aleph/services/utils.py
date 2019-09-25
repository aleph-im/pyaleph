import aiohttp
import socket

async def get_IP():
    ip = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('https://v4.ident.me/.json') as resp:
                ip = (await resp.json())['address']
    except:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    
    return ip