import logging
import re
import socket

import aiohttp

logger = logging.getLogger(__name__)

IP4_SERVICE_URL = "https://v4.ident.me/"
IP4_SOCKET_ENDPOINT = "8.8.8.8"


def is_valid_ip4(ip: str) -> bool:
    return bool(re.match(r"\d+\.\d+\.\d+\.\d+", ip))


async def get_ip4_from_service() -> str:
    """Get the public IPv4 of this system by calling a third-party service"""
    async with aiohttp.ClientSession() as session:
        async with session.get(IP4_SERVICE_URL) as resp:
            resp.raise_for_status()
            ip = await resp.text()

            if is_valid_ip4(ip):
                return ip
            else:
                raise ValueError(f"Response does not match IPv4 format: {ip}")


def get_ip4_from_socket() -> str:
    """Get the public IPv4 of this system by inspecting a socket connection.
    Warning: This returns a local IP address when running behind a NAT, e.g. on Docker.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((IP4_SOCKET_ENDPOINT, 80))
        return s.getsockname()[0]
    finally:
        s.close()


async def get_IP() -> str:
    """Get the public IPv4 of this system."""
    try:
        return await get_ip4_from_service()
    except Exception as error:
        logging.exception("Error when fetching IPv4 from service")
        return get_ip4_from_socket()
