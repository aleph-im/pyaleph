from aiohttp import web

from aleph.web.controllers.app_state_getters import get_node_cache_from_request


async def public_multiaddress(request):
    """Broadcast public node addresses

    According to multiaddr spec https://multiformats.io/multiaddr/
    """

    node_cache = get_node_cache_from_request(request)
    public_addresses = await node_cache.get_public_addresses()

    output = {"node_multi_addresses": public_addresses}
    return web.json_response(output)
