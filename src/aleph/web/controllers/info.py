from aiohttp import web


async def public_multiaddress(request):
    """Broadcast public node addresses

    According to multiaddr spec https://multiformats.io/multiaddr/
    """

    output = {
        "node_multi_addresses": request.config_dict["extra_config"]["public_adresses"],
    }
    return web.json_response(output)
