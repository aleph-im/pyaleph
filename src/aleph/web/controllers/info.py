from aiohttp import web
from aleph.web import app


async def public_multriaddres(request):
    """Broadcast public node adresses

    According to multriaddr spec https://multiformats.io/multiaddr/
    """

    output = {
        "node_multi_addresses": request.config_dict["extra_config"]["public_adresses"],
    }
    return web.json_response(output)


app.router.add_get("/api/v0/info/public.json", public_multriaddres)
