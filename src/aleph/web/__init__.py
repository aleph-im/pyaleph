from aiohttp import web
import aiohttp_cors
import aiohttp_jinja2
import jinja2

import pkg_resources

import time
import pprint
import socketio

from datetime import date, datetime, timedelta

app = web.Application(client_max_size=1024**2*64)
sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
auth = None

# Configure default CORS settings.
cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
            allow_methods=["GET", "POST"],
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
})
sio.attach(app)

tpl_path = pkg_resources.resource_filename('aleph.web', 'templates')
JINJA_LOADER = jinja2.ChoiceLoader([jinja2.FileSystemLoader(tpl_path), ])
aiohttp_jinja2.setup(app,
                     loader=JINJA_LOADER)
env = aiohttp_jinja2.get_env(app)
env.globals.update({
    'app': app,
    'date': date,
    'datetime': datetime,
    'time': time,
    'timedelta': timedelta,
    'int': int,
    'float': float,
    'len': len,
    'pprint': pprint
})


def init_cors():
    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        if "/socket.io/" not in repr(route.resource):
            cors.add(route)
