from aiohttp import web
import aiohttp_cors
import aiohttp_jinja2
import jinja2

from aiohttp_session import setup, get_session, session_middleware
from aiohttp_session.cookie_storage import EncryptedCookieStorage

#from bottle import Jinja2Template, url

import pkg_resources

import time
import json
from bson import json_util
import pprint

import configparser
from datetime import date, datetime, timedelta

app = web.Application()
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

tpl_path = pkg_resources.resource_filename('pyaleph.web', 'templates')
JINJA_LOADER = jinja2.ChoiceLoader([jinja2.FileSystemLoader(tpl_path),])
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
        cors.add(route)
