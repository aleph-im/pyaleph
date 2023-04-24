import json
from io import BytesIO, StringIO
from math import ceil
from typing import Optional, Union, IO

import aio_pika
import aiohttp_jinja2
from aiohttp import web
from aiohttp.web_request import FileField
from configmanager import Config
from multidict import MultiDictProxy

DEFAULT_MESSAGES_PER_PAGE = 20
DEFAULT_PAGE = 1
LIST_FIELD_SEPARATOR = ","


def multidict_proxy_to_io(
    multi_dict: MultiDictProxy[Union[str, bytes, FileField]]
) -> IO:
    file_field = multi_dict["file"]
    if isinstance(file_field, bytes):
        return BytesIO(file_field)
    elif isinstance(file_field, str):
        return StringIO(file_field)

    return file_field.file


def get_path_page(request: web.Request) -> Optional[int]:
    page_str = request.match_info.get("page")
    if page_str is None:
        return None

    try:
        page = int(page_str)
    except ValueError:
        raise web.HTTPBadRequest(text=f"Invalid page value in path: {page_str}")

    if page < 1:
        raise web.HTTPUnprocessableEntity(text=f"Page number must be greater than 1.")

    return page


class Pagination(object):
    @staticmethod
    def get_pagination_params(request):
        pagination_page = int(request.match_info.get("page", "1"))
        pagination_page = int(request.query.get("page", pagination_page))
        pagination_param = int(
            request.query.get("pagination", DEFAULT_MESSAGES_PER_PAGE)
        )
        with_pagination = pagination_param != 0

        if pagination_page < 1:
            raise web.HTTPBadRequest(
                text=f"Query field 'page' must be ≥ 1, not {pagination_page}"
            )

        if not with_pagination:
            pagination_per_page = None
            pagination_skip = None
        else:
            pagination_per_page = pagination_param
            pagination_skip = (pagination_page - 1) * pagination_param

        return (pagination_page, pagination_per_page, pagination_skip)

    def __init__(self, page, per_page, total_count, url_base=None, query_string=None):
        self.page = page
        self.per_page = per_page
        self.total_count = total_count
        self.url_base = url_base
        self.query_string = query_string

    @property
    def pages(self):
        return int(ceil(self.total_count / float(self.per_page)))

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def has_next(self):
        return self.page < self.pages

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (
                    num > self.page - left_current - 1
                    and num < self.page + right_current
                )
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


def prepare_date_filters(request, filter_key):
    date_filters = None

    start_date = float(request.query.get("startDate", 0))
    end_date = float(request.query.get("endDate", 0))

    if start_date < 0:
        raise ValueError("startDate field may not be negative")
    if end_date < 0:
        raise ValueError("endDate field may not be negative")

    if start_date:
        date_filters = {}
        date_filters[filter_key] = {"$gte": start_date}

    if end_date:
        new_filter = {}
        new_filter[filter_key] = {"$lte": end_date}
        if date_filters is not None:
            date_filters = {"$and": [date_filters, new_filter]}
        else:
            date_filters = new_filter

    return date_filters


def cond_output(request, context, template):
    if request.rel_url.path.endswith(".json"):
        if "pagination" in context:
            context.pop("pagination")
        response = web.json_response(context, dumps=lambda v: json.dumps(v))
    else:
        response = aiohttp_jinja2.render_template(template, request, context)

    response.enable_compression()

    return response


async def mq_make_aleph_message_topic_queue(
    mq_conn: aio_pika.abc.AbstractConnection,
    config: Config,
    routing_key: Optional[str] = None,
) -> aio_pika.abc.AbstractQueue:
    channel = await mq_conn.channel()
    mq_message_exchange = await channel.declare_exchange(
        name=config.rabbitmq.message_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    mq_queue = await channel.declare_queue(auto_delete=True)
    await mq_queue.bind(mq_message_exchange, routing_key=routing_key)
    return mq_queue
