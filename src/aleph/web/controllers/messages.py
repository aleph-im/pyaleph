from aleph.web import app
from aleph.web.controllers.utils import (Pagination,
                                         cond_output, prepare_date_filters,
                                         prepare_block_height_filters)
from aleph.model.messages import Message


async def view_messages_list(request):
    """ Messages list view with filters (default type: POST)
    """

    find_filters = {}
    filters = [
        {'type': request.query.get('msgType', 'POST')}
    ]

    query_string = request.query_string
    addresses = request.query.get('addresses', None)
    if addresses is not None:
        addresses = addresses.split(',')

    refs = request.query.get('refs', None)
    if refs is not None:
        refs = refs.split(',')

    post_types = request.query.get('types', None)
    if post_types is not None:
        post_types = post_types.split(',')

    tags = request.query.get('tags', None)
    if tags is not None:
        tags = tags.split(',')

    date_filters = prepare_date_filters(request, 'time')
    block_height_filters = prepare_block_height_filters(request, 'blockHeight')

    if addresses is not None:
        filters.append({
            'content.address': {'$in': addresses}
        })

    if post_types is not None:
        filters.append({'content.type': {'$in': post_types}})

    if refs is not None:
        filters.append({'content.ref': {'$in': refs}})

    if tags is not None:
        filters.append({'content.tags': {'$elemMatch': {'$in': tags}}})

    if date_filters is not None:
        filters.append(date_filters)

    if block_height_filters is not None:
        filters.append(block_height_filters)

    if len(filters) > 0:
        find_filters = {'$and': filters} if len(filters) > 1 else filters[0]

    pagination_page, pagination_per_page, pagination_skip = \
        Pagination.get_pagination_params(request)
    if pagination_per_page is None:
        pagination_per_page = 0
    if pagination_skip is None:
        pagination_skip = 0

    messages = [msg async for msg
                in Message.collection.find(
                    find_filters, limit=pagination_per_page,
                    skip=pagination_skip, sort=[('time', -1)])]

    context = {
        'messages': messages
    }

    if pagination_per_page is not None:
        total_msgs = await Message.collection.count(find_filters)

        pagination = Pagination(pagination_page, pagination_per_page,
                                total_msgs,
                                url_base='/messages/posts/page/',
                                query_string=query_string)

        context.update({
            'pagination': pagination,
            'pagination_page': pagination_page,
            'pagination_total': total_msgs,
            'pagination_per_page': pagination_per_page,
            'pagination_item': 'posts'
        })

    return cond_output(request, context, 'TODO.html')

app.router.add_get('/api/v0/messages.json', view_messages_list)
app.router.add_get('/api/v0/messages/page/{page}.json', view_messages_list)
