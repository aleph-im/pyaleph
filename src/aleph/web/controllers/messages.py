from aleph.web import app
from aleph.web.controllers.utils import (Pagination,
                                         cond_output, prepare_date_filters)
from aleph.model.messages import Message


async def view_messages_list(request):
    """ Messages list view with filters
    """

    find_filters = {}

    query_string = request.query_string
    msg_type = request.query.get('msgType', None)

    filters = []
    addresses = request.query.get('addresses', None)
    if addresses is not None:
        addresses = addresses.split(',')

    refs = request.query.get('refs', None)
    if refs is not None:
        refs = refs.split(',')

    content_types = request.query.get('contentTypes', None)
    if content_types is not None:
        content_types = content_types.split(',')

    channels = request.query.get('channels', None)
    if channels is not None:
        channels = channels.split(',')

    tags = request.query.get('tags', None)
    if tags is not None:
        tags = tags.split(',')

    hashes = request.query.get('hashes', None)
    if hashes is not None:
        hashes = hashes.split(',')

    date_filters = prepare_date_filters(request, 'time')

    if msg_type is not None:
        filters.append({'type': msg_type})

    if addresses is not None:
        filters.append(
            {'$or': [
                {'content.address': {'$in': addresses}},
                {'sender': {'$in': addresses}},
            ]}
        )

    if content_types is not None:
        filters.append({'content.type': {'$in': content_types}})

    if refs is not None:
        filters.append({'content.ref': {'$in': refs}})

    if tags is not None:
        filters.append({'content.tags': {'$elemMatch': {'$in': tags}}})

    if channels is not None:
        filters.append({'channel': {'$in': channels}})

    if hashes is not None:
        filters.append({'$or': [
            {'item_hash': {'$in': hashes}},
            {'tx_hash': {'$in': hashes}}
        ]})

    if date_filters is not None:
        filters.append(date_filters)

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
                    skip=pagination_skip,
                    sort=[('time',
                           int(request.query.get('sort_order', '-1')))])]

    context = {
        'messages': messages
    }

    if pagination_per_page is not None:
        if len(find_filters.keys()):
            total_msgs = await Message.collection.count_documents(find_filters)
        else:
            total_msgs = await Message.collection.estimated_document_count()

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
