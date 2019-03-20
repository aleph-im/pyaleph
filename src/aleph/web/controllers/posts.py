from aleph.web import app
from aleph.web.controllers.utils import (Pagination,
                                         cond_output, prepare_date_filters,
                                         prepare_block_height_filters)
from aleph.model.messages import Message, get_merged_posts


async def view_posts_list(request):
    """ Posts list view with filters
    TODO: return state with amended posts
    """

    find_filters = {}
    filters = [
        # {'type': request.query.get('msgType', 'POST')}
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

    hashes = request.query.get('hashes', None)
    if hashes is not None:
        hashes = hashes.split(',')

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

    if hashes is not None:
        filters.append({'$or': [
            {'item_hash': {'$in': hashes}},
            {'tx_hash': {'$in': hashes}}
        ]})

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
        
    posts = [msg
             async for msg
             in await get_merged_posts(find_filters,
                                       limit=pagination_per_page,
                                       skip=pagination_skip)]

    context = {
        'posts': posts
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

app.router.add_get('/api/v0/posts.json', view_posts_list)
app.router.add_get('/api/v0/posts/page/{page}.json', view_posts_list)
