from aiohttp import web
import pkg_resources

from aleph.web.controllers import (
    aggregates,
    channels,
    info,
    ipfs,
    main,
    messages,
    p2p,
    posts,
    stats,
    storage,
    version,
)


def register_routes(app: web.Application):
    app.router.add_static(
        "/static/",
        path=pkg_resources.resource_filename("aleph.web", "static/"),
        name="static",
    )
    app.router.add_get("/", main.index)
    app.router.add_get("/api/ws0/status", main.status_ws)
    app.router.add_get("/metrics", main.metrics)
    app.router.add_get("/metrics.json", main.metrics_json)

    app.router.add_get(
        "/api/v0/aggregates/{address}.json", aggregates.address_aggregate
    )

    app.router.add_get("/api/v0/channels/list.json", channels.used_channels)

    app.router.add_get("/api/v0/info/public.json", info.public_multiaddress)

    app.router.add_post("/api/v0/ipfs/add_file", ipfs.ipfs_add_file)

    app.router.add_get("/api/v0/messages.json", messages.view_messages_list)
    app.router.add_get("/api/v0/messages/page/{page}.json", messages.view_messages_list)
    app.router.add_get("/api/ws0/messages", messages.messages_ws)

    app.router.add_post("/api/v0/ipfs/pubsub/pub", p2p.pub_json)
    app.router.add_post("/api/v0/p2p/pubsub/pub", p2p.pub_json)

    app.router.add_get("/api/v0/posts.json", posts.view_posts_list)
    app.router.add_get("/api/v0/posts/page/{page}.json", posts.view_posts_list)

    app.router.add_get("/api/v0/addresses/stats.json", stats.addresses_stats_view)

    app.router.add_post("/api/v0/ipfs/add_json", storage.add_ipfs_json_controller)
    app.router.add_post("/api/v0/storage/add_json", storage.add_storage_json_controller)
    app.router.add_post("/api/v0/storage/add_file", storage.storage_add_file)
    app.router.add_get("/api/v0/storage/{hash}", storage.get_hash)
    app.router.add_get("/api/v0/storage/raw/{hash}", storage.get_raw_hash)
    app.router.add_get(
        "/api/v0/storage/count/{hash}", storage.get_file_references_count
    )

    app.router.add_get("/version", version.version)
    app.router.add_get("/api/v0/version", version.version)
