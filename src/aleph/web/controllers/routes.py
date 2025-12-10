import pkg_resources
from aiohttp import web

from aleph.web.controllers import (
    accounts,
    aggregates,
    channels,
    info,
    ipfs,
    main,
    messages,
    p2p,
    posts,
    prices,
    storage,
    version,
)
from aleph.web.controllers.programs import get_programs_on_message


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

    app.router.add_get("/api/v0/core/{node_id}/metrics", main.ccn_metric)
    app.router.add_get("/api/v0/compute/{node_id}/metrics", main.crn_metric)

    app.router.add_get(
        "/api/v0/aggregates/{address}.json", aggregates.address_aggregate
    )

    app.router.add_get("/api/v0/channels/list.json", channels.used_channels)

    app.router.add_get("/api/v0/info/public.json", info.public_multiaddress)

    app.router.add_post("/api/v0/ipfs/add_file", ipfs.ipfs_add_file)

    app.router.add_get("/api/v0/messages.json", messages.view_messages_list)
    # Note that this endpoint is implemented in the p2p module out of simplicity because
    # of the large amount of code shared with pub_json.
    app.router.add_post("/api/v0/messages", p2p.pub_message)
    app.router.add_get("/api/v0/messages/hashes", messages.view_message_hashes)
    app.router.add_get("/api/v0/messages/{item_hash}", messages.view_message)
    app.router.add_get(
        "/api/v0/messages/{item_hash}/content", messages.view_message_content
    )
    app.router.add_get(
        "/api/v0/messages/{item_hash}/status", messages.view_message_status
    )
    app.router.add_get("/api/v0/messages/page/{page}.json", messages.view_messages_list)
    app.router.add_get("/api/ws0/messages", messages.messages_ws)

    app.router.add_post("/api/v0/ipfs/pubsub/pub", p2p.pub_json)
    app.router.add_post("/api/v0/p2p/pubsub/pub", p2p.pub_json)

    app.router.add_get("/api/v0/posts.json", posts.view_posts_list_v0)
    app.router.add_get("/api/v0/posts/page/{page}.json", posts.view_posts_list_v0)
    app.router.add_get("/api/v1/posts.json", posts.view_posts_list_v1)
    app.router.add_get("/api/v1/posts/page/{page}.json", posts.view_posts_list_v1)

    app.router.add_get("/api/v0/price/{item_hash}", prices.message_price)
    app.router.add_post("/api/v0/price/estimate", prices.message_price_estimate)
    app.router.add_post("/api/v0/price/recalculate", prices.recalculate_message_costs)
    app.router.add_post(
        "/api/v0/price/{item_hash}/recalculate", prices.recalculate_message_costs
    )

    app.router.add_get("/api/v0/addresses/stats.json", accounts.addresses_stats_view)
    app.router.add_get(
        "/api/v0/addresses/{address}/balance", accounts.get_account_balance
    )
    app.router.add_get("/api/v0/balances", accounts.get_chain_balances)
    app.router.add_get("/api/v0/credit_balances", accounts.get_credit_balances_handler)
    app.router.add_get("/api/v0/addresses/{address}/files", accounts.get_account_files)
    app.router.add_get(
        "/api/v0/addresses/{address}/post_types", accounts.get_account_post_types
    )
    app.router.add_get(
        "/api/v0/addresses/{address}/channels", accounts.get_account_channels
    )
    app.router.add_get(
        "/api/v0/addresses/{address}/credit_history",
        accounts.get_account_credit_history,
    )
    app.router.add_get(
        "/api/v0/messages/{item_hash}/consumed_credits",
        accounts.get_resource_consumed_credits_controller,
    )

    app.router.add_post("/api/v0/ipfs/add_json", storage.add_ipfs_json_controller)
    app.router.add_post("/api/v0/storage/add_json", storage.add_storage_json_controller)
    app.router.add_post("/api/v0/storage/add_file", storage.storage_add_file)
    app.router.add_get("/api/v0/storage/{hash}", storage.get_hash)
    app.router.add_get("/api/v0/storage/raw/{hash}", storage.get_raw_hash)
    app.router.add_get("/api/v0/storage/count/{hash}", storage.get_file_pins_count)

    app.router.add_get("/version", version.version)
    app.router.add_get("/api/v0/version", version.version)

    app.router.add_get("/api/v0/programs/on/message", get_programs_on_message)
