import importlib.resources

from aiohttp import web
from aiohttp_swagger3 import SwaggerDocs

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


def register_routes(app: web.Application, swagger: SwaggerDocs):
    app.router.add_static(
        "/static/",
        path=str(importlib.resources.files("aleph.web") / "static/"),
        name="static",
    )

    # Non-API routes stay on app.router (HTML template, WebSockets)
    app.router.add_get("/", main.index)
    app.router.add_get("/api/ws0/status", main.status_ws)
    app.router.add_get("/api/ws0/messages", messages.messages_ws)

    # Duplicate routes that share a handler with different path params
    # can't go through swagger (docstring is validated against every route).
    app.router.add_get("/api/v0/messages/page/{page}.json", messages.view_messages_list)
    app.router.add_get("/api/v0/aggregates.json", aggregates.view_aggregates_list)
    app.router.add_get("/api/v0/posts/page/{page}.json", posts.view_posts_list_v0)
    app.router.add_get("/api/v1/posts/page/{page}.json", posts.view_posts_list_v1)
    app.router.add_post("/api/v0/p2p/pubsub/pub", p2p.pub_json)
    app.router.add_post(
        "/api/v0/price/{item_hash}/recalculate",
        prices.recalculate_message_costs,
    )
    app.router.add_get(
        "/api/v0/storage/by-ref/{address}/{ref}",
        storage.get_file_metadata_by_ref,
    )
    app.router.add_get("/api/v0/version", version.version)

    # All API routes go through swagger for documentation
    swagger.add_routes(
        [
            web.get("/metrics", main.metrics),
            web.get("/metrics.json", main.metrics_json),
            web.get("/api/v0/core/{node_id}/metrics", main.ccn_metric),
            web.get("/api/v0/compute/{node_id}/metrics", main.crn_metric),
            web.get("/api/v0/aggregates/{address}.json", aggregates.address_aggregate),
            web.get("/api/v0/aggregates", aggregates.view_aggregates_list),
            web.get("/api/v0/channels/list.json", channels.used_channels),
            web.get("/api/v0/info/public.json", info.public_multiaddress),
            web.get("/api/v0/messages.json", messages.view_messages_list),
            web.post("/api/v0/messages", p2p.pub_message),
            web.get("/api/v0/messages/hashes", messages.view_message_hashes),
            web.get("/api/v0/messages/{item_hash}", messages.view_message),
            web.get(
                "/api/v0/messages/{item_hash}/content",
                messages.view_message_content,
            ),
            web.get(
                "/api/v0/messages/{item_hash}/status",
                messages.view_message_status,
            ),
            web.post("/api/v0/ipfs/pubsub/pub", p2p.pub_json),
            web.get("/api/v0/posts.json", posts.view_posts_list_v0),
            web.get("/api/v1/posts.json", posts.view_posts_list_v1),
            web.get("/api/v0/costs", prices.get_costs),
            web.get("/api/v0/price/{item_hash}", prices.message_price),
            web.post("/api/v0/price/estimate", prices.message_price_estimate),
            web.post(
                "/api/v0/price/recalculate",
                prices.recalculate_message_costs,
            ),
            web.get(
                "/api/v0/addresses/stats.json",
                accounts.addresses_stats_view_v0,
            ),
            web.get(
                "/api/v1/addresses/stats.json",
                accounts.addresses_stats_view_v1,
            ),
            web.get(
                "/api/v0/addresses/{address}/balance",
                accounts.get_account_balance,
            ),
            web.get("/api/v0/balances", accounts.get_chain_balances),
            web.get(
                "/api/v0/credit_balances",
                accounts.get_credit_balances_handler,
            ),
            web.get(
                "/api/v0/addresses/{address}/files",
                accounts.get_account_files,
            ),
            web.get(
                "/api/v0/addresses/{address}/post_types",
                accounts.get_account_post_types,
            ),
            web.get(
                "/api/v0/addresses/{address}/channels",
                accounts.get_account_channels,
            ),
            web.get(
                "/api/v0/addresses/{address}/credit_history",
                accounts.get_account_credit_history,
            ),
            web.get(
                "/api/v0/messages/{item_hash}/consumed_credits",
                accounts.get_resource_consumed_credits_controller,
            ),
            web.post("/api/v0/ipfs/add_file", ipfs.ipfs_add_file),
            web.post(
                "/api/v0/ipfs/add_json",
                storage.add_ipfs_json_controller,
            ),
            web.post(
                "/api/v0/storage/add_file",
                storage.storage_add_file,
            ),
            web.post(
                "/api/v0/storage/add_json",
                storage.add_storage_json_controller,
            ),
            web.get("/api/v0/storage/{file_hash}", storage.get_hash),
            web.get("/api/v0/storage/raw/{file_hash}", storage.get_raw_hash),
            web.get(
                "/api/v0/storage/by-message-hash/{message_hash}",
                storage.get_file_metadata_by_message_hash,
            ),
            web.get(
                "/api/v0/storage/by-ref/{ref}",
                storage.get_file_metadata_by_ref,
            ),
            web.get(
                "/api/v0/storage/count/{hash}",
                storage.get_file_pins_count,
            ),
            web.get("/version", version.version),
            web.get(
                "/api/v0/programs/on/message",
                get_programs_on_message,
            ),
        ]
    )
