import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Sequence

from aiohttp import web
from aiohttp.web_exceptions import HTTPException
from aleph_message.models import ExecutableContent, ItemHash, MessageType
from dataclasses_json import DataClassJsonMixin
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.balances import (
    get_consumed_credits_by_resource,
    get_total_consumed_credits,
)
from aleph.db.accessors.cost import (
    count_resources_with_costs,
    delete_costs_for_message,
    get_costs_summary,
    get_message_costs,
    get_resources_with_costs,
    make_costs_upsert_query,
)
from aleph.db.accessors.messages import get_message_by_item_hash, get_message_status
from aleph.db.models import MessageDb
from aleph.schemas.api.costs import (
    CostComponentDetail,
    CostsFiltersResponse,
    CostsSummaryResponse,
    EstimatedCostsResponse,
    GetCostsQueryParams,
    GetCostsResponse,
    ResourceCostItem,
)
from aleph.schemas.cost_estimation_messages import (
    validate_cost_estimation_message_content,
    validate_cost_estimation_message_dict,
)
from aleph.services.cost import (
    _get_product_price_type,
    _get_settings,
    get_detailed_costs,
    get_payment_type,
    get_total_and_detailed_costs,
    get_total_and_detailed_costs_from_db,
)
from aleph.services.pricing_utils import get_pricing_timeline
from aleph.toolkit.costs import format_cost_str
from aleph.toolkit.ecdsa import require_auth_token
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus
from aleph.web.controllers.app_state_getters import (
    get_session_factory_from_request,
    get_storage_service_from_request,
)
from aleph.web.controllers.utils import get_item_hash_from_request

LOGGER = logging.getLogger(__name__)


# This is not defined in aiohttp.web_exceptions
class HTTPProcessing(HTTPException):
    status_code = 102


# Mapping between message statuses to their corresponding exceptions and messages
MESSAGE_STATUS_EXCEPTIONS = {
    MessageStatus.PENDING: (HTTPProcessing, "Message still pending"),
    MessageStatus.REJECTED: (web.HTTPNotFound, "This message was rejected"),
    MessageStatus.FORGOTTEN: (
        web.HTTPGone,
        "This message has been forgotten",
    ),
    MessageStatus.REMOVED: (
        web.HTTPGone,
        "This message has been removed",
    ),
}


@dataclass
class MessagePrice(DataClassJsonMixin):
    """Dataclass used to expose message required tokens."""

    required_tokens: Optional[Decimal] = None


async def get_executable_message(session: DbSession, item_hash: ItemHash) -> MessageDb:
    """Attempt to get an executable message from the database.
    Raises an HTTP exception if the message is not found, not processed or is not an executable message.
    """

    # Get the message status from the database
    message_status_db = get_message_status(session=session, item_hash=item_hash)
    if not message_status_db:
        raise web.HTTPNotFound(body=f"Message not found with hash: {item_hash}")
    # Loop through the status_exceptions to find a match and raise the corresponding exception
    if message_status_db.status in MESSAGE_STATUS_EXCEPTIONS:
        exception, error_message = MESSAGE_STATUS_EXCEPTIONS[message_status_db.status]
        raise exception(body=f"{error_message}: {item_hash}")
    assert message_status_db.status == MessageStatus.PROCESSED

    # Get the message from the database
    message: Optional[MessageDb] = get_message_by_item_hash(session, item_hash)
    if not message:
        raise web.HTTPNotFound(body="Message not found, despite appearing as processed")
    if message.type not in (
        MessageType.instance,
        MessageType.program,
        MessageType.store,
    ):
        raise web.HTTPBadRequest(
            body=f"Message is not an executable or store message: {item_hash}"
        )

    return message


async def message_price(request: web.Request):
    """
    Returns the price of an executable message.

    ---
    summary: Get message price
    tags:
      - Prices
    parameters:
      - name: item_hash
        in: path
        required: true
        schema:
          type: string
    responses:
      '200':
        description: Estimated costs for the message
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/EstimatedCostsResponse'
      '404':
        description: Message not found
    """

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        item_hash = get_item_hash_from_request(request)
        message = await get_executable_message(session, item_hash)
        content: ExecutableContent = message.parsed_content

        try:
            payment_type = get_payment_type(content)
            required_tokens, costs = get_total_and_detailed_costs_from_db(
                session, content, item_hash
            )

        except RuntimeError as e:
            raise web.HTTPNotFound(reason=str(e))

    model = {
        "required_tokens": float(required_tokens),
        "payment_type": payment_type,
        "cost": format_cost_str(required_tokens),
        "detail": costs,
        "charged_address": content.address,
    }

    response = EstimatedCostsResponse.model_validate(model)

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


class PubMessageRequest(BaseModel):
    message_dict: Dict[str, Any] = Field(alias="message")


async def message_price_estimate(request: web.Request):
    """
    Returns the estimated price of an executable message passed in the body.

    ---
    summary: Estimate message price
    tags:
      - Prices
    requestBody:
      required: true
      content:
        application/json:
          schema:
            type: object
            required:
              - message
            properties:
              message:
                type: object
    responses:
      '200':
        description: Estimated costs
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/EstimatedCostsResponse'
      '404':
        description: Pricing error
    """

    session_factory = get_session_factory_from_request(request)
    storage_service = get_storage_service_from_request(request)

    with session_factory() as session:
        parsed_body = PubMessageRequest.model_validate(await request.json())
        message = validate_cost_estimation_message_dict(parsed_body.message_dict)
        content = await validate_cost_estimation_message_content(
            message, storage_service
        )
        item_hash = message.item_hash

        try:
            payment_type = get_payment_type(content)
            required_tokens, costs = get_total_and_detailed_costs(
                session, content, item_hash
            )

        except RuntimeError as e:
            raise web.HTTPNotFound(reason=str(e))

    model = {
        "required_tokens": float(required_tokens),
        "payment_type": payment_type,
        "cost": format_cost_str(required_tokens),
        "detail": costs,
        "charged_address": content.address,
    }

    response = EstimatedCostsResponse.model_validate(model)

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


@require_auth_token
async def recalculate_message_costs(request: web.Request):
    """
    Force recalculation of message costs with historical pricing. Requires auth token.

    ---
    summary: Recalculate message costs
    tags:
      - Prices
    responses:
      '200':
        description: Recalculation result
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/RecalculateCostsResponse'
    """

    session_factory = get_session_factory_from_request(request)

    # Check if a specific message hash was provided
    item_hash_param = request.match_info.get("item_hash")

    with session_factory() as session:
        messages_to_recalculate: Sequence[MessageDb] = []

        if item_hash_param:
            # Recalculate costs for a specific message
            try:
                # Parse the item_hash_param into an ItemHash object
                try:
                    item_hash = ItemHash(item_hash_param)
                except ValueError:
                    raise web.HTTPBadRequest(
                        body=f"Invalid message hash: {item_hash_param}"
                    )

                message = await get_executable_message(session, item_hash)
                messages_to_recalculate = [message]
            except HTTPException:
                raise
        else:
            # Recalculate costs for all executable messages, ordered by time (oldest first)
            select_stmt = (
                select(MessageDb)
                .where(
                    MessageDb.type.in_(
                        [MessageType.instance, MessageType.program, MessageType.store]
                    )
                )
                .order_by(MessageDb.time.asc())
            )
            result = session.execute(select_stmt)
            messages_to_recalculate = result.scalars().all()

        if not messages_to_recalculate:
            return web.json_response(
                {
                    "message": "No messages found for cost recalculation",
                    "recalculated_count": 0,
                    "total_messages": 0,
                }
            )

        # Get the pricing timeline to track price changes over time
        pricing_timeline = get_pricing_timeline(session)
        LOGGER.info(f"Found {len(pricing_timeline)} pricing changes in timeline")

        recalculated_count = 0
        errors = []
        current_pricing_model = None
        current_pricing_index = 0

        settings = _get_settings(session)

        for message in messages_to_recalculate:
            try:
                # Find the applicable pricing model for this message's timestamp
                while (
                    current_pricing_index < len(pricing_timeline) - 1
                    and pricing_timeline[current_pricing_index + 1][0] <= message.time
                ):
                    current_pricing_index += 1

                current_pricing_model = pricing_timeline[current_pricing_index][1]
                pricing_timestamp = pricing_timeline[current_pricing_index][0]

                LOGGER.debug(
                    f"Message {message.item_hash} at {message.time} using pricing from {pricing_timestamp}"
                )

                # Delete existing cost entries for this message
                delete_costs_for_message(session, message.item_hash)

                # Get the message content and determine product type
                content: ExecutableContent = message.parsed_content
                product_type = _get_product_price_type(
                    content, settings, current_pricing_model
                )

                # Get the pricing for this specific product type
                if product_type not in current_pricing_model:
                    LOGGER.warning(
                        f"Product type {product_type} not found in pricing model for message {message.item_hash}"
                    )
                    continue

                pricing = current_pricing_model[product_type]

                # Calculate new costs using the historical pricing model
                new_costs = get_detailed_costs(
                    session, content, message.item_hash, pricing
                )

                if new_costs:
                    # Store the new cost calculations
                    upsert_stmt = make_costs_upsert_query(new_costs)
                    session.execute(upsert_stmt)

                recalculated_count += 1

            except Exception as e:
                error_msg = f"Failed to recalculate costs for message {message.item_hash}: {str(e)}"
                LOGGER.error(error_msg)
                errors.append({"item_hash": message.item_hash, "error": str(e)})

        # Commit all changes
        session.commit()

        response_data = {
            "message": "Cost recalculation completed with historical pricing",
            "recalculated_count": recalculated_count,
            "total_messages": len(messages_to_recalculate),
            "pricing_changes_found": len(pricing_timeline),
        }

        if errors:
            response_data["errors"] = errors

        return web.json_response(response_data)


async def get_costs(request: web.Request) -> web.Response:
    """
    Get aggregated costs with optional filtering.

    Returns a summary of costs (total_cost_hold, total_cost_stream, total_cost_credit,
    total_consumed_credits, resource_count) with optional filtering by address, item_hash,
    and payment_type.

    Query parameters:
    - address: Filter by owner address
    - item_hash: Filter by specific resource
    - payment_type: Filter by payment type (hold, superfluid, credit)
    - include_details: Detail level (0=summary only, 1=resource list, 2=resource list with breakdown)
    - pagination: Items per page for resource list
    - page: Page number (1-indexed)
    """
    try:
        query_params = GetCostsQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json())

    session_factory = get_session_factory_from_request(request)

    with session_factory() as session:
        # Get cost summary
        cost_summary = get_costs_summary(
            session=session,
            address=query_params.address,
            item_hash=query_params.item_hash,
            payment_type=query_params.payment_type,
        )

        # Get total consumed credits
        total_consumed_credits = get_total_consumed_credits(
            session=session,
            address=query_params.address,
            item_hash=query_params.item_hash,
        )

        summary = CostsSummaryResponse(
            total_consumed_credits=total_consumed_credits,
            total_cost_hold=cost_summary["total_cost_hold"],
            total_cost_stream=cost_summary["total_cost_stream"],
            total_cost_credit=cost_summary["total_cost_credit"],
            resource_count=cost_summary["resource_count"],
        )

        filters = CostsFiltersResponse(
            address=query_params.address,
            item_hash=query_params.item_hash,
            payment_type=(
                query_params.payment_type.value if query_params.payment_type else None
            ),
        )

        response_data: dict = {
            "summary": summary,
            "filters": filters,
        }

        # Include resource list if requested (include_details >= 1)
        if query_params.include_details >= 1:
            resources_data = get_resources_with_costs(
                session=session,
                address=query_params.address,
                item_hash=query_params.item_hash,
                payment_type=query_params.payment_type,
                page=query_params.page,
                pagination=query_params.pagination,
            )

            # Get consumed credits only for the item_hashes in the current page
            item_hashes = [row.item_hash for row in resources_data]
            consumed_credits_map = get_consumed_credits_by_resource(
                session=session,
                item_hashes=item_hashes,
            )

            resources = []
            for row in resources_data:
                resource_item = {
                    "item_hash": row.item_hash,
                    "owner": row.owner,
                    "payment_type": (
                        row.payment_type.value
                        if hasattr(row.payment_type, "value")
                        else str(row.payment_type)
                    ),
                    "consumed_credits": consumed_credits_map.get(row.item_hash, 0),
                    "cost_hold": row.cost_hold,
                    "cost_stream": row.cost_stream,
                    "cost_credit": row.cost_credit,
                }

                # Include detailed breakdown if requested (include_details == 2)
                if query_params.include_details >= 2:
                    cost_details = get_message_costs(session, row.item_hash)
                    resource_item["detail"] = [
                        CostComponentDetail(
                            type=(
                                cost.type.value
                                if hasattr(cost.type, "value")
                                else str(cost.type)
                            ),
                            name=cost.name,
                            cost_hold=str(cost.cost_hold),
                            cost_stream=str(cost.cost_stream),
                            cost_credit=str(cost.cost_credit),
                        )
                        for cost in cost_details
                    ]

                resources.append(ResourceCostItem.model_validate(resource_item))

            total_resources = count_resources_with_costs(
                session=session,
                address=query_params.address,
                item_hash=query_params.item_hash,
                payment_type=query_params.payment_type,
            )

            response_data["resources"] = resources
            response_data["pagination_page"] = query_params.page
            response_data["pagination_total"] = total_resources
            response_data["pagination_per_page"] = query_params.pagination
            response_data["pagination_item"] = "resources"

        response = GetCostsResponse.model_validate(response_data)
        return web.json_response(text=response.model_dump_json())
