from typing import Optional, Dict, List, Any
from datetime import datetime
import math

from fastapi import HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient
from shippo import Shippo

from config import get_logger, settings
from models.schemas import WithdrawRequest, CursorPaginationInfo, AllWithdrawRequestsResponse, WithdrawRequestDetail, UserCard

logger = get_logger(__name__)

async def get_withdraw_request_by_id(request_id: str, user_id: str, db_client: AsyncClient) -> WithdrawRequestDetail:
    """
    Get a specific withdraw request by ID.
    This function also checks the status of the shipment with the Shippo API and updates the status in Firestore.

    Args:
        request_id: The ID of the withdraw request to get
        user_id: The ID of the user who made the withdraw request
        db_client: Firestore client

    Returns:
        WithdrawRequestDetail object containing the withdraw request details and cards

    Raises:
        HTTPException: If there's an error getting the withdraw request
    """
    try:
        # Check if user exists
        user_ref = db_client.collection('users').document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the withdraw request
        withdraw_request_ref = user_ref.collection('withdraw_requests').document(request_id)
        withdraw_request_doc = await withdraw_request_ref.get()

        if not withdraw_request_doc.exists:
            raise HTTPException(status_code=404, detail=f"Withdraw request with ID {request_id} not found for user {user_id}")

        # Get the withdraw request data
        withdraw_request_data = withdraw_request_doc.to_dict()

        # Check if the withdraw request has a Shippo transaction ID
        shippo_transaction_id = withdraw_request_data.get('shippo_transaction_id')
        if shippo_transaction_id:
            # Initialize the Shippo SDK with API key
            if not hasattr(settings, 'shippo_api_key') or not settings.shippo_api_key:
                logger.error("Shippo API key not configured")
                raise HTTPException(status_code=500, detail="Shipping service not configured")

            shippo_sdk = Shippo(
                api_key_header=settings.shippo_api_key
            )

            try:
                # Retrieve the transaction from Shippo
                shippo_transaction = shippo_sdk.transactions.get(shippo_transaction_id)

                # Get the current status from Shippo
                current_status = shippo_transaction.status

                # Map Shippo status to our shipping_status
                shipping_status_map = {
                    'QUEUED': 'label_created',
                    'WAITING': 'label_created',
                    'PROCESSING': 'label_created',
                    'SUCCESS': 'label_created',
                    'ERROR': 'error',
                    'DELIVERED': 'delivered',
                    'TRANSIT': 'shipped',
                    'FAILURE': 'error',
                    'RETURNED': 'returned',
                    'UNKNOWN': 'unknown'
                }

                # Get the current shipping_status from Firestore
                current_shipping_status = withdraw_request_data.get('shipping_status', 'pending')

                # Map the Shippo status to our shipping_status
                new_shipping_status = shipping_status_map.get(current_status, current_shipping_status)

                # Update the shipping_status in Firestore if it has changed
                if new_shipping_status != current_shipping_status:
                    await withdraw_request_ref.update({
                        'shipping_status': new_shipping_status
                    })
                    withdraw_request_data['shipping_status'] = new_shipping_status
                    logger.info(f"Updated shipping status for withdraw request {request_id} from {current_shipping_status} to {new_shipping_status}")
            except Exception as e:
                logger.error(f"Error checking shipment status for withdraw request {request_id}: {e}", exc_info=True)
                # Don't raise an exception here, just log the error and continue
                # The withdraw request can still be returned with its current status

        # Get the cards in the withdraw request
        cards_ref = withdraw_request_ref.collection('cards')
        cards_docs = await cards_ref.get()

        cards = []
        for card_doc in cards_docs:
            card_data = card_doc.to_dict()
            card = UserCard(**card_data)
            cards.append(card)

        # Create and return the WithdrawRequestDetail object
        withdraw_request_detail = WithdrawRequestDetail(
            id=withdraw_request_doc.id,
            created_at=withdraw_request_data.get('created_at'),
            request_date=withdraw_request_data.get('request_date'),
            status=withdraw_request_data.get('status', 'pending'),
            user_id=withdraw_request_data.get('user_id', user_id),
            card_count=withdraw_request_data.get('card_count', len(cards)),
            cards=cards,
            shipping_address=withdraw_request_data.get('shipping_address'),
            shippo_address_id=withdraw_request_data.get('shippo_address_id'),
            shippo_parcel_id=withdraw_request_data.get('shippo_parcel_id'),
            shippo_shipment_id=withdraw_request_data.get('shippo_shipment_id'),
            shippo_transaction_id=withdraw_request_data.get('shippo_transaction_id'),
            shippo_label_url=withdraw_request_data.get('shippo_label_url'),
            tracking_number=withdraw_request_data.get('tracking_number'),
            tracking_url=withdraw_request_data.get('tracking_url'),
            shipping_status=withdraw_request_data.get('shipping_status')
        )

        logger.info(f"Retrieved withdraw request {request_id} for user {user_id} with {len(cards)} cards")
        return withdraw_request_detail
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get withdraw request: {str(e)}")


async def update_withdraw_request_status(
    user_id: str,
    request_id: str,
    status: str,
    shipping_status: str,
    db_client: AsyncClient
) -> WithdrawRequestDetail:
    """
    Update the status of a withdraw request and its corresponding card_shipping document.

    Args:
        user_id: The ID of the user who made the withdraw request
        request_id: The ID of the withdraw request to update
        status: The new status for the withdraw request
        shipping_status: The new shipping status for the withdraw request
        db_client: Firestore client

    Returns:
        WithdrawRequestDetail object containing the updated withdraw request details

    Raises:
        HTTPException: If there's an error updating the withdraw request
    """
    try:
        # Check if user exists
        user_ref = db_client.collection('users').document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the withdraw request
        withdraw_request_ref = user_ref.collection('withdraw_requests').document(request_id)
        withdraw_request_doc = await withdraw_request_ref.get()

        if not withdraw_request_doc.exists:
            raise HTTPException(status_code=404, detail=f"Withdraw request with ID {request_id} not found for user {user_id}")

        # Update the withdraw request status
        await withdraw_request_ref.update({
            'status': status,
            'shipping_status': shipping_status
        })

        # Update the card_shipping document with the same status
        card_shipping_ref = db_client.collection('card_shipping').document(request_id)
        card_shipping_doc = await card_shipping_ref.get()

        if card_shipping_doc.exists:
            await card_shipping_ref.update({
                'status': status,
                'shipping_status': shipping_status
            })
            logger.info(f"Updated card_shipping document {request_id} with status {status} and shipping_status {shipping_status}")
        else:
            logger.warning(f"Card shipping document with ID {request_id} not found, only updated withdraw request")

        # Get the updated withdraw request
        return await get_withdraw_request_by_id(request_id, user_id, db_client)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update withdraw request: {str(e)}")


async def get_all_withdraw_requests_with_cursor(
    db_client: AsyncClient, 
    limit: int = 10, 
    cursor: Optional[str] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc"
) -> AllWithdrawRequestsResponse:
    """
    Get all withdraw requests with cursor-based pagination.
    This function retrieves withdraw requests from the card_shipping collection.

    Args:
        db_client: Firestore client
        limit: The maximum number of items to return (default: 10)
        cursor: The cursor to start after (optional)
        sort_by: The field to sort by (default: "created_at")
        sort_order: The sort order ("asc" or "desc", default: "desc")

    Returns:
        AllWithdrawRequestsResponse object containing the withdraw requests and cursor pagination info

    Raises:
        HTTPException: If there's an error getting the withdraw requests
    """
    try:
        # Get the card_shipping collection
        card_shipping_ref = db_client.collection('card_shipping')

        # Determine sort direction
        if sort_order.lower() == "desc":
            direction = firestore.Query.DESCENDING
        elif sort_order.lower() == "asc":
            direction = firestore.Query.ASCENDING
        else:
            logger.warning(f"Invalid sort_order '{sort_order}'. Defaulting to DESCENDING.")
            direction = firestore.Query.DESCENDING

        # Apply sorting
        query = card_shipping_ref.order_by(sort_by, direction=direction)

        # Apply cursor pagination if cursor is provided
        if cursor:
            try:
                # Get the document at the cursor
                cursor_doc_ref = card_shipping_ref.document(cursor)
                cursor_doc = await cursor_doc_ref.get()

                if not cursor_doc.exists:
                    raise HTTPException(status_code=400, detail=f"Invalid cursor: document with ID {cursor} not found")

                # Start after the cursor document
                query = query.start_after(cursor_doc)
            except Exception as e:
                logger.error(f"Error applying cursor pagination: {e}", exc_info=True)
                raise HTTPException(status_code=400, detail=f"Invalid cursor: {str(e)}")

        # Apply limit
        query = query.limit(limit + 1)  # Get one extra to check if there are more items

        # Execute the query
        logger.info(f"Executing Firestore query for all withdraw requests with cursor pagination")
        withdraw_requests = await query.get()

        # Check if there are more items
        has_more = len(withdraw_requests) > limit

        # Remove the extra item if there are more
        if has_more:
            withdraw_requests = withdraw_requests[:limit]

        all_withdraw_requests = []
        next_cursor = None

        # Convert each withdraw request to a WithdrawRequest object
        for request in withdraw_requests:
            request_data = request.to_dict()
            # Transform card data to ensure it has all required fields
            cards_data = request_data.get('cards', [])
            transformed_cards = []

            logger.info(f"Transforming {len(cards_data)} cards for withdraw request {request.id}")

            for card_data in cards_data:
                # Create a copy of the card data to avoid modifying the original
                card = dict(card_data)

                # Ensure card_reference is present (this is the most critical field)
                if not card.get('card_reference'):
                    # Try to use other fields that might contain the reference
                    card['card_reference'] = card.get('reference', card.get('card_id', 'unknown_reference'))

                # Ensure each card has the required fields
                if not card.get('date_got'):
                    # Use request_date as fallback or current time if not available
                    card['date_got'] = request_data.get('request_date', datetime.now())

                if not card.get('id'):
                    # Use card_id as fallback or generate an ID based on card_reference
                    card['id'] = card.get('card_id', f"card_{card.get('card_reference')}")

                # Ensure other required fields are present
                if not card.get('card_name'):
                    card['card_name'] = card.get('name', 'Unknown Card')

                if not card.get('image_url'):
                    card['image_url'] = card.get('image', '')

                if not card.get('point_worth'):
                    card['point_worth'] = card.get('worth', 0)

                if not card.get('quantity'):
                    card['quantity'] = card.get('qty', 1)

                if not card.get('rarity'):
                    card['rarity'] = card.get('rarity_level', 0)

                transformed_cards.append(card)

            withdraw_request = WithdrawRequest(
                id=request.id,
                created_at=request_data.get('created_at'),
                request_date=request_data.get('request_date'),
                status=request_data.get('status', 'pending'),
                user_id=request_data.get('user_id'),
                card_count=request_data.get('card_count'),
                shipping_address=request_data.get('shipping_address'),
                shippo_address_id=request_data.get('shippo_address_id'),
                shippo_parcel_id=request_data.get('shippo_parcel_id'),
                shippo_shipment_id=request_data.get('shippo_shipment_id'),
                shippo_transaction_id=request_data.get('shippo_transaction_id'),
                shippo_label_url=request_data.get('shippo_label_url'),
                tracking_number=request_data.get('tracking_number'),
                tracking_url=request_data.get('tracking_url'),
                shipping_status=request_data.get('shipping_status'),
                cards=transformed_cards
            )
            all_withdraw_requests.append(withdraw_request)

            # Set the next cursor to the last document ID
            next_cursor = request.id

        # Create and return the response
        response = AllWithdrawRequestsResponse(
            withdraw_requests=all_withdraw_requests,
            pagination=CursorPaginationInfo(
                next_cursor=next_cursor if has_more else None,
                limit=limit,
                has_more=has_more
            )
        )

        logger.info(f"Retrieved {len(all_withdraw_requests)} withdraw requests with cursor pagination")
        return response
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting all withdraw requests with cursor pagination: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get withdraw requests: {str(e)}")
