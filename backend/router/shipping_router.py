from fastapi import APIRouter, HTTPException, Depends, Query, Path, Body
from typing import Optional
from google.cloud import firestore

from models.schemas import AllWithdrawRequestsResponse, WithdrawRequestDetail, UpdateWithdrawRequestStatusRequest
from service.shipping_service import get_all_withdraw_requests_with_cursor, update_withdraw_request_status
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/shipping",
    tags=["shipping"],
)

@router.get("/withdraw-requests", response_model=AllWithdrawRequestsResponse)
async def get_all_withdraw_requests_with_cursor_route(
    limit: int = Query(10, description="Maximum number of items to return (default: 10)"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination (optional)"),
    sort_by: str = Query("created_at", description="Field to sort by (default: created_at)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    List all withdraw requests across all users with cursor-based pagination.

    This endpoint:
    1. Retrieves withdraw requests from the card_shipping collection
    2. Supports cursor-based pagination with limit and cursor query parameters
    3. Supports sorting with sort_by and sort_order query parameters
    4. Returns a response with withdraw requests and cursor pagination information

    The cursor parameter is optional for the first request. For subsequent requests,
    use the next_cursor value from the previous response to get the next page.
    """
    try:
        withdraw_requests_response = await get_all_withdraw_requests_with_cursor(
            db_client=db,
            limit=limit,
            cursor=cursor,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return withdraw_requests_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all withdraw requests with cursor pagination: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving withdraw requests")


@router.put("/withdraw-requests/{user_id}/{request_id}/status", response_model=WithdrawRequestDetail)
async def update_withdraw_request_status_route(
    user_id: str = Path(..., description="The ID of the user who made the withdraw request"),
    request_id: str = Path(..., description="The ID of the withdraw request to update"),
    request: UpdateWithdrawRequestStatusRequest = Body(..., description="Request body containing new status values"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update the status of a withdraw request and its corresponding card_shipping document.

    This endpoint:
    1. Takes a user ID and request ID as path parameters
    2. Takes a request body with new status and shipping_status values
    3. Updates the status and shipping_status in both the withdraw request document and the card_shipping document
    4. Returns the updated withdraw request details

    The status field represents the overall status of the withdraw request (e.g., 'pending', 'processing', 'completed').
    The shipping_status field represents the status of the shipment (e.g., 'label_created', 'shipped', 'delivered').
    """
    try:
        updated_withdraw_request = await update_withdraw_request_status(
            user_id=user_id,
            request_id=request_id,
            status=request.status,
            shipping_status=request.shipping_status,
            db_client=db
        )
        return updated_withdraw_request
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the withdraw request status")
