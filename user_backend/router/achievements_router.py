from fastapi import APIRouter, HTTPException, Depends, Path
from typing import Dict, Any
from google.cloud import firestore

from service.achievements_service import calculate_and_update_level
from models.schemas import CalculateLevelResponse
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/achievements",
    tags=["achievements"],
)

@router.post("/users/{user_id}/calculate-level", response_model=CalculateLevelResponse)
async def calculate_level_route(
    user_id: str = Path(..., description="The ID of the user to calculate level for"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Calculate and update a user's level based on their total_drawn value.
    The level is calculated as total_drawn/10000 (rounded down).

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Retrieves the user's total_drawn value from their document
    3. Calculates the new level as total_drawn/10000 (rounded down)
    4. Updates the user's level in Firestore if it has changed
    5. Returns information about the user's previous and current levels

    Returns:
        A dictionary containing:
        - user_id: The ID of the user
        - previous_level: The user's level before the update
        - current_level: The user's level after the update
        - total_drawn: The user's total_drawn value used for the calculation
    """
    try:
        result = await calculate_and_update_level(
            user_id=user_id,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculating level for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while calculating the level")
