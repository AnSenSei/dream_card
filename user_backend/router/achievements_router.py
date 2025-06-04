from fastapi import APIRouter, HTTPException, Depends, Path, Query
from typing import Dict, Any, Optional
from google.cloud import firestore

from service.achievements_service import (
    calculate_and_update_level,
    get_user_achievements,
    get_all_achievements,
    get_user_achievement_by_id
)
from models.schemas import (
    CalculateLevelResponse,
    UserAchievementsResponse,
    AllAchievementsResponse,
    AchievementWithProgress
)
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

# Export the users_router for use in main.py
__all__ = ["router", "users_router"]

router = APIRouter(
    prefix="/achievements",
    tags=["achievements"],
)

# Create a new router for the /users/{user_id}/achievements/{achievement_id} endpoint
users_router = APIRouter(
    prefix="/users",
    tags=["users", "achievements"],
)

@users_router.get("/{user_id}/achievements/{achievement_id}", response_model=AchievementWithProgress)
async def get_user_achievement_by_id_route(
    user_id: str = Path(..., description="The ID of the user"),
    achievement_id: str = Path(..., description="The ID of the achievement"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a specific achievement for a user by achievement ID.

    This endpoint:
    1. Takes a user ID and achievement ID as path parameters
    2. Retrieves the achievement details from the database
    3. Retrieves the user's progress for this achievement
    4. Returns an AchievementWithProgress object that combines both sets of data

    Returns:
        AchievementWithProgress: An object containing the achievement details and user progress
    """
    try:
        achievement = await get_user_achievement_by_id(
            user_id=user_id,
            achievement_id=achievement_id,
            db_client=db
        )
        return achievement
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting achievement {achievement_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting the achievement")

@users_router.get("/{user_id}/achievements", response_model=UserAchievementsResponse)
async def get_user_achievements_route(
    user_id: str = Path(..., description="The ID of the user to get achievements for"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page"),
    sort_by: str = Query("awardedAt", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all achievements for a specific user with pagination and sorting.

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Retrieves all achievements for the user from the database
    3. Applies pagination and sorting based on query parameters
    4. Returns a list of user achievements with pagination information

    Returns:
        UserAchievementsResponse: A response containing:
        - achievements: List of AchievementWithProgress objects
        - pagination: Pagination information
    """
    try:
        achievements, pagination = await get_user_achievements(
            user_id=user_id,
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return UserAchievementsResponse(
            achievements=achievements,
            pagination=pagination
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting achievements for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting user achievements")

@router.get("", response_model=AllAchievementsResponse)
async def get_all_achievements_route(
    user_id: Optional[str] = Query(None, description="Optional user ID to get progress for"),
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(10, ge=1, le=100, description="Items per page"),
    sort_by: str = Query("created_at", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order (asc or desc)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all achievements in the system with pagination and sorting.
    If user_id is provided, include the user's progress for each achievement.

    This endpoint:
    1. Retrieves all achievements from the database
    2. If a user ID is provided, includes the user's progress for each achievement
    3. Applies pagination and sorting based on query parameters
    4. Returns a list of achievements with pagination information

    Returns:
        AllAchievementsResponse: A response containing:
        - achievements: List of AchievementResponse objects (excludes image_url, criteria, updated_at, awardedAt fields)
        - pagination: Pagination information
    """
    try:
        achievements, pagination = await get_all_achievements(
            db_client=db,
            user_id=user_id,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return AllAchievementsResponse(
            achievements=achievements,
            pagination=pagination
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all achievements: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting achievements")

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
