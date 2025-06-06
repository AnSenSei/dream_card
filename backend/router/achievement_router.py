from fastapi import APIRouter, HTTPException, Depends, Path, Body, File, UploadFile, Form, Query
from google.cloud import firestore
from typing import Optional

from models.achievement_schemas import AchievementCreate, AchievementResponse, AchievementCreateForm, UploadAchievementSchema, PaginatedAchievementResponse
from service.achievement_service import upload_achievement_json, get_achievements
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/achievements",
    tags=["achievements"],
)


@router.post("/upload", response_model=AchievementResponse)
async def upload_achievement(
    achievement: UploadAchievementSchema = Body(...),
) -> AchievementResponse:
    """
    Upload an achievement with optional emblem image.

    The emblem image should be provided as a base64 encoded string in the reward object.
    If an emblem image is provided in this form:{
    "type": "emblem",
    "image": ""
    }
, it will be uploaded to GCS and a document will be created in the emblems collection.

    Returns:
        AchievementResponse: The created achievement with emblem ID and URL if applicable.
    """
    logger.info(f"Received request to upload achievement: {achievement.name}")

    # Call the service function to handle the upload
    result = await upload_achievement_json(achievement)

    return result


@router.get("/", response_model=PaginatedAchievementResponse)
async def list_achievements(
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=100, description="Items per page"),
    condition_type: Optional[str] = Query(None, description="Filter by condition type"),
    sort_by: Optional[str] = Query(None, description="Sort by field (rank, rarity, created_at)"),
    sort_direction: Optional[str] = Query("desc", description="Sort direction (asc, desc)")
) -> PaginatedAchievementResponse:
    """
    List all achievements with pagination, optional filtering by condition type, and sorting by rank or rarity.

    Args:
        page: Page number (starts from 1)
        size: Number of items per page (between 1 and 100)
        condition_type: Optional filter by condition type
        sort_by: Field to sort by (rank, rarity, created_at)
        sort_direction: Sort direction (asc, desc)

    Returns:
        PaginatedAchievementResponse: Paginated list of achievements
    """
    logger.info(f"Listing achievements - page: {page}, size: {size}, condition_type: {condition_type}, sort_by: {sort_by}, sort_direction: {sort_direction}")

    # Call the service function to get the achievements
    result = await get_achievements(page, size, condition_type, sort_by, sort_direction)

    return result
