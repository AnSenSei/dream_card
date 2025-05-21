from fastapi import APIRouter, HTTPException, Depends, Path, Query
from typing import List
from google.cloud import firestore
from datetime import datetime, timedelta

from models.schemas import RankEntry
from service.user_service import get_weekly_spending_rank
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/rank",
    tags=["rank"],
)

@router.get("/weekly_spent/weekly_spent", response_model=List[str])
async def get_weekly_spending_rank_route(
    limit: int = Query(100, description="The maximum number of users to return (default: 100)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get the top users by weekly spending for the current week.

    This endpoint:
    1. Automatically calculates the current week ID (no input required)
    2. Returns a list of strings in the format "user_id:spent" for the top users by weekly spending

    The week ID is calculated as the start of the week (Monday) using:
    ```python
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    week_id = start_of_week.strftime("%Y-%m-%d")
    ```
    """
    try:
        # Get the top users by weekly spending for the current week
        rank_entries = await get_weekly_spending_rank(db, None, limit)

        # Convert the RankEntry objects to strings in the format "user_id:spent"
        result = [str(entry) for entry in rank_entries]

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting weekly spending rank: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting the weekly spending rank")
