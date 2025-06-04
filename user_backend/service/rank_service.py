
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, timedelta

from fastapi import HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient

from config import get_logger, settings
from config.db_connection import db_connection
from models.schemas import RankEntry, LevelRankEntry
from utils.gcs_utils import generate_signed_url

logger = get_logger(__name__)

async def get_weekly_spending_rank(db_client: AsyncClient, week_id: Optional[str] = None, limit: int = 100) -> List[RankEntry]:
    """
    Get the top users by weekly spending for a specific week.

    Args:
        db_client: Firestore async client
        week_id: The week ID in the format 'YYYY-MM-DD' (default: current week)
        limit: The maximum number of users to return (default: 100)

    Returns:
        List[RankEntry]: A list of RankEntry objects containing user_id and spent amount

    Raises:
        HTTPException: If there's an error getting the weekly spending rank
    """
    try:
        # If week_id is not provided, calculate the current week's ID
        if not week_id:
            today = datetime.now()
            start_of_week = today - timedelta(days=today.weekday())
            week_id = start_of_week.strftime("%Y-%m-%d")

        # Reference to the weekly_spent collection for the specified week
        weekly_spent_ref = db_client.collection('weekly_spent').document('weekly_spent').collection(week_id)

        # Query the collection, order by 'spent' in descending order, and limit to the specified number
        query = weekly_spent_ref.order_by('spent', direction=firestore.Query.DESCENDING).limit(limit)

        # Execute the query
        docs = await query.get()

        # Convert the documents to RankEntry objects
        result = []
        for doc in docs:
            data = doc.to_dict()
            spent = data.get('spent', 0)
            result.append(RankEntry(user_id=doc.id, spent=spent))

        return result
    except Exception as e:
        logger.error(f"Error getting weekly spending rank for week {week_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get weekly spending rank: {str(e)}")

async def get_top_level_users(db_client: AsyncClient, limit: int = 100) -> List[LevelRankEntry]:
    """
    Get the top users by level, sorted by total_drawn.

    Args:
        db_client: Firestore async client
        limit: The maximum number of users to return (default: 100)

    Returns:
        List[LevelRankEntry]: A list of LevelRankEntry objects containing user_id, total_drawn, level, display_name, and avatar (with signed URL)

    Raises:
        HTTPException: If there's an error getting the top level users
    """
    try:
        # Reference to the users collection
        users_ref = db_client.collection(settings.firestore_collection_users)

        # Query the collection, order by 'total_drawn' in descending order, and limit to the specified number
        query = users_ref.order_by('total_drawn', direction=firestore.Query.DESCENDING).limit(limit)

        # Execute the query
        docs = await query.get()

        # Convert the documents to LevelRankEntry objects
        result = []
        for doc in docs:
            data = doc.to_dict()
            total_drawn = data.get('total_drawn', 0)
            level = data.get('level', 1)
            display_name = data.get('displayName', None)
            avatar = data.get('avatar', None)

            # Generate signed URL for avatar if it exists
            if avatar:
                try:
                    avatar = await generate_signed_url(avatar)
                    logger.info(f"Generated signed URL for avatar of user {doc.id}")
                except Exception as e:
                    logger.error(f"Failed to generate signed URL for avatar of user {doc.id}: {e}")
                    # Keep the original avatar URL if signing fails

            result.append(LevelRankEntry(
                user_id=doc.id, 
                total_drawn=total_drawn, 
                level=level,
                display_name=display_name,
                avatar=avatar
            ))

        return result
    except Exception as e:
        logger.error(f"Error getting top level users: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get top level users: {str(e)}")

async def get_user_pack_opening_history(user_id: str, page: int = 1, per_page: int = 10) -> Dict[str, Any]:
    """
    Get a user's pack opening history.

    Args:
        user_id: The ID of the user
        page: The page number (default: 1)
        per_page: The number of items per page (default: 10)

    Returns:
        A dictionary containing the pack opening history and total count
    """
    try:
        logger.info(f"Getting pack opening history for user {user_id}, page {page}, per_page {per_page}")

        # Calculate offset
        offset = (page - 1) * per_page

        # Query to get total count
        count_query = "SELECT COUNT(*) FROM pack_openings WHERE user_id = %s"

        # Query to get pack openings with pagination
        query = """
            SELECT id, user_id, pack_type, pack_count, price_points, 
                   client_seed, nonce, server_seed_hash, server_seed, random_hash, 
                   opened_at
            FROM pack_openings 
            WHERE user_id = %s
            ORDER BY opened_at DESC
            LIMIT %s OFFSET %s
        """

        # Execute queries
        with db_connection() as conn:
            cursor = conn.cursor()

            # Get total count
            cursor.execute(count_query, (user_id,))
            total_count = cursor.fetchone()[0]

            # Get pack openings
            cursor.execute(query, (user_id, per_page, offset))
            pack_openings = []

            for row in cursor.fetchall():
                pack_opening = {
                    "id": row[0],
                    "user_id": row[1],
                    "pack_type": row[2],
                    "pack_count": row[3],
                    "price_points": row[4],
                    "client_seed": row[5],
                    "nonce": row[6],
                    "server_seed_hash": row[7],
                    "server_seed": row[8],
                    "random_hash": row[9],
                    "opened_at": row[10]
                }
                pack_openings.append(pack_opening)

        logger.info(f"Found {len(pack_openings)} pack openings for user {user_id} (total: {total_count})")

        return {
            "pack_openings": pack_openings,
            "total_count": total_count
        }
    except Exception as e:
        logger.error(f"Error getting pack opening history for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get pack opening history: {str(e)}")
