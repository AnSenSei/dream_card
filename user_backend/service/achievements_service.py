from typing import Dict, Any, Optional
from fastapi import HTTPException
from google.cloud.firestore_v1 import AsyncClient
import math

from config import get_logger, settings

logger = get_logger(__name__)

async def calculate_and_update_level(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Calculate and update a user's level based on their total_drawn value.
    The level is calculated as total_drawn/10000 (rounded down).
    
    Args:
        user_id: The ID of the user to update
        db_client: Firestore client
        
    Returns:
        A dictionary containing the user's current level, previous level, and total_drawn value
        
    Raises:
        HTTPException: If there's an error calculating or updating the level
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        
        # Get the user data
        user_data = user_doc.to_dict()
        
        # Get the total_drawn value (default to 0 if not present)
        total_drawn = user_data.get('total_drawn', 0)
        
        # Calculate the new level based on total_drawn/10000
        new_level = math.floor(total_drawn / 10000)
        
        # Ensure minimum level is 1
        if new_level < 1:
            new_level = 1
        
        # Get the current level
        current_level = user_data.get('level', 1)
        
        # Only update if the level has changed
        if new_level != current_level:
            # Update the user's level
            await user_ref.update({"level": new_level})
            logger.info(f"Updated level for user {user_id} from {current_level} to {new_level} (total_drawn: {total_drawn})")
        else:
            logger.info(f"Level for user {user_id} remains at {current_level} (total_drawn: {total_drawn})")
        
        # Return the result
        return {
            "user_id": user_id,
            "previous_level": current_level,
            "current_level": new_level,
            "total_drawn": total_drawn
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error calculating and updating level for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to calculate and update level: {str(e)}")