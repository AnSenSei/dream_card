from typing import Optional, Dict, List, Any
import base64
import uuid
from datetime import datetime
import json

from fastapi import HTTPException, UploadFile
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient

from config import get_logger, settings, get_firestore_client, get_storage_client
from models.achievement_schemas import Achievement, AchievementCreate, AchievementResponse, AchievementCreateForm, UploadAchievementSchema, PaginatedAchievementResponse
from utils.gcs_utils import parse_base64_image, get_file_extension

logger = get_logger(__name__)

async def upload_achievement_json(achievement_data: UploadAchievementSchema) -> AchievementResponse:
    """
    Process achievement data with optional emblem image and create an achievement.

    Args:
        achievement_data: The achievement data with optional emblem image

    Returns:
        AchievementResponse: The created achievement with emblem ID and URL if applicable

    Raises:
        HTTPException: If there's an error processing the achievement data
    """
    try:
        # Generate a unique ID for the achievement
        achievement_id = str(uuid.uuid4())

        # Process the condition
        condition = achievement_data.condition

        # Process the rewards
        processed_rewards = []

        for reward in achievement_data.reward:
            if reward.get("type") == "point":
                # Process point reward
                processed_rewards.append({
                    "type": "point",
                    "amount": reward.get("amount", 0)
                })
            elif reward.get("type") == "emblem":
                # Process emblem reward if image is provided
                image_base64 = reward.get("image")
                if image_base64:
                    # Generate emblem ID
                    emblem_id = str(uuid.uuid4())

                    # Parse base64 image
                    try:
                        content_type, base64_data = parse_base64_image(image_base64)
                        image_data = base64.b64decode(base64_data)
                    except ValueError as e:
                        logger.error(f"Invalid base64 image: {e}")
                        raise HTTPException(status_code=400, detail=f"Invalid base64 image: {e}")

                    # Upload image to GCS
                    storage_client = get_storage_client()
                    bucket_name = settings.emblem_bucket

                    # Create a unique filename
                    file_extension = get_file_extension(content_type)
                    blob_name = f"emblems/{emblem_id}.{file_extension}"

                    # Get the bucket
                    bucket = storage_client.bucket(bucket_name)

                    # Create a blob
                    blob = bucket.blob(blob_name)

                    # Upload the image
                    blob.upload_from_string(
                        image_data,
                        content_type=content_type
                    )

                    # Generate GCS URI
                    gcs_uri = f"gs://{bucket_name}/{blob_name}"

                    # Create emblem document in Firestore
                    firestore_client = get_firestore_client()
                    emblem_ref = firestore_client.collection("emblems").document(emblem_id)

                    await emblem_ref.set({
                        "id": emblem_id,
                        "gcs_uri": gcs_uri,
                        "created_at": firestore.SERVER_TIMESTAMP,
                        "achievement_id": achievement_id
                    })

                    # Add emblem to processed rewards
                    processed_rewards.append({
                        "type": "emblem",
                        "emblemId": emblem_id,
                        "url": gcs_uri  # This will be converted to a signed URL in the response
                    })

        # Create achievement document in Firestore
        firestore_client = get_firestore_client()
        achievement_ref = firestore_client.collection("achievements").document(achievement_id)

        achievement_data_dict = {
            "id": achievement_id,
            "name": achievement_data.name,
            "description": achievement_data.description,
            "condition": condition,
            "reward": processed_rewards,
            "rarity": achievement_data.rarity,
            "rank": achievement_data.rank,
            "created_at": firestore.SERVER_TIMESTAMP
        }

        await achievement_ref.set(achievement_data_dict)

        # Create response
        response = AchievementResponse(
            id=achievement_id,
            name=achievement_data.name,
            description=achievement_data.description,
            condition=condition,
            reward=processed_rewards,
            rarity=achievement_data.rarity,
            rank=achievement_data.rank
        )

        return response

    except Exception as e:
        logger.error(f"Error processing achievement data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process achievement: {str(e)}")


async def update_achievement(
    achievement_id: str,
    update_data: Dict[str, Any]
) -> AchievementResponse:
    """
    Update an achievement by ID.

    Args:
        achievement_id: The ID of the achievement to update
        update_data: Dictionary containing the fields to update

    Returns:
        AchievementResponse: The updated achievement

    Raises:
        HTTPException: If the achievement is not found or there's an error updating it
    """
    try:
        # Get Firestore client
        firestore_client = get_firestore_client()

        # Get a reference to the achievement document
        achievement_ref = firestore_client.collection("achievements").document(achievement_id)

        # Check if the achievement exists
        achievement_doc = await achievement_ref.get()
        if not achievement_doc.exists:
            raise HTTPException(status_code=404, detail=f"Achievement with ID {achievement_id} not found")

        # Get the current achievement data
        achievement_data = achievement_doc.to_dict()

        # Update the achievement document
        # Only update the fields that are provided in update_data
        update_fields = {}
        if "name" in update_data:
            update_fields["name"] = update_data["name"]
        if "description" in update_data:
            update_fields["description"] = update_data["description"]
        if "condition" in update_data:
            update_fields["condition"] = update_data["condition"]
        if "reward" in update_data:
            update_fields["reward"] = update_data["reward"]
        if "rarity" in update_data:
            update_fields["rarity"] = update_data["rarity"]
        if "rank" in update_data:
            update_fields["rank"] = update_data["rank"]

        # Add updated_at timestamp
        update_fields["updated_at"] = firestore.SERVER_TIMESTAMP

        # Update the document
        await achievement_ref.update(update_fields)

        # Get the updated achievement data
        updated_doc = await achievement_ref.get()
        updated_data = updated_doc.to_dict()

        # Create response
        response = AchievementResponse(
            id=updated_data.get("id"),
            name=updated_data.get("name"),
            description=updated_data.get("description"),
            condition=updated_data.get("condition"),
            reward=updated_data.get("reward"),
            rarity=updated_data.get("rarity"),
            rank=updated_data.get("rank")
        )

        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating achievement {achievement_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update achievement: {str(e)}")

async def delete_achievement(achievement_id: str) -> Dict[str, Any]:
    """
    Delete an achievement by ID.

    Args:
        achievement_id: The ID of the achievement to delete

    Returns:
        Dict[str, Any]: Success message

    Raises:
        HTTPException: If the achievement is not found or there's an error deleting it
    """
    try:
        # Get Firestore client
        firestore_client = get_firestore_client()

        # Get a reference to the achievement document
        achievement_ref = firestore_client.collection("achievements").document(achievement_id)

        # Check if the achievement exists
        achievement_doc = await achievement_ref.get()
        if not achievement_doc.exists:
            raise HTTPException(status_code=404, detail=f"Achievement with ID {achievement_id} not found")

        # Delete the achievement document
        await achievement_ref.delete()

        return {
            "status": "success",
            "message": f"Achievement with ID {achievement_id} deleted successfully"
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting achievement {achievement_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete achievement: {str(e)}")

async def get_achievements(
    page: int, 
    size: int, 
    condition_type: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = "desc"
) -> PaginatedAchievementResponse:
    """
    Get achievements with pagination, optional filtering by condition type, and sorting.

    Args:
        page: Page number (starts from 1)
        size: Number of items per page
        condition_type: Optional filter by condition type
        sort_by: Field to sort by (rank, rarity, created_at)
        sort_direction: Sort direction (asc, desc)

    Returns:
        PaginatedAchievementResponse: Paginated list of achievements

    Raises:
        HTTPException: If there's an error fetching the achievements
    """
    try:
        # Calculate offset for pagination
        offset = (page - 1) * size

        # Get Firestore client
        firestore_client = get_firestore_client()

        # Create base query
        query = firestore_client.collection("achievements")

        # Apply condition type filter if provided
        if condition_type:
            query = query.where("condition.type", "==", condition_type)

        # Get total count for pagination
        total_query = query
        total_docs = [doc async for doc in total_query.stream()]
        total = len(total_docs)

        # Determine sort field and direction
        sort_field = "created_at"  # Default sort field
        if sort_by in ["rank", "rarity"]:
            sort_field = sort_by

        # Determine sort direction
        direction = firestore.Query.DESCENDING
        if sort_direction and sort_direction.lower() == "asc":
            direction = firestore.Query.ASCENDING

        # Apply sorting and pagination
        # Note: If filtering by condition.type and sorting by a different field,
        # a composite index may be required in Firestore
        try:
            query = query.order_by(sort_field, direction=direction)
            query = query.offset(offset).limit(size)
        except Exception as e:
            # If there's an error with the query (e.g., missing index),
            # fall back to sorting by created_at
            logger.warning(f"Error applying sort by {sort_field}: {e}. Falling back to created_at.")
            query = query.order_by("created_at", direction=firestore.Query.DESCENDING)
            query = query.offset(offset).limit(size)

        # Execute query
        achievements = []
        async for doc in query.stream():
            achievement_data = doc.to_dict()

            # Convert to AchievementResponse
            achievement = AchievementResponse(
                id=achievement_data.get("id"),
                name=achievement_data.get("name"),
                description=achievement_data.get("description"),
                condition=achievement_data.get("condition"),
                reward=achievement_data.get("reward"),
                rarity=achievement_data.get("rarity"),
                rank=achievement_data.get("rank")
            )

            achievements.append(achievement)

        # Create paginated response
        response = PaginatedAchievementResponse(
            items=achievements,
            total=total,
            page=page,
            size=size
        )

        return response

    except Exception as e:
        logger.error(f"Error fetching achievements: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch achievements: {str(e)}")
