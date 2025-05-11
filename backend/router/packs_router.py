from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile, Body
from typing import List, Dict, Optional, Any
import json
from models.pack_schema import CardPack, AddPackRequest, RarityDetail, UpdatePackRequest
from service.packs_service import (
    create_pack_in_firestore,
    get_all_packs_from_firestore,
    get_pack_by_id_from_firestore,
    update_pack_in_firestore
)
from config import get_firestore_client, get_storage_client, settings, get_logger
from google.cloud import firestore, storage



logger = get_logger(__name__)

router = APIRouter(
    prefix="/packs",
    tags=["packs"],
)

@router.get("/", response_model=List[CardPack])
async def list_packs_route(db: firestore.AsyncClient = Depends(get_firestore_client)):
    """Lists all available card packs from Firestore."""
    return await get_all_packs_from_firestore(db)

@router.get("/{pack_id}", response_model=CardPack)
async def get_pack_details_route(pack_id: str, db: firestore.AsyncClient = Depends(get_firestore_client)):
    """Gets details for a specific card pack from Firestore."""
    return await get_pack_by_id_from_firestore(pack_id, db)

@router.post("/", response_model=Dict[str, str], status_code=201)
async def add_pack_route(
    pack_name: str = Form(...),
    rarities_config_str: str = Form(...),
    db: firestore.AsyncClient = Depends(get_firestore_client),
    storage_client: storage.Client = Depends(get_storage_client),
    image_file: Optional[UploadFile] = File(None)
):
    """
    Adds a new card pack to Firestore, optionally including an image.
    Uses centralized Firestore and Storage clients from the config module.

    - **pack_name**: Name of the new pack (sent as form field).
    - **rarities_config_str**: JSON string of the rarities configuration (sent as form field).
      Example: `{"Common":{"data":{"probability":0.5}}, "Rare":{"data":{"probability":0.3}}}`
    - **image_file**: Optional image file for the pack.
    """
    try:
        # Parse rarities_config_str from JSON to dict
        try:
            rarities_data = json.loads(rarities_config_str)
            # Reconstruct AddPackRequest model
            # Assuming rarities_data is Dict[str, Dict[str, Any]] where inner dict is for RarityDetail.data
            parsed_rarities_config = { 
                key: RarityDetail(data=value.get('data', {})) 
                for key, value in rarities_data.items() 
            }
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON format for rarities_config_str.")
        except Exception as e: # Catch other potential errors during parsing/reconstruction
             raise HTTPException(status_code=400, detail=f"Error processing rarities_config: {str(e)}")

        pack_request_model = AddPackRequest(
            pack_name=pack_name,
            rarities_config=parsed_rarities_config
        )

        pack_id = await create_pack_in_firestore(pack_request_model, db, storage_client, image_file)
        return {"pack_id": pack_id, "message": "Pack created successfully"}
    except ValueError as e:
        # This could be from Pydantic validation (e.g. not 7 rarities)
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        # Re-raise HTTPExceptions from the service layer or dependency
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in add_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while creating the pack.") 
    
@router.put("/{pack_id}", response_model=Dict[str, str])
async def update_pack_route(
    pack_id: str,
    updates: UpdatePackRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    try:
        # Validate that the pack exists
        await get_pack_by_id_from_firestore(pack_id, db)

        # Convert Pydantic model to dict for the service function
        updates_dict_for_service = {}
        if updates.pack_name is not None:
            updates_dict_for_service["name"] = updates.pack_name
        if updates.description is not None:
            updates_dict_for_service["description"] = updates.description
        
        if updates.rarities:
            transformed_rarities = {
                level: rarity_detail.data
                for level, rarity_detail in updates.rarities.items()
                if rarity_detail and rarity_detail.data 
            }
            if transformed_rarities: # Only add if there are actual rarity data updates
                updates_dict_for_service["rarities"] = transformed_rarities
                     
        if updates.cards_to_add:
            updates_dict_for_service["cards_to_add"] = updates.cards_to_add
        if updates.cards_to_delete:
            updates_dict_for_service["cards_to_delete"] = updates.cards_to_delete
        
        # Validate the updates structure - at least one valid field must be present to update
        # Check if updates_dict_for_service has any actual values to update
        if not updates_dict_for_service: # No fields were provided or all were None
            raise HTTPException(
                status_code=400,
                detail="Update request must contain at least one field with a value to update."
            )

        # Apply the updates
        success = await update_pack_in_firestore(pack_id, updates_dict_for_service, db)

        if success:
            return {"message": f"Pack '{pack_id}' updated successfully"}
        else:
            raise HTTPException(status_code=500, detail=f"Failed to update pack '{pack_id}'")

    except HTTPException as e:
        # Re-raise HTTPExceptions
        raise e
    except Exception as e:
        logger.error(f"Error updating pack '{pack_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred while updating the pack: {str(e)}")
    
@router.put("/{pack_id}/update", response_model=Dict[str, str])
async def update_pack_generic_route(
    pack_id: str,
    updates: Dict[str, Any] = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates an existing card pack in Firestore.

    The updates object can contain any of these fields:
    - **rarities**: Update properties of rarities such as probability
    - **cards_to_add**: Add new cards to specific rarities
    - **cards_to_delete**: Remove cards from specific rarities

    Example request body:
    ```json
    {
        "rarities": {
            "common": {"probability": 0.70},
            "rare": {"probability": 0.25}
        },
        "cards_to_add": {
            "common": ["card_id_3", "card_id_4"],
            "epic": ["card_id_5"]
        },
        "cards_to_delete": {
            "common": ["card_id_1"],
            "rare": ["card_id_2"]
        }
    }
    ```
    """
    try:
        # Validate that the pack exists
        await get_pack_by_id_from_firestore(pack_id, db)

        # Validate the updates structure - at least one valid key must be present
        valid_update_keys = {"rarities", "cards_to_add", "cards_to_delete"}
        if not any(key in updates for key in valid_update_keys):
            raise HTTPException(
                status_code=400,
                detail=f"Update must contain at least one of: {', '.join(valid_update_keys)}"
            )

        # Apply the updates
        success = await update_pack_in_firestore(pack_id, updates, db)

        if success:
            return {"message": f"Pack '{pack_id}' updated successfully"}
        else:
            raise HTTPException(status_code=500, detail=f"Failed to update pack '{pack_id}'")

    except HTTPException as e:
        # Re-raise HTTPExceptions
        raise e
    except Exception as e:
        logger.error(f"Error updating pack '{pack_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred while updating the pack: {str(e)}")