from fastapi import APIRouter, HTTPException, Depends, Form, File, UploadFile, Body
from typing import List, Dict, Optional, Any
import json
from models.pack_schema import (
    CardPack, 
    AddPackRequest, 
    RarityDetail, 
    UpdatePackRequest, 
    AddCardToPackRequest,
    UpdateRarityProbabilityRequest,
    AddRarityRequest,
    DeleteRarityRequest,
    AddCardToRarityRequest,
    DeleteCardFromRarityRequest
)
from service.packs_service import (
    create_pack_in_firestore,
    get_all_packs_from_firestore,
    get_pack_by_id_from_firestore,
    update_pack_in_firestore,
    add_card_to_pack_rarity,
    add_card_from_storage_to_pack,
    update_rarity_probability,
    add_rarity_with_probability,
    delete_rarity,
    get_packs_collection_from_firestore,
    add_card_to_rarity,
    delete_card_from_rarity
)
from config import get_firestore_client, get_storage_client, settings, get_logger
from google.cloud import firestore, storage



logger = get_logger(__name__)

router = APIRouter(
    prefix="/packs",
    tags=["packs"],
)

@router.get("/packs_collection", response_model=List[CardPack])
async def list_packs_route(db: firestore.AsyncClient = Depends(get_firestore_client)):
    """Lists all available card packs from Firestore."""
    return await get_all_packs_from_firestore(db)

@router.get("/collection/{collection_id}", response_model=List[CardPack])
async def get_packs_in_collection_route(
    collection_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Lists all packs under a specific collection in Firestore.

    Args:
        collection_id: The ID of the collection to get packs from
        db: Firestore client dependency

    Returns:
        List of card packs in the collection
    """
    return await get_packs_collection_from_firestore(collection_id, db)

@router.get("/{pack_id}", response_model=CardPack)
async def get_pack_details_route(
    pack_id: str, 
    collection_id: Optional[str] = None,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Gets details for a specific card pack from Firestore.

    Args:
        pack_id: The ID of the pack to retrieve
        collection_id: Optional ID of the collection containing the pack
        db: Firestore client dependency
    """
    return await get_pack_by_id_from_firestore(pack_id, db, collection_id)

@router.post("/", response_model=Dict[str, str], status_code=201)
async def add_pack_route(
    pack_name: str = Form(...),
    rarities_config_str: str = Form(...),
    collection_id: str = Form(...),
    win_rate: Optional[int] = Form(None),
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
    - **collection_id**: ID of the pack collection (sent as form field).
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
            rarities_config=parsed_rarities_config,
            collection_id=collection_id,
            win_rate=win_rate
        )

        pack_id = await create_pack_in_firestore(pack_request_model, db, storage_client, image_file)
        return {
            "pack_id": pack_id, 
            "pack_name": pack_name,
            "collection_id": collection_id,
            "rarities_config": rarities_config_str,
            "message": f"Pack '{pack_name}' created successfully in collection '{collection_id}' with rarities configuration"
        }
    except ValueError as e:
        # This could be from Pydantic validation (e.g. not 7 rarities)
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        # Re-raise HTTPExceptions from the service layer or dependency
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in add_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while creating the pack.") 

@router.patch("/{collection_id}/{pack_id}/rarities/{rarity_id}/probability", response_model=Dict[str, str])
async def update_rarity_probability_route(
    collection_id: str,
    pack_id: str,
    rarity_id: str,
    request: UpdateRarityProbabilityRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates the probability of a specific rarity within a pack in a collection.

    Args:
        collection_id: The ID of the pack collection
        pack_id: The ID of the pack
        rarity_id: The ID of the rarity
        request: UpdateRarityProbabilityRequest containing the new probability value
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        await update_rarity_probability(collection_id, pack_id, rarity_id, request.probability, db)
        return {"message": f"Successfully updated probability for rarity '{rarity_id}' in pack '{pack_id}' in collection '{collection_id}'"}
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in update_rarity_probability_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the rarity probability.")

@router.post("/{collection_id}/{pack_id}/rarities", response_model=Dict[str, str], status_code=201)
async def add_rarity_route(
    collection_id: str,
    pack_id: str,
    request: AddRarityRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Adds a new rarity with probability to a pack in a collection.

    Args:
        collection_id: The ID of the pack collection
        pack_id: The ID of the pack to add the rarity to
        request: AddRarityRequest containing the rarity name and probability
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        await add_rarity_with_probability(collection_id, pack_id, request.rarity_id, request.probability, db)
        return {"message": f"Successfully added rarity '{request.rarity_id}' to pack '{pack_id}' in collection '{collection_id}'"}
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in add_rarity_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while adding the rarity.")

@router.delete("/{collection_id}/{pack_id}/rarities/{rarity_id}", response_model=Dict[str, str])
async def delete_rarity_route(
    collection_id: str,
    pack_id: str,
    rarity_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a rarity from a pack in a collection.

    Args:
        collection_id: The ID of the pack collection
        pack_id: The ID of the pack containing the rarity
        rarity_id: The ID of the rarity to delete
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        await delete_rarity(collection_id, pack_id, rarity_id, db)
        return {"message": f"Successfully deleted rarity '{rarity_id}' from pack '{pack_id}' in collection '{collection_id}'"}
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in delete_rarity_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the rarity.")

@router.post("/{collection_id}/{pack_id}/rarities/{rarity_id}/cards", response_model=Dict[str, str], status_code=201)
async def add_card_to_rarity_route(
    collection_id: str,
    pack_id: str,
    rarity_id: str,
    request: AddCardToRarityRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Adds a card to the cards subcollection within a specific rarity in a pack.

    The card information is fetched from the storage service using the collection_metadata_id
    and document_id provided in the request.

    The card is stored as a document under /packs/{collection_id}/{packId}/rarities/{rarityId}/cards/{cardId}
    with the following fields:
    - globalRef: Reference to the global card document (DocumentReference)
    - name: Card name
    - quantity: Card quantity (updated after each draw)
    - point: Card point value (updated after each draw)

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to add the card to
        rarity_id: The ID of the rarity to add the card to
        request: AddCardToRarityRequest containing collection_metadata_id and document_id
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await add_card_to_rarity(
            collection_metadata_id=request.collection_metadata_id,
            document_id=request.document_id,
            pack_id=pack_path,
            rarity_id=rarity_id,
            db_client=db
        )
        return {
            "message": f"Successfully added card '{request.document_id}' to rarity '{rarity_id}' in pack '{pack_id}' in collection '{collection_id}'",
            "card_id": request.document_id,
            "rarity_id": rarity_id,
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in add_card_to_rarity_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while adding the card to the rarity.")

@router.delete("/{collection_id}/{pack_id}/rarities/{rarity_id}/cards", response_model=Dict[str, str])
async def delete_card_from_rarity_route(
    collection_id: str,
    pack_id: str,
    rarity_id: str,
    request: DeleteCardFromRarityRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a card from the cards subcollection within a specific rarity in a pack.

    The card is identified by the collection_metadata_id and document_id provided in the request.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack containing the card
        rarity_id: The ID of the rarity containing the card
        request: DeleteCardFromRarityRequest containing collection_metadata_id and document_id
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await delete_card_from_rarity(
            collection_metadata_id=request.collection_metadata_id,
            document_id=request.document_id,
            pack_id=pack_path,
            rarity_id=rarity_id,
            db_client=db
        )
        return {
            "message": f"Successfully deleted card '{request.document_id}' from rarity '{rarity_id}' in pack '{pack_id}' in collection '{collection_id}'",
            "card_id": request.document_id,
            "rarity_id": rarity_id,
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in delete_card_from_rarity_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the card from the rarity.")
