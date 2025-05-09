from fastapi import APIRouter, File, UploadFile, Form, HTTPException, Query, Body
from typing import Annotated, List

from models.schemas import StoredCardInfo, UpdateQuantityRequest, UpdateCardRequest, CardListResponse, CollectionMetadata
from service.storage_service import (
    process_new_card_submission,
    get_all_stored_cards,
    update_card_quantity,
    update_card_information,
    delete_card_from_firestore,
    add_collection_metadata,
    get_collection_metadata,
    get_all_collection_metadata
)
from config import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/storage",
    tags=["storage"],
)

@router.post("/upload_card", response_model=StoredCardInfo)
async def upload_card_endpoint(
    image_file: Annotated[UploadFile, File()],
    card_name: Annotated[str, Form()],
    rarity: Annotated[str, Form()],
    point_worth: Annotated[int, Form()],
    date_got_in_stock: Annotated[str, Form()],
    quantity: Annotated[int, Form()] = 0,
    collection_metadata_id: str | None = None,
    collectionName: str | None = None  # Keep for backward compatibility
):
    """
    Endpoint to upload a card image and its information.
    - **image_file**: The card image to upload.
    - **card_name**: Name of the card.
    - **rarity**: Rarity of the card.
    - **point_worth**: How many points the card is worth.
    - **date_got_in_stock**: Date the card was acquired.
    - **quantity**: Number of cards in stock (defaults to 0).
    - **collection_metadata_id** (query param, optional): The ID of the collection metadata to use.
    - **collectionName** (query param, optional): The Firestore collection to target (deprecated, use collection_metadata_id instead).
    """
    # For backward compatibility, use collectionName if collection_metadata_id is not provided
    effective_collection_metadata_id = collection_metadata_id if collection_metadata_id is not None else collectionName

    logger.info(f"Received request to upload card: {card_name}. Collection metadata ID: {effective_collection_metadata_id if effective_collection_metadata_id else 'default'}")
    logger.info(f"Original collection_metadata_id: {collection_metadata_id}, collectionName: {collectionName}")
    try:
        # The process_new_card_submission function in the service layer will handle
        # uploading the image and then creating the StoredCardInfo object (including image_url).
        stored_card = await process_new_card_submission(
            image_file=image_file,
            card_name=card_name,
            rarity=rarity,
            point_worth=point_worth,
            date_got_in_stock=date_got_in_stock,
            quantity=quantity,
            collection_metadata_id=effective_collection_metadata_id
        )
        return stored_card
    except HTTPException as e:
        # Log the exception details if needed, then re-raise
        logger.error(f"HTTPException in upload_card_endpoint for {card_name}: {e.detail}")
        raise e
    except Exception as e:
        # Catch any other unexpected errors and return a generic 500
        logger.error(f"Unexpected error in upload_card_endpoint for {card_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred processing the card: {str(e)}")

@router.get("/cards", response_model=CardListResponse)
async def get_all_cards_endpoint(
    collectionName: str | None = None, 
    page: int = Query(1, ge=1, description="Page number to retrieve"),
    per_page: int = Query(10, ge=1, le=100, description="Number of items per page"),
    sort_by: str = Query("point_worth", description="Field to sort by (e.g., point_worth, card_name, date_got_in_stock, quantity, rarity)"),
    sort_order: str = Query("desc", description="Sort order: 'asc' or 'desc'"),
    search_query: str | None = Query(None, description="Search query for card name (prefix match)")
):
    """
    Endpoint to retrieve all stored card information with pagination, sorting, and name search.
    - **collectionName**: Optional Firestore collection name.
    - **page**: Page number to retrieve (default: 1).
    - **per_page**: Number of items per page (default: 10, max: 100).
    - **sort_by**: Field to sort cards by (default: point_worth). Valid fields: point_worth, card_name, date_got_in_stock, quantity, rarity.
    - **sort_order**: Sort order, 'asc' or 'desc' (default: desc).
    - **search_query**: Optional search term for card name (prefix match).
    """
    logger.info(
        f"Received request to get all stored cards. Collection: {collectionName if collectionName else 'default'}, "
        f"Page: {page}, PerPage: {per_page}, SortBy: {sort_by}, SortOrder: {sort_order}, Search: {search_query}"
    )
    try:
        # Pass all parameters to the service function
        card_list_response = await get_all_stored_cards(
            collection_name=collectionName,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )
        return card_list_response
    except HTTPException as e:
        logger.error(f"HTTPException in get_all_cards_endpoint: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in get_all_cards_endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching cards: {str(e)}")

@router.patch("/cards/{document_id}/quantity", response_model=StoredCardInfo)
async def update_card_quantity_endpoint(
    document_id: str,
    request: UpdateQuantityRequest,
    collectionName: str | None = None
):
    """
    Update the quantity of a card by adding or subtracting the specified amount.
    - **document_id**: The Firestore document ID of the card
    - **request.quantity_change**: The amount to change the quantity by (positive to add, negative to subtract)
    - **collectionName** (query param, optional): The Firestore collection to target.
    """
    logger.info(f"Received request to update quantity for card {document_id} by {request.quantity_change}. Collection: {collectionName if collectionName else 'default'}")
    try:
        updated_card = await update_card_quantity(document_id, request.quantity_change, collection_name=collectionName)
        return updated_card
    except HTTPException as e:
        logger.error(f"HTTPException in update_card_quantity_endpoint for {document_id}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in update_card_quantity_endpoint for {document_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred updating card quantity: {str(e)}")

@router.put("/cards/{document_id}", response_model=StoredCardInfo)
async def update_card_endpoint(
    document_id: str,
    card_update: UpdateCardRequest,
    collectionName: str | None = None
):
    """
    Update card information.
    - **document_id**: The Firestore document ID of the card
    - **card_update**: The fields to update (only provided fields will be updated)
    - **collectionName** (query param, optional): The Firestore collection to target.
    """
    logger.info(f"Received request to update card {document_id}. Collection: {collectionName if collectionName else 'default'}")
    try:
        # Convert Pydantic model to dict, excluding None values
        update_data = {k: v for k, v in card_update.model_dump().items() if v is not None}
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        updated_card = await update_card_information(document_id, update_data, collection_name=collectionName)
        return updated_card
    except HTTPException as e:
        logger.error(f"HTTPException in update_card_endpoint for {document_id}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in update_card_endpoint for {document_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred updating card: {str(e)}")

@router.delete("/cards/{document_id}", status_code=204) # 204 No Content is typical for successful DELETE
async def delete_card_endpoint(
    document_id: str,
    collectionName: str | None = None
):
    """
    Delete a card from Firestore.
    - **document_id**: The Firestore document ID of the card.
    - **collectionName** (query param, optional): The Firestore collection to target.
    """
    logger.info(f"Received request to delete card {document_id}. Collection: {collectionName if collectionName else 'default'}")
    try:
        await delete_card_from_firestore(document_id, collection_name=collectionName)
        # No content to return, FastAPI handles the 204 response code
    except HTTPException as e:
        logger.error(f"HTTPException in delete_card_endpoint for {document_id}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in delete_card_endpoint for {document_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while deleting card: {str(e)}")

@router.post("/collection-metadata", response_model=CollectionMetadata)
async def add_collection_metadata_endpoint(
    metadata: CollectionMetadata = Body(...)
):
    """
    Add metadata for a collection to the metadata collection.
    - **metadata**: The metadata for the collection, including name, firestoreCollection, and storagePrefix.
    """
    logger.info(f"Received request to add metadata for collection: {metadata.name}")
    try:
        saved_metadata = await add_collection_metadata(metadata)
        return saved_metadata
    except HTTPException as e:
        logger.error(f"HTTPException in add_collection_metadata_endpoint for {metadata.name}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in add_collection_metadata_endpoint for {metadata.name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred adding collection metadata: {str(e)}")

@router.get("/collection-metadata/{collection_name}", response_model=CollectionMetadata)
async def get_collection_metadata_endpoint(
    collection_name: str
):
    """
    Retrieve metadata for a specific collection from the metadata collection.
    - **collection_name**: The name of the collection to retrieve metadata for.
    """
    logger.info(f"Received request to get metadata for collection: {collection_name}")
    try:
        metadata = await get_collection_metadata(collection_name)
        return metadata
    except HTTPException as e:
        logger.error(f"HTTPException in get_collection_metadata_endpoint for {collection_name}: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in get_collection_metadata_endpoint for {collection_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred retrieving collection metadata: {str(e)}")

@router.get("/collection-metadata", response_model=List[CollectionMetadata])
async def get_all_collection_metadata_endpoint():
    """
    Retrieve metadata for all collections from the metadata collection.
    """
    logger.info("Received request to get metadata for all collections")
    try:
        metadata_list = await get_all_collection_metadata()
        return metadata_list
    except HTTPException as e:
        logger.error(f"HTTPException in get_all_collection_metadata_endpoint: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in get_all_collection_metadata_endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred retrieving collection metadata: {str(e)}")
