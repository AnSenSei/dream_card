from fastapi import APIRouter, HTTPException, Depends, Form, Body, Query
from typing import List, Dict, Optional, Any
import json
from models.pack_schema import (
    CardPack, 
    AddPackRequest, 
    UpdatePackRequest, 
    AddCardToPackRequest,
    AddCardToPackDirectRequest,
    DeleteCardFromPackRequest,
    PaginatedPacksResponse,
    PaginationInfo,
    AppliedFilters
)
from models.schemas import StoredCardInfo
from service.packs_service import (
    create_pack_in_firestore,
    get_all_packs_from_firestore,
    get_pack_by_id_from_firestore,
    update_pack_in_firestore,
    add_card_from_storage_to_pack,
    get_packs_collection_from_firestore,
    add_card_direct_to_pack,
    delete_card_from_pack,
    activate_pack_in_firestore,
    inactivate_pack_in_firestore,
    delete_pack_in_firestore,
    get_all_cards_in_pack,
    get_inactive_packs_from_collection,
    get_inactive_packs_from_collection_paginated
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

@router.get("/collection/{collection_id}", response_model=PaginatedPacksResponse)
async def get_packs_in_collection_route(
    collection_id: str,
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)"),
    sort_by: Optional[str] = Query("popularity", description="Field to sort by (default: popularity)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    search_query: Optional[str] = Query(None, description="Optional search query to filter packs by name"),
    search_by_cards: bool = Query(False, description="Whether to search by cards in pack (default: False)"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination (ID of the last document in the previous page)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Lists packs under a specific collection in Firestore with pagination, filtering, sorting, and searching.

    Args:
        collection_id: The ID of the collection to get packs from
        page: Page number (default: 1)
        per_page: Items per page (default: 10)
        sort_by: Field to sort by (default: "popularity")
        sort_order: Sort order (asc or desc, default: desc)
        search_query: Optional search query to filter packs by name
        search_by_cards: Whether to search by cards in pack (default: False)
        cursor: Optional cursor for pagination (ID of the last document in the previous page)
        db: Firestore client dependency

    Returns:
        PaginatedPacksResponse containing:
            - packs: List of packs in the collection
            - pagination: Pagination information
            - filters: Applied filters
            - next_cursor: Cursor for the next page
    """
    result = await get_packs_collection_from_firestore(
        collection_id=collection_id,
        db_client=db,
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        search_query=search_query,
        search_by_cards=search_by_cards,
        cursor=cursor
    )

    return PaginatedPacksResponse(
        packs=result["packs"],
        pagination=result["pagination"],
        filters=result["filters"],
        next_cursor=result["next_cursor"]
    )

@router.get("/collection/{collection_id}/inactive", response_model=PaginatedPacksResponse)
async def get_inactive_packs_in_collection_route(
    collection_id: str,
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)"),
    sort_by: Optional[str] = Query("popularity", description="Field to sort by (default: popularity)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    search_query: Optional[str] = Query(None, description="Optional search query to filter packs by name"),
    search_by_cards: bool = Query(False, description="Whether to search by cards in pack (default: False)"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination (ID of the last document in the previous page)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Lists inactive packs (where is_active == False) under a specific collection in Firestore with pagination, filtering, sorting, and searching.

    Args:
        collection_id: The ID of the collection to get inactive packs from
        page: Page number (default: 1)
        per_page: Items per page (default: 10)
        sort_by: Field to sort by (default: "popularity")
        sort_order: Sort order (asc or desc, default: desc)
        search_query: Optional search query to filter packs by name
        search_by_cards: Whether to search by cards in pack (default: False)
        cursor: Optional cursor for pagination (ID of the last document in the previous page)
        db: Firestore client dependency

    Returns:
        PaginatedPacksResponse containing:
            - packs: List of inactive packs in the collection
            - pagination: Pagination information
            - filters: Applied filters
            - next_cursor: Cursor for the next page
    """
    result = await get_inactive_packs_from_collection_paginated(
        collection_id=collection_id,
        db_client=db,
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        search_query=search_query,
        search_by_cards=search_by_cards,
        cursor=cursor
    )

    return PaginatedPacksResponse(
        packs=result["packs"],
        pagination=result["pagination"],
        filters=result["filters"],
        next_cursor=result["next_cursor"]
    )

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
    collection_id: str = Form(...),
    price: int = Form(...),
    win_rate: Optional[int] = Form(None),
    max_win: Optional[int] = Form(None),
    popularity: Optional[int] = Form(None),
    db: firestore.AsyncClient = Depends(get_firestore_client),
    storage_client: storage.Client = Depends(get_storage_client),
    image_file: Optional[str] = Form(None)  # Changed to accept base64 encoded image string
):
    """
    Adds a new card pack to Firestore, optionally including an image.
    Uses centralized Firestore and Storage clients from the config module.

    - **pack_name**: Name of the new pack (sent as form field).
    - **collection_id**: ID of the pack collection (sent as form field).
    - **price**: Price of the pack (sent as form field).
    - **win_rate**: Optional win rate for the pack (sent as form field).
    - **max_win**: Optional maximum win value for the pack (sent as form field).
    - **popularity**: Optional popularity value for the pack (sent as form field). Defaults to 0 if not provided.
    - **image_file**: Optional base64 encoded image string for the pack (format: "data:image/jpeg;base64,...").
    """
    try:
        pack_request_model = AddPackRequest(
            pack_name=pack_name,
            collection_id=collection_id,
            price=price,
            win_rate=win_rate,
            max_win=max_win,
            is_active=False,
            popularity=popularity
        )

        pack_id = await create_pack_in_firestore(pack_request_model, db, storage_client, image_file)
        return {
            "pack_id": pack_id, 
            "pack_name": pack_name,
            "collection_id": collection_id,
            "price": str(price),
            "win_rate": str(win_rate if win_rate is not None else "None"),
            "max_win": str(max_win if max_win is not None else "None"),
            "popularity": str(popularity if popularity is not None else 0),
            "message": f"Pack '{pack_name}' created successfully in collection '{collection_id}'"
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


@router.post("/{collection_id}/{pack_id}/cards", response_model=Dict[str, str], status_code=201)
async def add_card_to_pack_direct_route(
    collection_id: str,
    pack_id: str,
    request: AddCardToPackDirectRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Adds a card directly to a pack with its own probability.

    The card information is fetched from the storage service using the collection_metadata_id
    and document_id provided in the request.

    The card is stored as a document under /packs/{collection_id}/{packId}/cards/{cardId}
    with the following fields:
    - globalRef: Reference to the global card document (DocumentReference)
    - name: Card name
    - quantity: Card quantity (updated after each draw)
    - point: Card point value (updated after each draw)
    - probability: The probability value for the card (0.0 to 1.0)
    - condition: The condition of the card (e.g., "mint", "near mint", etc.)

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to add the card to
        request: AddCardToPackDirectRequest containing collection_metadata_id, document_id, probability, and condition
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await add_card_direct_to_pack(
            collection_metadata_id=request.collection_metadata_id,
            document_id=request.document_id,
            pack_id=pack_path,
            probability=request.probability,
            db_client=db,
            condition=request.condition
        )
        return {
            "message": f"Successfully added card '{request.document_id}' directly to pack '{pack_id}' in collection '{collection_id}' with probability {request.probability}",
            "card_id": request.document_id,
            "pack_id": pack_id,
            "collection_id": collection_id,
            "probability": str(request.probability)
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in add_card_to_pack_direct_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while adding the card to the pack.")

@router.delete("/{collection_id}/{pack_id}/cards", response_model=Dict[str, str])
async def delete_card_from_pack_route(
    collection_id: str,
    pack_id: str,
    request: DeleteCardFromPackRequest,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a card directly from a pack.

    The card is identified by the collection_metadata_id and document_id provided in the request.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack containing the card
        request: DeleteCardFromPackRequest containing collection_metadata_id and document_id
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await delete_card_from_pack(
            collection_metadata_id=request.collection_metadata_id,
            document_id=request.document_id,
            pack_id=pack_path,
            db_client=db
        )
        return {
            "message": f"Successfully deleted card '{request.document_id}' from pack '{pack_id}' in collection '{collection_id}'",
            "card_id": request.document_id,
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in delete_card_from_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the card from the pack.")

@router.patch("/{collection_id}/{pack_id}/activate", response_model=Dict[str, str])
async def activate_pack_route(
    collection_id: str,
    pack_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Activates a pack by setting its is_active field to True.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to activate
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await activate_pack_in_firestore(
            pack_id=pack_path,
            db_client=db
        )
        return {
            "message": f"Successfully activated pack '{pack_id}' in collection '{collection_id}'",
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in activate_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while activating the pack.")

@router.patch("/{collection_id}/{pack_id}/inactivate", response_model=Dict[str, str])
async def inactivate_pack_route(
    collection_id: str,
    pack_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Inactivates a pack by setting its is_active field to False.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to inactivate
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await inactivate_pack_in_firestore(
            pack_id=pack_path,
            db_client=db
        )
        return {
            "message": f"Successfully inactivated pack '{pack_id}' in collection '{collection_id}'",
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in inactivate_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while inactivating the pack.")

@router.get("/{collection_id}/{pack_id}/cards", response_model=List[StoredCardInfo])
async def get_pack_cards_route(
    collection_id: str,
    pack_id: str,
    sort_by: str = "point_worth",
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Gets all cards in a pack, sorted by the specified field in descending order.
    Default sort is by point_worth in descending order.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to get cards from
        sort_by: Field to sort by, either "point_worth" (default) or "rarity"
        db: Firestore client dependency

    Returns:
        List of StoredCardInfo objects representing all cards in the pack, sorted by the specified field in descending order
    """
    try:
        cards = await get_all_cards_in_pack(
            collection_id=collection_id,
            pack_id=pack_id,
            db_client=db,
            sort_by=sort_by
        )
        return cards
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in get_pack_cards_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving cards from the pack.")

@router.patch("/{collection_id}/{pack_id}/max_win", response_model=Dict[str, str])
async def update_max_win_route(
    collection_id: str,
    pack_id: str,
    max_win: int = Form(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates the max_win value for a specific pack.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to update
        max_win: The new max_win value for the pack
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        # Create an updates dictionary with just the max_win field
        updates = {"max_win": max_win}

        # Use the existing update_pack_in_firestore function to update the pack
        await update_pack_in_firestore(
            pack_id=pack_path,
            updates=updates,
            db_client=db
        )
        return {
            "message": f"Successfully updated max_win to {max_win} for pack '{pack_id}' in collection '{collection_id}'",
            "pack_id": pack_id,
            "collection_id": collection_id,
            "max_win": str(max_win)
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in update_max_win_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the pack's max_win value.")

@router.patch("/{collection_id}/{pack_id}/min_win", response_model=Dict[str, str])
async def update_min_win_route(
    collection_id: str,
    pack_id: str,
    min_win: int = Form(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates the min_win value for a specific pack.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to update
        min_win: The new min_win value for the pack
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        # Create an updates dictionary with just the min_win field
        updates = {"min_win": min_win}

        # Use the existing update_pack_in_firestore function to update the pack
        await update_pack_in_firestore(
            pack_id=pack_path,
            updates=updates,
            db_client=db
        )
        return {
            "message": f"Successfully updated min_win to {min_win} for pack '{pack_id}' in collection '{collection_id}'",
            "pack_id": pack_id,
            "collection_id": collection_id,
            "min_win": str(min_win)
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in update_min_win_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the pack's min_win value.")

@router.delete("/{collection_id}/{pack_id}", response_model=Dict[str, str])
async def delete_pack_route(
    collection_id: str,
    pack_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a pack and all its cards from Firestore.

    Args:
        collection_id: The ID of the pack collection containing the pack
        pack_id: The ID of the pack to delete
        db: Firestore client dependency

    Returns:
        Dictionary with success message
    """
    try:
        # Pass the collection_id as part of the pack_id path parameter
        # Format: collection_id/pack_id
        pack_path = f"{collection_id}/{pack_id}"

        await delete_pack_in_firestore(
            pack_id=pack_path,
            db_client=db
        )
        return {
            "message": f"Successfully deleted pack '{pack_id}' in collection '{collection_id}'",
            "pack_id": pack_id,
            "collection_id": collection_id
        }
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer
        raise
    except Exception as e:
        logger.error(f"Unhandled error in delete_pack_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the pack.")
