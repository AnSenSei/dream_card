from fastapi import APIRouter, HTTPException, Depends, Path, Query, Body
from typing import Optional, List
from google.cloud import firestore

from models.schemas import UserCard, UserCardsResponse, DrawnCard, CardReferencesRequest, PerformFusionRequest, PerformFusionResponse, RandomFusionRequest
from service.user_service import (
    add_card_to_user,
    add_multiple_cards_to_user,
    draw_card_from_pack,
    draw_multiple_cards_from_pack,
    get_user_cards,
    destroy_card,
    withdraw_ship_card,
    perform_fusion,
    perform_random_fusion,
    get_user_card
)
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["cards"],
)

@router.post("/{user_id}/cards", response_model=dict, status_code=201)
async def add_card_to_user_route(
    user_id: str = Path(...),
    collection_metadata_id: str = Query(..., description="The ID of the collection metadata to use for the subcollection"),
    card_request: CardReferencesRequest = Body(..., description="Request body containing card references"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add one or multiple cards to a user's collection.

    This endpoint:
    1. Takes a list of card_references in the request body
    2. Requires collection_metadata_id as a query parameter
    3. Gets all card information from the references
    4. Adds the cards to the user's collection with the following fields:
       - card_reference
       - card_name
       - date_got (server timestamp)
       - id (from the card reference)
       - image_url
       - point_worth
       - quantity (1)
       - rarity
       - expireAt (if point_worth < 1000)
       - buybackexpiresAt
    5. Returns a success message
    """
    try:
        if len(card_request.card_references) == 1:
            # Add a single card
            result = await add_card_to_user(
                user_id=user_id,
                card_reference=card_request.card_references[0],
                db_client=db,
                collection_metadata_id=collection_metadata_id
            )
            return {"message": result}
        else:
            # Add multiple cards
            result = await add_multiple_cards_to_user(
                user_id=user_id,
                card_references=card_request.card_references,
                db_client=db,
                collection_metadata_id=collection_metadata_id
            )
            return {"message": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding card(s) to user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the card(s)")

@router.get("/{user_id}/cards", response_model=UserCardsResponse)
async def get_user_cards_route(
    user_id: str = Path(...),
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)"),
    sort_by: str = Query("date_got", description="Field to sort by (default: date_got)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    search_query: Optional[str] = Query(None, description="Optional search query to filter cards by name"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all cards for a user with pagination.

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Supports pagination with page and per_page query parameters
    3. Supports sorting with sort_by and sort_order query parameters
    4. Supports searching with search_query query parameter
    5. Returns a list of cards grouped by subcollection
    """
    try:
        cards_response = await get_user_cards(
            user_id=user_id,
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )
        return cards_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the cards")

@router.post("/{user_id}/draw-card", response_model=DrawnCard)
async def draw_card_route(
    user_id: str = Path(...),
    pack_id: str = Query(..., description="The ID of the pack to draw from"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Draw a card from a pack for a user.

    This endpoint:
    1. Takes a user ID and pack ID as arguments
    2. Draws a random card from the pack based on the pack's probabilities
    3. Adds the card to the user's collection
    4. Returns the drawn card
    """
    try:
        drawn_card = await draw_card_from_pack(
            user_id=user_id,
            pack_id=pack_id,
            db_client=db
        )
        return drawn_card
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error drawing card for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while drawing the card")

@router.post("/{user_id}/draw-multiple-cards", response_model=List[DrawnCard])
async def draw_multiple_cards_route(
    user_id: str = Path(...),
    pack_id: str = Query(..., description="The ID of the pack to draw from"),
    count: int = Query(..., description="The number of cards to draw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Draw multiple cards from a pack for a user.

    This endpoint:
    1. Takes a user ID, pack ID, and count as arguments
    2. Draws the specified number of random cards from the pack based on the pack's probabilities
    3. Adds the cards to the user's collection
    4. Returns the drawn cards
    """
    try:
        drawn_cards = await draw_multiple_cards_from_pack(
            user_id=user_id,
            pack_id=pack_id,
            count=count,
            db_client=db
        )
        return drawn_cards
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error drawing multiple cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while drawing the cards")

@router.delete("/{user_id}/cards/{card_id}", response_model=dict)
async def destroy_card_route(
    user_id: str = Path(...),
    card_id: str = Path(...),
    subcollection_name: str = Query(..., description="The name of the subcollection where the card is stored"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Destroy a card in a user's collection.

    This endpoint:
    1. Takes a user ID, card ID, and subcollection name as arguments
    2. Deletes the card from the user's collection
    3. Returns a success message
    """
    try:
        result = await destroy_card(
            user_id=user_id,
            card_id=card_id,
            subcollection_name=subcollection_name,
            db_client=db
        )
        return {"message": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error destroying card for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while destroying the card")

@router.post("/{user_id}/cards/{card_id}/withdraw-ship", response_model=UserCard)
async def withdraw_ship_card_route(
    user_id: str = Path(...),
    card_id: str = Path(...),
    subcollection_name: str = Query(..., description="The name of the subcollection where the card is stored"),
    quantity: int = Query(1, description="The quantity to withdraw/ship (default: 1)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Withdraw or ship a card from a user's collection.

    This endpoint:
    1. Takes a user ID, card ID, subcollection name, and quantity as arguments
    2. Moves the card from the specified subcollection to the "shipped" subcollection
    3. If quantity is less than the card's quantity, only moves the specified quantity
    4. Only removes the card from the original subcollection if the remaining quantity is 0
    5. Returns the updated shipped card
    """
    try:
        shipped_card = await withdraw_ship_card(
            user_id=user_id,
            card_id=card_id,
            subcollection_name=subcollection_name,
            db_client=db,
            quantity=quantity
        )
        return shipped_card
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error withdrawing/shipping card for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while withdrawing/shipping the card")

@router.get("/{user_id}/cards/{collection_id}/{card_id}", response_model=UserCard)
async def get_user_card_route(
    user_id: str = Path(..., description="The ID of the user who owns the card"),
    collection_id: str = Path(..., description="The collection ID of the card (e.g., 'pokemon')"),
    card_id: str = Path(..., description="The ID of the card to retrieve"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a specific card from a user's collection.

    This endpoint:
    1. Takes a user ID, collection ID, and card ID as path parameters
    2. Retrieves the card from the user's collection
    3. Returns the card details
    """
    try:
        card = await get_user_card(
            user_id=user_id,
            collection_id=collection_id,
            card_id=card_id,
            db_client=db
        )
        return card
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting card for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the card")

@router.post("/{user_id}/fusion", response_model=PerformFusionResponse)
async def perform_fusion_route(
    user_id: str = Path(..., description="The ID of the user performing the fusion"),
    fusion_request: PerformFusionRequest = Body(..., description="The fusion recipe to use"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Perform a fusion operation for a user.

    This endpoint:
    1. Takes a user ID and fusion recipe ID as arguments
    2. Checks if the user has all required ingredients for the fusion
    3. If yes, performs the fusion by removing ingredient cards and adding the result card
    4. If no, returns an error message about missing cards
    5. Returns a success/failure message and the resulting card if successful
    """
    try:
        fusion_result = await perform_fusion(
            user_id=user_id,
            result_card_id=fusion_request.result_card_id,
            db_client=db
        )
        return fusion_result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error performing fusion for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while performing the fusion")

@router.post("/{user_id}/fusion_recipes/{result_card_id}", response_model=PerformFusionResponse)
async def perform_fusion_by_recipe_id_route(
    user_id: str = Path(..., description="The ID of the user performing the fusion"),
    result_card_id: str = Path(..., description="The ID of the fusion recipe to use"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Perform a fusion operation for a user using a specific recipe ID in the URL path.

    This endpoint:
    1. Takes a user ID and fusion recipe ID as path parameters
    2. Checks if the user has all required ingredients for the fusion
    3. If yes, performs the fusion by removing ingredient cards and adding the result card
    4. If no, returns an error message about missing cards
    5. Returns a success/failure message and the resulting card if successful
    """
    try:
        fusion_result = await perform_fusion(
            user_id=user_id,
            result_card_id=result_card_id,
            db_client=db
        )
        return fusion_result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error performing fusion for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while performing the fusion")

@router.post("/{user_id}/random-fusion", response_model=PerformFusionResponse)
async def perform_random_fusion_route(
    user_id: str = Path(..., description="The ID of the user performing the random fusion"),
    fusion_request: RandomFusionRequest = Body(..., description="The cards to fuse"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Perform a random fusion operation for a user.

    This endpoint:
    1. Takes a user ID and two card IDs with their collection
    2. Verifies both cards have point_worth < 500
    3. Calculates the combined point_worth and determines the valid range (0.75-0.90)
    4. Randomly selects a card from the same collection with point_worth in that range
    5. Removes the ingredient cards from the user's collection
    6. Adds the result card to the user's collection
    7. Returns a success/failure message and the resulting card if successful
    """
    try:
        fusion_result = await perform_random_fusion(
            user_id=user_id,
            fusion_request=fusion_request,
            db_client=db
        )
        return fusion_result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error performing random fusion for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while performing the random fusion")