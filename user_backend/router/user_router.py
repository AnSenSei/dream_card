from fastapi import APIRouter, HTTPException, Depends, Path, Query
from typing import Optional
from google.cloud import firestore

from models.schemas import User, UserCard, UserCardsResponse
from service.user_service import (
    get_user_by_id,
    add_card_to_user,
    draw_card_from_pack,
    get_user_cards,
    destroy_card
)
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["users"],
)

@router.get("/{user_id}", response_model=User)
async def get_user_route(
    user_id: str = Path(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a user by ID.
    """
    try:
        user = await get_user_by_id(user_id, db)
        if not user:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the user")

@router.post("/{user_id}/cards", response_model=UserCard, status_code=201)
async def add_card_to_user_route(
    user_id: str = Path(...),
    card_reference: str = Query(..., description="The reference to the card to add (e.g., 'GlobalCards/checkout')"),
    collection_metadata_id: str = Query(..., description="The ID of the collection metadata to use for the subcollection"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add a card to a user's collection.

    This endpoint:
    1. Takes a card reference and collection_metadata_id as arguments
    2. Gets all card information from the reference
    3. Adds the card to the user's collection with the following fields:
       - card_reference
       - card_name
       - date_got (timestamp)
       - id (card_id)
       - image_url
       - point_worth
       - rarity
    4. If point_worth is less than 1000, also adds:
       - expireAt (timestamp)
       - buybackexpiresAt (timestamp)
    5. Creates a subcollection under the card using collection_metadata_id and puts the card there
    """
    try:
        added_card = await add_card_to_user(user_id, card_reference, db, collection_metadata_id)
        return added_card
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding card to user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the card to the user")

@router.get("/{user_id}/cards", response_model=UserCardsResponse)
async def get_user_cards_route(
    user_id: str = Path(..., description="The ID of the user to get cards for"),
    page: int = Query(1, description="The page number to get (default: 1)"),
    per_page: int = Query(10, description="The number of items per page (default: 10)"),
    sort_by: str = Query("date_got", description="The field to sort by (default: date_got)"),
    sort_order: str = Query("desc", description="The sort order (asc or desc, default: desc)"),
    search_query: Optional[str] = Query(None, description="Optional search query to filter cards by name"),
    subcollection_name: Optional[str] = Query(None, description="Optional subcollection name to filter by"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all cards for a user, separated by subcollection with pagination.

    This endpoint:
    1. Gets all subcollections under the user's 'cards/cards' document
    2. For each subcollection, gets the cards with pagination
    3. Returns a UserCardsResponse with a list of UserCardListResponse objects, one for each subcollection
    4. Each UserCardListResponse contains:
       - subcollection_name
       - cards (list of UserCard objects)
       - pagination info (total_items, items_per_page, current_page, total_pages)
       - applied filters (sort_by, sort_order, search_query)
    """
    try:
        user_cards = await get_user_cards(
            user_id=user_id,
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query,
            subcollection_name=subcollection_name
        )
        return user_cards
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting cards for the user")

@router.post("/draw-card/{collection_id}/{pack_id}", response_model=dict)
async def draw_card_route(
    collection_id: str = Path(..., description="The ID of the collection containing the pack"),
    pack_id: str = Path(..., description="The ID of the pack to draw from"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Draw a card from a pack based on probabilities.

    This endpoint:
    1. Gets all probabilities from cards.values() in the pack
    2. Randomly chooses a card id based on these probabilities
    3. Retrieves the card information from the cards subcollection
    4. Logs the card information
    5. Generates a signed URL for the card image if it's a GCS URI
    6. Returns a dictionary containing:
       - A success message
       - The signed URL for the card image
       - The point_worth of the card
    """
    try:
        result = await draw_card_from_pack(collection_id, pack_id, db)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error drawing card from pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while drawing a card from the pack")

@router.delete("/{user_id}/cards/{subcollection_name}/{card_id}", response_model=dict)
async def destroy_card_route(
    user_id: str = Path(..., description="The ID of the user who owns the card"),
    subcollection_name: str = Path(..., description="The name of the subcollection where the card is stored"),
    card_id: str = Path(..., description="The ID of the card to destroy"),
    quantity: int = Query(1, description="The quantity to destroy (default: 1)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Destroy a card from a user's collection and add its point_worth to the user's pointsBalance.
    If quantity is less than the card's quantity, only reduce the quantity.
    Only remove the card if the remaining quantity is 0.

    This endpoint:
    1. Verifies the user and card exist
    2. Gets the card's point_worth and quantity
    3. Validates the requested quantity is valid
    4. If remaining quantity is 0, removes the card from the user's collection
    5. Otherwise, decrements the card's quantity
    6. Adds the appropriate amount of points to the user's pointsBalance
    7. Returns the updated user and the card
    """
    try:
        updated_user, destroyed_card = await destroy_card(
            user_id=user_id,
            card_id=card_id,
            subcollection_name=subcollection_name,
            db_client=db,
            quantity=quantity
        )
        # Calculate points added based on quantity and point_worth per card
        points_added = destroyed_card.point_worth * quantity

        # Create appropriate message based on whether card was completely destroyed
        if destroyed_card.quantity == 0:
            message = f"Successfully destroyed card {card_id} and added {points_added} points to balance"
        else:
            message = f"Successfully destroyed {quantity} of card {card_id} and added {points_added} points to balance. Remaining quantity: {destroyed_card.quantity}"

        return {
            "message": message,
            "user": updated_user.model_dump(),
            "destroyed_card": destroyed_card.model_dump()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error destroying card: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while destroying the card")
