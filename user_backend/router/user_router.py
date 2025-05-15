from fastapi import APIRouter, HTTPException, Depends, Path, Query, Body
from typing import Optional, List
from google.cloud import firestore

from models.schemas import User, UserCard, UserCardsResponse, UserEmailAddressUpdate, Address, DrawnCard, CardReferencesRequest, AddPointsRequest, CreateAccountRequest, PerformFusionRequest, PerformFusionResponse, RandomFusionRequest
from service.user_service import (
    get_user_by_id,
    add_card_to_user,
    add_multiple_cards_to_user,
    draw_card_from_pack,
    draw_multiple_cards_from_pack,
    get_user_cards,
    destroy_card,
    withdraw_ship_card,
    update_user_email_and_address,
    add_user_address,
    delete_user_address,
    add_points_to_user,
    create_account,
    perform_fusion,
    perform_random_fusion
)
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["users"],
)

@router.post("/create-account", response_model=User, status_code=201)
async def create_account_route(
    request: CreateAccountRequest = Body(..., description="User account data"),
    user_id: Optional[str] = Query(None, description="Optional user ID. If not provided, a new UUID will be generated."),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a new user account with the specified fields and default values.

    This endpoint:
    1. Takes user account data as input
    2. Creates a new user document in Firestore with the specified fields and default values
    3. Returns the created User object

    The following fields are required:
    - email: User's email address

    The following fields have default values if not provided:
    - displayName: "AnSenSei"
    - addresses: [] (empty array)
    - avatar: null
    - currentMonthKey: Current month in format "YYYY-MM"
    - lastMonthKey: Last month in format "YYYY-MM"

    The following fields are automatically set:
    - createdAt: Current timestamp
    - currentMonthCash: 0
    - lastMonthCash: 0
    - level: 1
    - pointsBalance: 0
    - totalCashRecharged: 0
    - totalPointsSpent: 0
    """
    try:
        user = await create_account(request, db, user_id)
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the user account")

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
       - date_got (timestamp)
       - id (card_id)
       - image_url
       - point_worth
       - rarity
    5. If point_worth is less than 1000, also adds:
       - expireAt (timestamp)
       - buybackexpiresAt (timestamp)
    6. Creates a subcollection under the card using collection_metadata_id and puts the card there
    7. Returns a success message
    """
    try:
        # Process cards from request body
        result = await add_multiple_cards_to_user(
            user_id, 
            card_request.card_references, 
            db, 
            collection_metadata_id
        )
        return {"message": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding card(s) to user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the card(s) to the user")


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

@router.post("/{user_id}/draw-multiple-cards/{collection_id}/{pack_id}", response_model=List[DrawnCard])
async def draw_multiple_cards_route(
    user_id: str = Path(..., description="The ID of the user to add the cards to"),
    collection_id: str = Path(..., description="The ID of the collection containing the pack"),
    pack_id: str = Path(..., description="The ID of the pack to draw from"),
    count: int = Query(1, description="The number of cards to draw (1, 5 or 10)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Draw multiple cards (5 or 10) from a pack based on probabilities.

    This endpoint:
    1. Gets all probabilities from cards.values() in the pack
    2. Randomly chooses multiple card ids based on these probabilities
    3. Retrieves the card information from the cards subcollection for each card
    4. Logs the card information
    5. Returns a list of dictionaries containing the drawn card data

    Note: This endpoint only draws the cards and returns the result. It does not add the cards to the user's collection.
    """
    try:
        result = await draw_multiple_cards_from_pack(collection_id, pack_id, user_id, db, count)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error drawing multiple cards from pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while drawing multiple cards from the pack")

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
    7. Returns a success message
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
            message = f"Successfully destroyed {quantity} of card {card_id} and added {points_added} points to balance."

        return {
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error destroying card: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while destroying the card")

@router.post("/{user_id}/cards/{subcollection_name}/{card_id}/ship", response_model=dict)
async def withdraw_ship_card_route(
    user_id: str = Path(..., description="The ID of the user who owns the card"),
    subcollection_name: str = Path(..., description="The name of the subcollection where the card is stored"),
    card_id: str = Path(..., description="The ID of the card to withdraw/ship"),
    quantity: int = Query(1, description="The quantity to withdraw/ship (default: 1)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Withdraw or ship a card from a user's collection by moving it to the "shipped" subcollection.
    If quantity is less than the card's quantity, only move the specified quantity.
    Only remove the card from the original subcollection if the remaining quantity is 0.

    This endpoint:
    1. Verifies the user and card exist
    2. Gets the card's quantity
    3. Validates the requested quantity is valid
    4. Moves the card (or a portion of it) to the "shipped" subcollection
    5. Adds a request_date timestamp to the shipped card
    6. If remaining quantity is 0, removes the card from the original subcollection
    7. Otherwise, decrements the card's quantity in the original subcollection
    8. Returns a success message
    """
    try:
        shipped_card = await withdraw_ship_card(
            user_id=user_id,
            card_id=card_id,
            subcollection_name=subcollection_name,
            db_client=db,
            quantity=quantity
        )

        # Create appropriate message based on whether card was completely moved
        if shipped_card.quantity == quantity:
            message = f"Successfully shipped all {quantity} of card {card_id} to the shipped subcollection"
        else:
            message = f"Successfully shipped {quantity} of card {card_id} to the shipped subcollection. Total shipped quantity: {shipped_card.quantity}"

        return {
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error shipping card: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while shipping the card")

@router.put("/{user_id}/email-address", response_model=dict)
async def update_user_email_address_route(
    user_id: str = Path(..., description="The ID of the user to update"),
    update_data: UserEmailAddressUpdate = Body(..., description="The email and avatar to update"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update a user's email and avatar.

    This endpoint:
    1. Takes a user ID, email, and optional avatar as arguments
    2. Validates the email format
    3. If avatar is provided, uploads it to Google Cloud Storage
    4. Updates the user's email and avatar fields
    5. Returns a success message

    The avatar field can be either:
    1. A base64 encoded image string in the format: "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEA..."
    2. Binary data (string | null)($binary)
    """
    try:
        updated_user = await update_user_email_and_address(
            user_id=user_id,
            email=update_data.email,
            db_client=db,
            avatar=update_data.avatar
        )
        return {"message": f"Successfully updated email and avatar for user {user_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating email, addresses, and avatar for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the user's email, addresses, and avatar")

@router.post("/{user_id}/address", response_model=dict)
async def add_user_address_route(
    user_id: str = Path(..., description="The ID of the user to update"),
    address: Address = Body(..., description="The address to add"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add a new address to a user's addresses list.

    This endpoint:
    1. Takes a user ID and address object as arguments
    2. Adds the address to the user's addresses list
    3. Returns a success message

    The address should have:
    - id (optional): An identifier like "home" or "work"
    - street: Street address
    - city: City
    - state: State or province
    - zip: Postal code
    - country: Country
    """
    try:
        updated_user = await add_user_address(
            user_id=user_id,
            address=address,
            db_client=db
        )
        address_id = address.id or f"address_{len(updated_user.addresses)}"
        return {"message": f"Successfully added address {address_id} to user {user_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding address for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the address")

@router.delete("/{user_id}/address/{address_id}", response_model=dict)
async def delete_user_address_route(
    user_id: str = Path(..., description="The ID of the user to update"),
    address_id: str = Path(..., description="The ID of the address to delete"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Delete an address from a user's addresses list.

    This endpoint:
    1. Takes a user ID and address ID as arguments
    2. Deletes the address from the user's addresses list
    3. Returns a success message
    """
    try:
        updated_user = await delete_user_address(
            user_id=user_id,
            address_id=address_id,
            db_client=db
        )
        return {"message": f"Successfully deleted address {address_id} from user {user_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting address for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while deleting the address")

@router.post("/{user_id}/points", response_model=dict)
async def add_points_to_user_route(
    user_id: str = Path(..., description="The ID of the user to add points to"),
    points_request: AddPointsRequest = Body(..., description="The points to add"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add points to a user's pointsBalance.

    This endpoint:
    1. Takes a user ID and points to add as arguments
    2. Validates that the points to add are greater than 0
    3. Adds the points to the user's pointsBalance
    4. Returns a success message with the updated points balance
    """
    try:
        updated_user = await add_points_to_user(
            user_id=user_id,
            points=points_request.points,
            db_client=db
        )
        return {
            "message": f"Successfully added {points_request.points} points to user {user_id}",
            "new_balance": updated_user.pointsBalance
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding points to user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding points to the user")

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
