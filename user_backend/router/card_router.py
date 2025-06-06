from fastapi import APIRouter, HTTPException, Depends, Path, Query, Body
from typing import Optional, List, Dict, Any
from google.cloud import firestore
from pydantic import BaseModel

from models.schemas import (UserCard, UserCardsResponse, UserCardListResponse, DrawnCard, CardReferencesRequest, PerformFusionRequest,
                            PerformFusionResponse, RandomFusionRequest, CheckCardMissingRequest, CheckCardMissingResponse, WithdrawCardsRequest, WithdrawCardsResponse,
                            WithdrawRequest, WithdrawRequestDetail, PackOpeningHistoryResponse, WithdrawRequestsResponse, UpdateWithdrawCardsRequest,
                            DestroyCardsRequest)
from service.card_service import (
    add_multiple_cards_to_user,
    draw_card_from_pack,
    draw_multiple_cards_from_pack,
    get_user_cards,
    destroy_card,
    destroy_multiple_cards,
    withdraw_ship_card,
    withdraw_ship_multiple_cards,
    perform_fusion,
    perform_random_fusion,
    get_user_card,
    check_card_missing,
    add_card_to_highlights,
    delete_card_from_highlights,
    add_card_to_user,
    add_cards_and_deduct_points,
    get_all_withdraw_requests,
    get_withdraw_request_by_id,
    get_user_pack_opening_history,
    add_to_top_hits,
    update_withdraw_request,
    get_user_highlights,
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
    from_marketplace: bool = Query(False, description="Whether the card is being purchased from the marketplace"),
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
                collection_metadata_id=collection_metadata_id,
                from_marketplace=from_marketplace
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

class CardWithPointsRequest(BaseModel):
    """
    Request model for adding cards to a user's collection while deducting points in a single transaction.
    """
    card_references: List[str]
    points_to_deduct: int

@router.post("/{user_id}/cards_with_points", response_model=dict, status_code=201)
async def add_cards_with_points_route(
    user_id: str = Path(...),
    collection_metadata_id: str = Query(..., description="The ID of the collection metadata to use for the subcollection"),
    request: CardWithPointsRequest = Body(..., description="Request body containing card references and points to deduct"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add cards to a user's collection and deduct points in a single atomic transaction.

    This endpoint:
    1. Takes a list of card_references and points_to_deduct in the request body
    2. Requires collection_metadata_id as a query parameter
    3. Deducts the specified points from the user's balance
    4. Adds the cards to the user's collection
    5. All operations are performed in a single transaction
    6. If any operation fails, the entire transaction is rolled back

    Returns:
        dict: Success message if all operations succeed
    """
    try:
        # Call the service function that handles both operations in a transaction
        result = await add_cards_and_deduct_points(
            user_id=user_id,
            card_references=request.card_references,
            points_to_deduct=request.points_to_deduct,
            collection_metadata_id=collection_metadata_id,
            db_client=db
        )

        return {
            "message": f"Successfully added {result['cards_added']} card(s) and deducted {request.points_to_deduct} points for user {user_id}",
            "result": result
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing cards and points transaction for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while processing the transaction")

@router.get("/{user_id}/cards", response_model=UserCardsResponse)
async def get_user_cards_route(
    user_id: str = Path(...),
    collection_id: str = Query(..., description="The ID of the collection to get cards from"),
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
    2. Requires a collection_id query parameter to specify which subcollection to get cards from
    3. Supports pagination with page and per_page query parameters
    4. Supports sorting with sort_by and sort_order query parameters
    5. Supports searching with search_query query parameter
    6. Returns a list of cards grouped by subcollection
    """
    try:
        cards_response = await get_user_cards(
            user_id=user_id,
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query,
            subcollection_name=collection_id
        )
        return cards_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the cards")

# @router.post("/{user_id}/draw-card", response_model=DrawnCard)
# async def draw_card_route(
#     user_id: str = Path(...),
#     pack_id: str = Query(..., description="The ID of the pack to draw from"),
#     db: firestore.AsyncClient = Depends(get_firestore_client)
# ):
#     """
#     Draw a card from a pack for a user.
#
#     This endpoint:
#     1. Takes a user ID and pack ID as arguments
#     2. Draws a random card from the pack based on the pack's probabilities
#     3. Adds the card to the user's collection
#     4. Returns the drawn card
#     """
#     try:
#         drawn_card = await draw_card_from_pack(
#             user_id=user_id,
#             pack_id=pack_id,
#             db_client=db
#         )
#         return drawn_card
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error drawing card for user: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="An error occurred while drawing the card")

@router.post("/{user_id}/draw-multiple-cards", response_model=List[DrawnCard])
async def draw_multiple_cards_route(
    user_id: str = Path(...),
    pack_id: str = Query(..., description="The ID of the pack to draw from"),
    collection_id: str = Query(..., description="The ID of the collection containing the pack"),
    count: int = Query(..., description="The number of cards to draw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Draw multiple cards from a pack for a user.

    This endpoint:
    1. Takes a user ID, pack ID, collection ID, and count as arguments
    2. Draws the specified number of random cards from the pack based on the pack's probabilities
    3. Adds the cards to the user's collection
    4. Returns the drawn cards
    """
    try:
        drawn_cards = await draw_multiple_cards_from_pack(
            collection_id=collection_id,
            pack_id=pack_id,
            user_id=user_id,
            db_client=db,
            count=count
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
    quantity: int = Query(1, description="The quantity of the card to destroy (default: 1)"),
    subcollection_name: str = Query(..., description="The name of the subcollection where the card is stored"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Destroy a card in a user's collection.

    This endpoint:
    1. Takes a user ID, card ID, and subcollection name as arguments
    2. Deletes the card from the user's collection
    3. Returns a success message

    Note: For destroying multiple cards at once, use the POST /users/{user_id}/cards/destroy endpoint.
    """
    try:
        result = await destroy_card(
            user_id=user_id,
            card_id=card_id,
            subcollection_name=subcollection_name,
            db_client=db,
            quantity = quantity
        )
        return {"message": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error destroying card for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while destroying the card")

@router.delete("/{user_id}/batch-destroy-cards", response_model=dict)
async def destroy_multiple_cards_route(
    user_id: str = Path(..., description="The ID of the user who owns the cards"),
    destroy_request: DestroyCardsRequest = Body(..., description="The cards to destroy"),
    subcollection_name: str = Query(..., description="The name of the subcollection where all cards are stored"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Destroy multiple cards from a user's collection.

    This endpoint:
    1. Takes a user ID and a list of cards to destroy (each with card_id and quantity)
    2. Takes a subcollection_name parameter that applies to all cards
    3. Destroys the specified quantity of each card
    4. Adds the point_worth of each destroyed card to the user's pointsBalance
    5. For each card, if quantity is less than the card's quantity, only destroys the specified quantity
    6. Only removes a card from the collection if the remaining quantity is 0
    7. Returns information about the destroyed cards and updated user balance
    """
    try:
        # Convert the CardToDestroy objects to dictionaries and add the subcollection_name to each
        cards_to_destroy = [{"card_id": card.card_id, "quantity": card.quantity, "subcollection_name":subcollection_name} for card in destroy_request.cards]

        result = await destroy_multiple_cards(
            user_id=user_id,
            cards_to_destroy=cards_to_destroy,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error destroying multiple cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while destroying the cards")

@router.post("/{user_id}/cards/withdraw", response_model=WithdrawCardsResponse)
async def withdraw_multiple_cards_route(
    user_id: str = Path(..., description="The ID of the user who owns the cards"),
    withdraw_request: WithdrawCardsRequest = Body(..., description="The cards to withdraw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a withdraw request for multiple cards from a user's collection.

    This endpoint:
    1. Takes a user ID and a list of cards to withdraw (each with card_id and quantity)
    2. Creates a new withdraw request with fields for request date and status
    3. Creates a "cards" subcollection under the withdraw request to store all withdrawn cards
    4. For each card, if quantity is less than the card's quantity, only withdraws the specified quantity
    5. Only removes a card from the original subcollection if the remaining quantity is 0
    6. Returns a list of the updated cards from the withdraw request
    """
    try:
        # Convert the CardToWithdraw objects to dictionaries
        cards_to_withdraw = [{"card_id": card.card_id, "quantity": card.quantity, "subcollection_name": card.subcollection_name} for card in withdraw_request.cards]

        withdrawn_cards = await withdraw_ship_multiple_cards(
            user_id=user_id,
            cards_to_withdraw=cards_to_withdraw,
            address_id=withdraw_request.address_id,
            phone_number=withdraw_request.phone_number,
            db_client=db
        )
        return WithdrawCardsResponse(cards=withdrawn_cards)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating withdraw request for multiple cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the withdraw request")

# @router.post("/{user_id}/cards/{card_id}/withdraw-ship", response_model=UserCard)
# async def withdraw_ship_card_route(
#     user_id: str = Path(...),
#     card_id: str = Path(...),
#     subcollection_name: str = Query(..., description="The name of the subcollection where the card is stored"),
#     quantity: int = Query(1, description="The quantity to withdraw (default: 1)"),
#     db: firestore.AsyncClient = Depends(get_firestore_client)
# ):
#     """
#     Create a withdraw request for a card from a user's collection.
#
#     This endpoint:
#     1. Takes a user ID, card ID, subcollection name, and quantity as arguments
#     2. Creates a new withdraw request with fields for request date and status
#     3. Creates a "cards" subcollection under the withdraw request to store the withdrawn card
#     4. If quantity is less than the card's quantity, only withdraws the specified quantity
#     5. Only removes the card from the original subcollection if the remaining quantity is 0
#     6. Returns the updated card from the withdraw request
#
#     Note: For withdrawing multiple cards at once, use the /users/{user_id}/cards/withdraw endpoint.
#     """
#     try:
#         withdrawn_card = await withdraw_ship_card(
#             user_id=user_id,
#             card_id=card_id,
#             subcollection_name=subcollection_name,
#             db_client=db,
#             quantity=quantity
#         )
#         return withdrawn_card
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error creating withdraw request for card for user: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="An error occurred while creating the withdraw request")

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

@router.post("/{user_id}/cards/{card_collection_id}/{card_id}/highlights", response_model=UserCard)
async def add_card_to_highlights_route(
    user_id: str = Path(..., description="The ID of the user who owns the card"),
    card_collection_id: str = Path(..., description="The collection ID of the card (e.g., 'pokemon')"),
    card_id: str = Path(..., description="The ID of the card to add to highlights"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add a card to the user's highlights subcollection.

    This endpoint:
    1. Takes a user ID, card collection ID, and card ID as path parameters
    2. Finds the card in the user's collection
    3. Adds the card to the highlights subcollection
    4. Returns the card that was added to highlights
    """
    try:
        card = await add_card_to_highlights(
            user_id=user_id,
            card_collection_id=card_collection_id,
            card_id=card_id,
            db_client=db
        )
        return card
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding card to highlights for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the card to highlights")

@router.delete("/{user_id}/highlights/{card_id}", response_model=dict)
async def delete_card_from_highlights_route(
    user_id: str = Path(..., description="The ID of the user who owns the card"),
    card_id: str = Path(..., description="The ID of the card to delete from highlights"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Delete a card from the user's highlights collection.

    This endpoint:
    1. Takes a user ID and card ID as path parameters
    2. Checks if the card exists in the user's highlights collection
    3. Deletes the card from the highlights collection
    4. Returns a success message
    """
    try:
        result = await delete_card_from_highlights(
            user_id=user_id,
            card_id=card_id,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting card from highlights for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while deleting the card from highlights")

@router.get("/{user_id}/highlights", response_model=UserCardListResponse)
async def get_user_highlights_route(
    user_id: str = Path(..., description="The ID of the user to get highlights for"),
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)"),
    sort_by: str = Query("date_got", description="Field to sort by (default: date_got)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    search_query: Optional[str] = Query(None, description="Optional search query to filter cards by name"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all cards in the user's highlights collection with pagination.

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Supports pagination with page and per_page query parameters
    3. Supports sorting with sort_by and sort_order query parameters
    4. Supports searching with search_query query parameter
    5. Returns a list of highlighted cards with pagination info
    """
    try:
        highlights_response = await get_user_highlights(
            user_id=user_id,
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )
        return highlights_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting highlights for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the highlights")

class TopHitsRequest(BaseModel):
    """Request model for adding a card to top hits"""
    user_id: str
    display_name: str
    card_reference: str

@router.post("/top_hits", response_model=dict)
async def add_to_top_hits_route(
    request: TopHitsRequest = Body(..., description="Request body containing user_id, display_name, and card_reference"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add a card to the top_hits collection.

    This endpoint:
    1. Takes user_id, display_name, and card_reference in the request body
    2. Parses the card_reference to get collection_id and card_id
    3. Fetches the card details from the master collection
    4. Creates a document in the top_hits collection with the required structure
    5. Returns a success message and the document ID
    """
    try:
        result = await add_to_top_hits(
            user_id=request.user_id,
            display_name=request.display_name,
            card_reference=request.card_reference,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding card to top_hits: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the card to top_hits")

# @router.post("/{user_id}/fusion", response_model=PerformFusionResponse)
# async def perform_fusion_route(
#     user_id: str = Path(..., description="The ID of the user performing the fusion"),
#     fusion_request: PerformFusionRequest = Body(..., description="The fusion recipe to use"),
#     db: firestore.AsyncClient = Depends(get_firestore_client)
# ):
#     """
#     Perform a fusion operation for a user.
#
#     This endpoint:
#     1. Takes a user ID and fusion recipe ID as arguments
#     2. Checks if the user has all required ingredients for the fusion
#     3. If yes, performs the fusion by removing ingredient cards and adding the result card
#     4. If no, returns an error message about missing cards
#     5. Returns a success/failure message and the resulting card if successful
#     """
#     try:
#         fusion_result = await perform_fusion(
#             user_id=user_id,
#             result_card_id=fusion_request.result_card_id,
#             db_client=db
#         )
#         return fusion_result
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error performing fusion for user: {e}", exc_info=True)
#         raise HTTPException(status_code=500, detail="An error occurred while performing the fusion")

@router.post("/{user_id}/fusion_recipes/{result_card_id}", response_model=PerformFusionResponse)
async def perform_fusion_by_recipe_id_route(
    user_id: str = Path(..., description="The ID of the user performing the fusion"),
    result_card_id: str = Path(..., description="The ID of the fusion recipe to use"),
    collection_id: Optional[str] = Query(None, description="The ID of the collection containing the fusion recipe"),
    pack_id: Optional[str] = Query(None, description="The ID of the pack containing the fusion recipe"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Perform a fusion operation for a user using a specific recipe ID in the URL path.

    This endpoint:
    1. Takes a user ID and fusion recipe ID as path parameters
    2. Optionally takes collection_id and pack_id as query parameters for direct recipe access
    3. Checks if the user has all required ingredients for the fusion
    4. If yes, performs the fusion by removing ingredient cards and adding the result card
    5. If no, returns an error message about missing cards
    6. Returns a success/failure message and the resulting card if successful

    Args:
        user_id: The ID of the user performing the fusion
        result_card_id: The ID of the fusion recipe to use
        collection_id: Optional. The ID of the collection containing the fusion recipe
        pack_id: Optional. The ID of the pack containing the fusion recipe
        db: Firestore client
    """
    try:
        fusion_result = await perform_fusion(
            user_id=user_id,
            result_card_id=result_card_id,
            db_client=db,
            collection_id=collection_id,
            pack_id=pack_id
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

@router.post("/{user_id}/check-missing-cards", response_model=CheckCardMissingResponse)
async def check_missing_cards_route(
    user_id: str = Path(..., description="The ID of the user to check cards for"),
    request: CheckCardMissingRequest = Body(..., description="The fusion recipe IDs to check"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Check which cards are missing for a user to perform fusion with specified recipes.

    This endpoint:
    1. Takes a user ID and a list of fusion recipe IDs
    2. For each recipe, checks which ingredients the user is missing
    3. Returns detailed information about missing cards for each recipe
    4. Includes card names, images, and quantities where available

    Args:
        user_id: The ID of the user to check cards for
        request: The CheckCardMissingRequest containing fusion recipe IDs to check
        db: Firestore client

    Returns:
        CheckCardMissingResponse with details about missing cards for each recipe
    """
    try:
        result = await check_card_missing(
            user_id=user_id,
            fusion_recipe_ids=request.fusion_recipe_ids,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking missing cards for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while checking missing cards")


@router.get("/{user_id}/withdraw-requests", response_model=WithdrawRequestsResponse)
async def get_all_withdraw_requests_route(
    user_id: str = Path(..., description="The ID of the user to get withdraw requests for"),
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)"),
    sort_by: str = Query("created_at", description="Field to sort by (default: created_at)"),
    sort_order: str = Query("desc", description="Sort order (asc or desc, default: desc)"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    List all withdraw requests for a specific user with pagination.

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Supports pagination with page and per_page query parameters
    3. Supports sorting with sort_by and sort_order query parameters
    4. Retrieves withdraw requests for the user with pagination and sorting
    5. Returns a response with withdraw requests and pagination information
    """
    try:
        withdraw_requests_response = await get_all_withdraw_requests(
            user_id=user_id, 
            db_client=db,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order
        )
        return withdraw_requests_response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting withdraw requests for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving withdraw requests")


@router.get("/{user_id}/withdraw-requests/{request_id}", response_model=WithdrawRequestDetail)
async def get_withdraw_request_by_id_route(
    user_id: str = Path(..., description="The ID of the user who made the withdraw request"),
    request_id: str = Path(..., description="The ID of the withdraw request to retrieve"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get details of a specific withdraw request by ID for a specific user.

    This endpoint:
    1. Takes a user ID and request ID as path parameters
    2. Retrieves the withdraw request with the specified ID for the specified user
    3. Returns the withdraw request details, including:
       - created_at: timestamp when the request was created
       - request_date: timestamp when the request was made
       - status: string indicating the status of the request (e.g., 'pending')
       - user_id: string identifying the user who made the request
       - cards: list of cards included in the withdraw request
    """
    try:
        withdraw_request = await get_withdraw_request_by_id(
            request_id=request_id,
            user_id=user_id,
            db_client=db
        )
        return withdraw_request
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the withdraw request")


@router.put("/{user_id}/withdraw-requests/{request_id}", response_model=WithdrawRequestDetail)
async def update_withdraw_request_route(
    user_id: str = Path(..., description="The ID of the user who made the withdraw request"),
    request_id: str = Path(..., description="The ID of the withdraw request to update"),
    update_request: UpdateWithdrawCardsRequest = Body(..., description="The updated cards to withdraw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update an existing withdraw request with new cards.

    This endpoint:
    1. Takes a user ID and request ID as path parameters
    2. Takes an UpdateWithdrawCardsRequest in the request body containing:
       - cards: list of cards to withdraw (each with card_id, quantity, and subcollection_name)
       - address_id: (optional) ID of the address to ship the cards to
       - phone_number: (optional) phone number of the recipient
    3. Updates the withdraw request with the new cards
    4. Returns the updated withdraw request details

    Note: Only withdraw requests with status 'pending' can be updated.
    If address_id and phone_number are not provided, the existing values in the withdraw request will be used.
    """
    try:
        # Convert the CardToWithdraw objects to dictionaries
        cards_to_withdraw = [{"card_id": card.card_id, "quantity": card.quantity, "subcollection_name": card.subcollection_name} for card in update_request.cards]

        # Prepare kwargs for the update_withdraw_request function
        kwargs = {
            "request_id": request_id,
            "user_id": user_id,
            "cards_to_withdraw": cards_to_withdraw,
            "db_client": db
        }

        # Only include address_id and phone_number if they are provided
        if update_request.address_id is not None:
            kwargs["address_id"] = update_request.address_id
        if update_request.phone_number is not None:
            kwargs["phone_number"] = update_request.phone_number

        updated_withdraw_request = await update_withdraw_request(**kwargs)
        return updated_withdraw_request
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the withdraw request")






@router.get("/{user_id}/pack-opening-history", response_model=PackOpeningHistoryResponse)
async def get_pack_opening_history_route(
    user_id: str = Path(..., description="The ID of the user to get pack opening history for"),
    page: int = Query(1, description="Page number (default: 1)"),
    per_page: int = Query(10, description="Items per page (default: 10)")
):
    """
    Get a user's pack opening history.

    This endpoint:
    1. Takes a user ID, page number, and items per page as arguments
    2. Retrieves the user's pack opening history from the database
    3. Returns the pack opening history with pagination information
    """
    try:
        history = await get_user_pack_opening_history(
            user_id=user_id,
            page=page,
            per_page=per_page
        )
        return history
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pack opening history for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting the pack opening history")
