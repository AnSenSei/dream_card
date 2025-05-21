from fastapi import APIRouter, HTTPException, Query, Path, Depends
from google.cloud import firestore
import httpx
from typing import Dict, Any, Optional

from service.storage_service import add_to_official_listing, withdraw_from_official_listing, get_all_official_listings
from service.marketplace_service import buy_card_from_official_listing
from config import get_logger, get_firestore_client, settings

logger = get_logger(__name__)

# Functions to interact with user_backend service
async def get_user_by_id(user_id: str, db_client: firestore.AsyncClient = None) -> Optional[Dict[str, Any]]:
    """
    Get a user by ID from the user_backend service.

    Args:
        user_id: The ID of the user
        db_client: Not used, kept for compatibility

    Returns:
        User data or None if not found

    Raises:
        HTTPException: If there's an error communicating with the user_backend service
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{settings.user_backend_url}/users/{user_id}")

            if response.status_code == 404:
                return None

            if response.status_code != 200:
                logger.error(f"Error getting user {user_id} from user_backend: {response.text}")
                raise HTTPException(status_code=response.status_code, detail=f"Error from user service: {response.text}")

            return response.json()
    except httpx.RequestError as e:
        logger.error(f"Error communicating with user_backend service: {e}")
        raise HTTPException(status_code=503, detail=f"User service unavailable: {str(e)}")

async def add_points_to_user(user_id: str, points: int, db_client: firestore.AsyncClient = None) -> Dict[str, Any]:
    """
    Add points to a user's account via the user_backend service.

    Args:
        user_id: The ID of the user
        points: The number of points to add (can be negative to deduct points)
        db_client: Not used, kept for compatibility

    Returns:
        Updated user data

    Raises:
        HTTPException: If there's an error communicating with the user_backend service
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{settings.user_backend_url}/users/{user_id}/points",
                json={"points": points}
            )

            if response.status_code != 200:
                logger.error(f"Error adding points to user {user_id}: {response.text}")
                raise HTTPException(status_code=response.status_code, detail=f"Error from user service: {response.text}")

            return response.json()
    except httpx.RequestError as e:
        logger.error(f"Error communicating with user_backend service: {e}")
        raise HTTPException(status_code=503, detail=f"User service unavailable: {str(e)}")

async def add_card_to_user(
    user_id: str,
    card_reference: str,
    db_client: firestore.AsyncClient = None,
    collection_id: str = None
) -> Dict[str, Any]:
    """
    Add a card to a user's collection via the user_backend service.

    Args:
        user_id: The ID of the user
        card_reference: Reference to the card in format "collection/card_id"
        db_client: Not used, kept for compatibility
        collection_id: Optional collection ID override

    Returns:
        Success message

    Raises:
        HTTPException: If there's an error communicating with the user_backend service
    """
    try:
        async with httpx.AsyncClient() as client:
            payload = {
                "card_references": [card_reference]
            }
            collection_metadata_id = collection_id if collection_id else card_reference.split('/')[0]

            response = await client.post(
                f"{settings.user_backend_url}/users/{user_id}/cards?collection_metadata_id={collection_metadata_id}",
                json=payload
            )

            if response.status_code != 200:
                logger.error(f"Error adding card to user {user_id}: {response.text}")
                raise HTTPException(status_code=response.status_code, detail=f"Error from user service: {response.text}")

            return response.json()
    except httpx.RequestError as e:
        logger.error(f"Error communicating with user_backend service: {e}")
        raise HTTPException(status_code=503, detail=f"User service unavailable: {str(e)}")

router = APIRouter(
    prefix="/marketplace",
    tags=["marketplace"],
    responses={404: {"description": "Not found"}},
)

@router.post("/official_listing")
async def add_to_official_listing_endpoint(
    collection_id: str = Query(..., description="Collection ID the card belongs to"),
    card_id: str = Query(..., description="Card ID to add to the official listing"),
    quantity: int = Query(1, description="Quantity of cards to add to the official listing"),
    pricePoints: int = Query(..., description="Price in points for the card in the official listing"),
    priceCash: int = Query(0, description="Price in cash for the card in the official listing")
):
    """
    Adds a card to the official_listing collection.
    Creates a new collection called "official_listing" if it doesn't exist.
    Adds a subcollection with the provided collection_id.
    Adds the card under that subcollection with the specified fields.

    Parameters:
    - collection_id: The ID of the collection the card belongs to
    - card_id: The ID of the card to add to the official listing
    - quantity: The quantity of cards to add to the official listing (default: 1)
    - pricePoints: The price in points for the card in the official listing (required)
    - priceCash: The price in cash for the card in the official listing (default: 0)
    """
    try:
        result = await add_to_official_listing(collection_id, card_id, quantity, pricePoints, priceCash)
        return {
            "status": "success",
            "message": f"Card {card_id} from collection {collection_id} added to official listing with quantity {quantity}, pricePoints {pricePoints}, and priceCash {priceCash}",
            "data": result
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in add_to_official_listing_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@router.get("/official_listings")
async def get_official_listings_endpoint(
    collection_id: str = Query(..., description="Collection ID to get official listings for")
):
    """
    Retrieves all cards from the official_listing collection for a specific collection.

    Parameters:
    - collection_id: The ID of the collection to get official listings for
    """
    try:
        result = await get_all_official_listings(collection_id)
        return {
            "status": "success",
            "message": f"Retrieved {len(result)} cards from official listing for collection {collection_id}",
            "data": result
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in get_official_listings_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@router.post("/withdraw_official_listing")
async def withdraw_official_listing_endpoint(
    collection_id: str = Query(..., description="Collection ID the card belongs to"),
    card_id: str = Query(..., description="Card ID to withdraw from the official listing"),
    quantity: int = Query(1, description="Quantity of cards to withdraw from the official listing")
):
    """
    Withdraws a card from the official_listing collection.
    This endpoint reverses what the official_listing endpoint does:
    - Gets the card from the official_listing collection
    - Updates the original card in Firestore:
      - Increases the quantity field by the specified quantity
      - Decreases the quantity_in_offical_marketplace field by the specified quantity
    - Updates the card in the official_listing collection:
      - Decreases the quantity field by the specified quantity
      - If the quantity becomes 0, removes the card from the official_listing collection

    Parameters:
    - collection_id: The ID of the collection the card belongs to
    - card_id: The ID of the card to withdraw from the official listing
    - quantity: The quantity of cards to withdraw from the official listing (default: 1)
    """
    try:
        result = await withdraw_from_official_listing(collection_id, card_id, quantity)
        return {
            "status": "success",
            "message": f"Card {card_id} from collection {collection_id} withdrawn from official listing with quantity {quantity}",
            "data": result
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in withdraw_official_listing_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@router.post("/buy_out/{user_id}")
async def buy_out_endpoint(
    user_id: str = Path(..., description="The ID of the user buying the card"),
    collection_id: str = Query(..., description="Collection ID the card belongs to"),
    card_id: str = Query(..., description="Card ID to buy from the official listing"),
    quantity: int = Query(1, description="Quantity of cards to buy (default: 1)"),
    db_client: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Buys a card from the official listing as a transaction:
    1. Gets the card from the official listing
    2. Checks if the user has enough points
    3. Deducts points from the user
    4. Adds the card to the user's collection
    5. Reduces the quantity in the official listing and updates quantity_in_official_marketplace

    All operations are performed in a transaction to ensure atomicity.

    Parameters:
    - user_id: The ID of the user buying the card
    - collection_id: The ID of the collection the card belongs to
    - card_id: The ID of the card to buy from the official listing
    - quantity: The quantity of cards to buy (default: 1)
    """
    try:
        # Use the marketplace service to handle the entire buy operation as a transaction
        result = await buy_card_from_official_listing(
            user_id=user_id,
            collection_id=collection_id,
            card_id=card_id,
            quantity=quantity,
            db_client=db_client
        )

        return {
            "status": "success",
            "message": f"Successfully bought {quantity} card(s) {card_id} from collection {collection_id}",
            "data": result
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in buy_out_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
