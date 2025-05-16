from fastapi import APIRouter, HTTPException, Depends, Path, Body
from google.cloud import firestore
from typing import List

from models.schemas import CardListing, CreateCardListingRequest, OfferPointsRequest
from service.user_service import create_card_listing, withdraw_listing, offer_points, withdraw_offer, get_user_listings, get_listing_by_id
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["marketplace"],
)

@router.post("/{user_id}/listings", response_model=CardListing)
async def create_card_listing_route(
    user_id: str = Path(..., description="The ID of the user creating the listing"),
    listing_request: CreateCardListingRequest = Body(..., description="The listing details"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a listing for a card that a user wants to sell.

    This endpoint:
    1. Takes a user ID and listing details as arguments
    2. Verifies the user has the card and enough quantity
    3. Creates a new document in the "listings" collection
    4. Reduces the quantity of the card in the user's collection
    5. Returns the created listing
    """
    try:
        listing = await create_card_listing(
            user_id=user_id,
            listing_request=listing_request,
            db_client=db
        )
        return listing
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating listing for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the listing")

@router.delete("/{user_id}/listings/{listing_id}", response_model=dict)
async def withdraw_listing_route(
    user_id: str = Path(..., description="The ID of the user withdrawing the listing"),
    listing_id: str = Path(..., description="The ID of the listing to withdraw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Withdraw a listing for a card that a user has put up for sale.

    This endpoint:
    1. Takes a user ID and listing ID as arguments
    2. Verifies the listing exists and the user is the owner
    3. Updates the user's card by decreasing locked_quantity and increasing quantity
    4. Deletes the listing
    5. Returns a success message
    """
    try:
        result = await withdraw_listing(
            user_id=user_id,
            listing_id=listing_id,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error withdrawing listing for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while withdrawing the listing")

@router.post("/{user_id}/listings/{listing_id}/offers/points", response_model=CardListing)
async def offer_points_route(
    user_id: str = Path(..., description="The ID of the user making the offer"),
    listing_id: str = Path(..., description="The ID of the listing to offer points for"),
    offer_request: OfferPointsRequest = Body(..., description="The points to offer"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Offer points for a listing.

    This endpoint:
    1. Takes a user ID, listing ID, and points to offer as arguments
    2. Verifies the user exists and has enough points
    3. Verifies the listing exists
    4. Creates a new offer document in the "offers" subcollection under the listing
    5. If it's the highest offer, updates the highestOfferPoints field in the listing document
    6. Returns the updated listing
    """
    try:
        listing = await offer_points(
            user_id=user_id,
            listing_id=listing_id,
            offer_request=offer_request,
            db_client=db
        )
        return listing
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error offering points for listing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while offering points for the listing")

@router.get("/{user_id}/listings", response_model=List[CardListing])
async def get_user_listings_route(
    user_id: str = Path(..., description="The ID of the user to get listings for"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all listings for a user.

    This endpoint:
    1. Takes a user ID as a path parameter
    2. Retrieves all listings where the user is the owner
    3. Returns a list of CardListing objects
    """
    try:
        listings = await get_user_listings(
            user_id=user_id,
            db_client=db
        )
        return listings
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting listings for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the listings")

@router.get("/{user_id}/listings/{listing_id}", response_model=CardListing)
async def get_listing_route(
    user_id: str = Path(..., description="The ID of the user"),
    listing_id: str = Path(..., description="The ID of the listing to retrieve"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get information about a specific listing.

    This endpoint:
    1. Takes a user ID and listing ID as path parameters
    2. Retrieves the listing from the database
    3. Returns the listing details

    Args:
        user_id: The ID of the user (not used in the function but required for consistent API pattern)
        listing_id: The ID of the listing to retrieve
        db: Firestore async client

    Returns:
        CardListing: The listing object with all its details
    """
    try:
        listing = await get_listing_by_id(
            listing_id=listing_id,
            db_client=db
        )
        return listing
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting listing {listing_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the listing")

@router.delete("/{user_id}/listings/{listing_id}/offers/{offer_id}", response_model=dict)
async def withdraw_offer_route(
    user_id: str = Path(..., description="The ID of the user withdrawing the offer"),
    listing_id: str = Path(..., description="The ID of the listing the offer was made for"),
    offer_id: str = Path(..., description="The ID of the offer to withdraw"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Withdraw an offer for a listing.

    This endpoint:
    1. Takes a user ID, listing ID, and offer ID as arguments
    2. Verifies the offer exists and belongs to the user
    3. Deletes the offer from the listing's "offers" subcollection
    4. Deletes the corresponding offer from the user's "my_offers" subcollection
    5. If it was the highest offer, updates the listing's highestOfferPoints field
    6. Returns a success message
    """
    try:
        result = await withdraw_offer(
            user_id=user_id,
            listing_id=listing_id,
            offer_id=offer_id,
            db_client=db
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error withdrawing offer for listing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while withdrawing the offer")
