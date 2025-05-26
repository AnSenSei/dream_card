from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, timedelta

from fastapi import HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, SERVER_TIMESTAMP, async_transactional, Increment

from config import get_logger, settings
from models.schemas import CreateCardListingRequest, OfferPointsRequest, OfferCashRequest, UpdatePointOfferRequest, UpdateCashOfferRequest, CardListing

from service.card_service import get_user_card, add_card_to_user
from utils.gcs_utils import generate_signed_url, upload_avatar_to_gcs, parse_base64_image
from config.db_connection import db_connection

logger = get_logger(__name__)

async def withdraw_listing(
    user_id: str,
    listing_id: str,
    db_client: AsyncClient
) -> dict:
    """
    Withdraw a listing for a card that a user has put up for sale.

    This function:
    1. Verifies the listing exists
    2. Verifies the user is the owner of the listing
    3. Gets the card reference and quantity from the listing
    4. Updates the user's card in a transaction to decrease locked_quantity and increase quantity
    5. Deletes the listing
    6. Returns a success message

    Args:
        user_id: The ID of the user withdrawing the listing
        listing_id: The ID of the listing to withdraw
        db_client: Firestore async client

    Returns:
        dict: A dictionary with a success message

    Raises:
        HTTPException: If there's an error withdrawing the listing
    """
    try:
        # 1. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 2. Verify user is the owner of the listing
        owner_reference = listing_data.get("owner_reference", "")
        expected_owner_path = f"{settings.firestore_collection_users}/{user_id}"

        if owner_reference != expected_owner_path:
            raise HTTPException(status_code=403, detail="You are not authorized to withdraw this listing")

        # 3. Get card reference and quantity from the listing
        card_reference = listing_data.get("card_reference", "")
        listing_quantity = listing_data.get("quantity", 0)

        if not card_reference or listing_quantity <= 0:
            raise HTTPException(status_code=400, detail="Invalid listing data")

        # Parse card_reference to get collection_id and card_id
        try:
            collection_id, card_id = card_reference.split('/')
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid card reference format: {card_reference}")

        collection_id = listing_data.get("collection_id", collection_id)
        logger.info(f"collection_id: {collection_id}")

        # Get reference to the user's card
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        card_ref = user_ref.collection('cards').document('cards').collection(collection_id).document(card_id)

        # Check if the card still exists
        card_doc = await card_ref.get()

        # Define the transaction function
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            if card_doc.exists:
                # Card exists, update quantity
                card_data = card_doc.to_dict()
                current_quantity = card_data.get('quantity', 0)
                current_locked_quantity = card_data.get('locked_quantity', 0)

                # Ensure we don't go below zero for locked_quantity
                new_locked_quantity = max(0, current_locked_quantity - listing_quantity)

                # Update the card with incremented quantity and decremented locked_quantity
                tx.update(card_ref, {
                    'quantity': current_quantity + listing_quantity,
                    'locked_quantity': new_locked_quantity
                })

            # Delete the listing
            tx.delete(listing_ref)

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        logger.info(f"Successfully withdrew listing {listing_id} for user {user_id}")
        return {"message": f"Listing {listing_id} withdrawn successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error withdrawing listing {listing_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to withdraw listing: {str(e)}")

async def create_card_listing(
    user_id: str,
    listing_request: CreateCardListingRequest,
    db_client: AsyncClient
) -> CardListing:
    """
    Create a listing for a card that a user wants to sell.

    This function:
    1. Verifies the user exists
    2. Checks if the user has the card and enough quantity
    3. Creates a new document in the "listings" collection
    4. Reduces the quantity of the card in the user's collection
    5. Returns the created listing

    Args:
        user_id: The ID of the user creating the listing
        listing_request: The CreateCardListingRequest containing listing details
        db_client: Firestore async client

    Returns:
        CardListing: The created listing

    Raises:
        HTTPException: If there's an error creating the listing
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Get collection_id and card_id from the request
        collection_id = listing_request.collection_id
        card_id = listing_request.card_id

        # 3. Get the user's card to retrieve card_reference and card data
        user_card = await get_user_card(
            user_id=user_id,
            collection_id=collection_id,
            card_id=card_id,
            db_client=db_client
        )

        # Get card_reference from the user's card
        card_reference = user_card.card_reference

        # 4. Check if user has enough available quantity (total quantity minus locked quantity)
        available_quantity = user_card.quantity
        if available_quantity < listing_request.quantity:
            raise HTTPException(
                status_code=400,
                detail=f"Not enough cards available. Requested: {listing_request.quantity}, Available: {available_quantity}"
            )

        # Get reference to the card document for the transaction
        card_ref = user_ref.collection('cards').document('cards').collection(collection_id).document(card_id)

        # 4. Create listing document
        now = datetime.now()
        listing_data = {
            "owner_reference": user_ref.path,  # Reference to the seller user document
            "card_reference": card_reference,  # Card global ID
            "collection_id": collection_id,  # Collection ID of the card
            "quantity": listing_request.quantity,  # Quantity being listed
            "createdAt": now,
            "pricePoints": listing_request.pricePoints,
            "priceCash": listing_request.priceCash,
            "image_url": user_card.image_url  # Add image_url from the user's card
        }

        # Add expiration date if provided
        if listing_request.expiresAt:
            listing_data["expiresAt"] = listing_request.expiresAt

        # 5. Create a new document in the listings collection
        listings_ref = db_client.collection('listings')
        new_listing_ref = listings_ref.document()  # Auto-generate ID

        # 6. Update user's card locked_quantity and quantity in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Increase the locked_quantity
            new_locked_quantity = user_card.locked_quantity + listing_request.quantity

            # Decrease the quantity
            new_quantity = user_card.quantity - listing_request.quantity

            # Update both locked_quantity and quantity
            tx.update(card_ref, {
                "locked_quantity": new_locked_quantity,
                "quantity": new_quantity
            })

            # Create the listing
            tx.set(new_listing_ref, listing_data)

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 7. Get the created listing
        listing_doc = await new_listing_ref.get()
        listing_data = listing_doc.to_dict()

        # 8. Create and return a CardListing object
        listing = CardListing(
            id=new_listing_ref.id,  # Include the listing ID
            owner_reference=listing_data["owner_reference"],
            card_reference=listing_data["card_reference"],
            collection_id=listing_data["collection_id"],
            quantity=listing_data["quantity"],
            createdAt=listing_data["createdAt"],
            pricePoints=listing_data.get("pricePoints"),
            priceCash=listing_data.get("priceCash"),
            expiresAt=listing_data.get("expiresAt"),
            highestOfferPoints=listing_data.get("highestOfferPoints"),
            highestOfferCash=listing_data.get("highestOfferCash"),
            image_url=listing_data.get("image_url")
        )

        logger.info(f"Successfully created listing {new_listing_ref.id} for card {card_reference} by user {user_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating listing for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create listing: {str(e)}")

async def get_user_listings(
    user_id: str,
    db_client: AsyncClient
) -> List[CardListing]:
    """
    Get all listings for a user.

    This function:
    1. Verifies the user exists
    2. Queries the listings collection for documents where owner_reference matches the user's path
    3. Converts the Firestore documents to CardListing objects
    4. Returns a list of CardListing objects

    Args:
        user_id: The ID of the user to get listings for
        db_client: Firestore async client

    Returns:
        List[CardListing]: A list of CardListing objects

    Raises:
        HTTPException: If there's an error getting the listings
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Query the listings collection for documents where owner_reference matches the user's path
        listings_ref = db_client.collection('listings')
        query = listings_ref.where("owner_reference", "==", user_ref.path)

        # 3. Execute the query
        listings_docs = await query.get()

        # 4. Convert the Firestore documents to CardListing objects
        listings = []
        for doc in listings_docs:
            listing_data = doc.to_dict()
            listing_data['id'] = doc.id  # Add the document ID to the data

            # Generate signed URL for the card image if it exists
            if 'image_url' in listing_data and listing_data['image_url']:
                try:
                    listing_data['image_url'] = await generate_signed_url(listing_data['image_url'])
                except Exception as sign_error:
                    logger.error(f"Failed to generate signed URL for {listing_data['image_url']}: {sign_error}")
                    # Keep the original URL if signing fails

            # Create a CardListing object
            listing = CardListing(
                id=listing_data["id"],  # Include the listing ID
                owner_reference=listing_data["owner_reference"],
                card_reference=listing_data["card_reference"],
                collection_id=listing_data.get("collection_id", ""),
                quantity=listing_data["quantity"],
                createdAt=listing_data["createdAt"],
                pricePoints=listing_data.get("pricePoints"),
                priceCash=listing_data.get("priceCash"),
                expiresAt=listing_data.get("expiresAt"),
                highestOfferPoints=listing_data.get("highestOfferPoints"),
                highestOfferCash=listing_data.get("highestOfferCash"),
                image_url=listing_data.get("image_url")
            )
            listings.append(listing)

        logger.info(f"Successfully retrieved {len(listings)} listings for user {user_id}")
        return listings

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting listings for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get listings: {str(e)}")

async def get_listing_by_id(
    listing_id: str,
    db_client: AsyncClient
) -> CardListing:
    """
    Get a listing by its ID.

    This function:
    1. Verifies the listing exists
    2. Retrieves the listing document from Firestore
    3. Converts the Firestore document to a CardListing object
    4. Returns the CardListing object

    Args:
        listing_id: The ID of the listing to retrieve
        db_client: Firestore async client

    Returns:
        CardListing: The listing object

    Raises:
        HTTPException: If there's an error getting the listing or if the listing doesn't exist
    """
    try:
        # 1. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        # 2. Get the listing data
        listing_data = listing_doc.to_dict()
        listing_data['id'] = listing_doc.id  # Add the document ID to the data

        # 3. Generate signed URL for the card image if it exists
        if 'image_url' in listing_data and listing_data['image_url']:
            try:
                listing_data['image_url'] = await generate_signed_url(listing_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {listing_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # 4. Create and return a CardListing object
        listing = CardListing(
            id=listing_data["id"],  # Include the listing ID
            owner_reference=listing_data["owner_reference"],
            card_reference=listing_data["card_reference"],
            collection_id=listing_data.get("collection_id", ""),
            quantity=listing_data["quantity"],
            createdAt=listing_data["createdAt"],
            pricePoints=listing_data.get("pricePoints"),
            priceCash=listing_data.get("priceCash"),
            expiresAt=listing_data.get("expiresAt"),
            highestOfferPoints=listing_data.get("highestOfferPoints"),
            highestOfferCash=listing_data.get("highestOfferCash"),
            image_url=listing_data.get("image_url")
        )

        logger.info(f"Successfully retrieved listing {listing_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting listing {listing_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get listing: {str(e)}")

async def withdraw_offer(
    user_id: str,
    listing_id: str,
    offer_id: str,
    db_client: AsyncClient
) -> dict:
    """
    Withdraw a point offer for a listing.

    This function:
    1. Verifies the user exists
    2. Verifies the listing exists
    3. Verifies the offer exists and belongs to the user
    4. Deletes the offer from the listing's "point_offers" subcollection
    5. Deletes the corresponding offer from the user's "my_point_offers" subcollection
    6. If it was the highest offer, updates the listing's highestOfferPoints field
    7. Returns a success message

    Args:
        user_id: The ID of the user withdrawing the offer
        listing_id: The ID of the listing the offer was made for
        offer_id: The ID of the offer to withdraw
        db_client: Firestore async client

    Returns:
        dict: A dictionary with a success message

    Raises:
        HTTPException: If there's an error withdrawing the offer
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()
        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Verify offer exists and belongs to the user
        offer_ref = listing_ref.collection('point_offers').document(offer_id)
        offer_doc = await offer_ref.get()
        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Point offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()
        expected_offerer_path = f"{settings.firestore_collection_users}/{user_id}"
        if offer_data.get("offererRef", "") != expected_offerer_path:
            raise HTTPException(status_code=403, detail="You are not authorized to withdraw this offer")

        # 4. Find the corresponding offer in the user's my_point_offers subcollection
        my_point_offers_ref = user_ref.collection('my_point_offers')
        my_point_offers_query = my_point_offers_ref.where("listingId", "==", listing_id)
        my_point_offers_docs = await my_point_offers_query.get()

        my_offer_ref = None
        for doc in my_point_offers_docs:
            my_offer_data = doc.to_dict()
            # Check if this is the same offer by comparing amount and timestamp
            if (my_offer_data.get("amount") == offer_data.get("amount") and
                my_offer_data.get("at") == offer_data.get("at")):
                my_offer_ref = doc.reference
                break

        if not my_offer_ref:
            logger.warning(f"Could not find corresponding my_offer for offer {offer_id} in user {user_id}'s my_offers collection")

        # 5. Check if this is the highest offer
        current_highest_offer = listing_data.get("highestOfferPoints", None)
        is_highest_offer = False

        if current_highest_offer and offer_data.get("offerreference") == current_highest_offer.get("offerreference"):
            is_highest_offer = True

        # 6. Delete the offers in a transaction
        @firestore.async_transactional
        async def _delete_txn(tx: firestore.AsyncTransaction):
            # Delete the offer from the listing's offers subcollection
            tx.delete(offer_ref)

            # Delete the corresponding offer from the user's my_offers subcollection if found
            if my_offer_ref:
                tx.delete(my_offer_ref)

        # Execute the delete transaction
        delete_transaction = db_client.transaction()
        await _delete_txn(delete_transaction)

        # 7. If this was the highest offer, find the next highest offer and update the listing
        if is_highest_offer:
            # Get all remaining offers and sort them by amount to find the highest
            offers_query = listing_ref.collection('point_offers').order_by("amount", direction=firestore.Query.DESCENDING).limit(1)
            offers_snapshot = await offers_query.get()

            logger.info(f"Found {len(offers_snapshot)} point offers after withdrawal")

            # Update the listing in a separate transaction
            @firestore.async_transactional
            async def _update_txn(tx: firestore.AsyncTransaction):
                if offers_snapshot and len(offers_snapshot) > 0:
                    # There is a new highest offer
                    new_highest_offer = offers_snapshot[0].to_dict()
                    logger.info(f"Setting new highest point offer: {new_highest_offer}")
                    tx.update(listing_ref, {
                        "highestOfferPoints": new_highest_offer
                    })
                else:
                    # No more offers, remove the highest offer field
                    logger.info(f"No more point offers, removing highestOfferPoints field")
                    tx.update(listing_ref, {
                        "highestOfferPoints": firestore.DELETE_FIELD
                    })

            # Execute the update transaction
            update_transaction = db_client.transaction()
            await _update_txn(update_transaction)

        logger.info(f"Successfully withdrew point offer {offer_id} for listing {listing_id} by user {user_id}")
        return {"message": f"Point offer for listing {listing_id} withdrawn successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error withdrawing point offer {offer_id} for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to withdraw point offer: {str(e)}")

async def withdraw_cash_offer(
    user_id: str,
    listing_id: str,
    offer_id: str,
    db_client: AsyncClient
) -> dict:
    """
    Withdraw a cash offer for a listing.

    This function:
    1. Verifies the user exists
    2. Verifies the listing exists
    3. Verifies the offer exists and belongs to the user
    4. Deletes the offer from the listing's "cash_offers" subcollection
    5. Deletes the corresponding offer from the user's "my_cash_offers" subcollection
    6. If it was the highest offer, updates the listing's highestOfferCash field
    7. Returns a success message

    Args:
        user_id: The ID of the user withdrawing the offer
        listing_id: The ID of the listing the offer was made for
        offer_id: The ID of the offer to withdraw
        db_client: Firestore async client

    Returns:
        dict: A dictionary with a success message

    Raises:
        HTTPException: If there's an error withdrawing the offer
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()
        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Verify offer exists and belongs to the user
        offer_ref = listing_ref.collection('cash_offers').document(offer_id)
        offer_doc = await offer_ref.get()
        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Cash offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()
        expected_offerer_path = f"{settings.firestore_collection_users}/{user_id}"
        if offer_data.get("offererRef", "") != expected_offerer_path:
            raise HTTPException(status_code=403, detail="You are not authorized to withdraw this offer")

        # 4. Find the corresponding offer in the user's my_cash_offers subcollection
        my_cash_offers_ref = user_ref.collection('my_cash_offers')
        my_cash_offers_query = my_cash_offers_ref.where("listingId", "==", listing_id)
        my_cash_offers_docs = await my_cash_offers_query.get()

        my_offer_ref = None
        for doc in my_cash_offers_docs:
            my_offer_data = doc.to_dict()
            # Check if this is the same offer by comparing amount and timestamp
            if (my_offer_data.get("amount") == offer_data.get("amount") and
                my_offer_data.get("at") == offer_data.get("at")):
                my_offer_ref = doc.reference
                break

        if not my_offer_ref:
            logger.warning(f"Could not find corresponding my_offer for offer {offer_id} in user {user_id}'s my_offers collection")

        # 5. Check if this is the highest offer
        current_highest_offer = listing_data.get("highestOfferCash", None)
        is_highest_offer = False

        if current_highest_offer and offer_data.get("offerreference") == current_highest_offer.get("offerreference"):
            is_highest_offer = True

        # 6. Delete the offers in a transaction
        @firestore.async_transactional
        async def _delete_txn(tx: firestore.AsyncTransaction):
            # Delete the offer from the listing's offers subcollection
            tx.delete(offer_ref)

            # Delete the corresponding offer from the user's my_offers subcollection if found
            if my_offer_ref:
                tx.delete(my_offer_ref)

        # Execute the delete transaction
        delete_transaction = db_client.transaction()
        await _delete_txn(delete_transaction)

        # 7. If this was the highest offer, find the next highest offer and update the listing
        if is_highest_offer:
            # Get all remaining offers and sort them by amount to find the highest
            offers_query = listing_ref.collection('cash_offers').order_by("amount", direction=firestore.Query.DESCENDING).limit(1)
            offers_snapshot = await offers_query.get()

            logger.info(f"Found {len(offers_snapshot)} cash offers after withdrawal")

            # Update the listing in a separate transaction
            @firestore.async_transactional
            async def _update_txn(tx: firestore.AsyncTransaction):
                if offers_snapshot and len(offers_snapshot) > 0:
                    # There is a new highest offer
                    new_highest_offer = offers_snapshot[0].to_dict()
                    logger.info(f"Setting new highest cash offer: {new_highest_offer}")
                    tx.update(listing_ref, {
                        "highestOfferCash": new_highest_offer
                    })
                else:
                    # No more offers, remove the highest offer field
                    logger.info(f"No more cash offers, removing highestOfferCash field")
                    tx.update(listing_ref, {
                        "highestOfferCash": firestore.DELETE_FIELD
                    })

            # Execute the update transaction
            update_transaction = db_client.transaction()
            await _update_txn(update_transaction)

        logger.info(f"Successfully withdrew cash offer {offer_id} for listing {listing_id} by user {user_id}")
        return {"message": f"Cash offer for listing {listing_id} withdrawn successfully"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error withdrawing cash offer {offer_id} for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to withdraw cash offer: {str(e)}")


async def offer_points(
    user_id: str,
    listing_id: str,
    offer_request: OfferPointsRequest,
    db_client: AsyncClient,
    expired: int = 7
) -> CardListing:
    """
    Offer points for a listing.

    This function:
    1. Verifies the listing exists
    2. Verifies the user exists
    3. Creates a new offer document in the "point_offers" subcollection under the listing
    4. Creates a new offer document in the "my_point_offers" subcollection under the user
    5. If it's the highest offer, updates the highestOfferPoint field in the listing document
    6. Returns the updated listing

    Args:
        user_id: The ID of the user making the offer
        listing_id: The ID of the listing to offer points for
        offer_request: The OfferPointsRequest containing the points to offer
        db_client: Firestore async client
        expired: Number of days until the offer expires (default: 7)

    Returns:
        CardListing: The updated listing

    Raises:
        HTTPException: If there's an error offering points for the listing
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Check if the user is the owner of the listing
        owner_reference = listing_data.get("owner_reference", "")
        expected_owner_path = f"{settings.firestore_collection_users}/{user_id}"

        if owner_reference == expected_owner_path:
            raise HTTPException(status_code=400, detail="You cannot offer points for your own listing")

        # 4. Create a new offer document in the "point_offers" subcollection
        now = datetime.now()
        expires_at = now + timedelta(days=expired)  # Calculate expiration date

        # Get the point_offers subcollection reference
        point_offers_ref = listing_ref.collection('point_offers')
        new_offer_ref = point_offers_ref.document()  # Auto-generate ID

        offer_data = {
            "offererRef": user_ref.path,  # Reference to the user making the offer
            "amount": offer_request.points,  # Points offered
            "at": now,  # Timestamp of the offer
            "offerreference": new_offer_ref.id,  # Reference to this offer
            "type": "point",  # Indicate this is a point offer
            "expiresAt": expires_at  # Add expiration date
        }

        # Get the user's my_point_offers subcollection reference
        my_point_offers_ref = user_ref.collection('my_point_offers')
        new_my_offer_ref = my_point_offers_ref.document()  # Auto-generate ID

        # Create my_offer_data with additional listing information
        my_offer_data = {
            **offer_data,  # Include all offer data
            "listingId": listing_id,  # Reference to the listing
            "card_reference": listing_data.get("card_reference", ""),  # Card reference from the listing
            "collection_id": listing_data.get("collection_id", ""),  # Collection ID from the listing
            "image_url": listing_data.get("image_url", "")  # Image URL from the listing
        }

        # 5. Check if this is the highest offer
        current_highest_offer = listing_data.get("highestOfferPoints", None)
        is_highest_offer = False

        if current_highest_offer is None or offer_request.points > current_highest_offer.get("amount", 0):
            is_highest_offer = True

        # 6. Update the listing and create the offer in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Create the offer in the listing's offers subcollection
            tx.set(new_offer_ref, offer_data)

            # Create the offer in the user's my_offers subcollection
            tx.set(new_my_offer_ref, my_offer_data)

            # If this is the highest offer, update the listing
            if is_highest_offer:
                tx.update(listing_ref, {
                    "highestOfferPoints": offer_data
                })

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 7. Get the updated listing
        updated_listing_doc = await listing_ref.get()
        updated_listing_data = updated_listing_doc.to_dict()

        # 8. Create and return a CardListing object
        listing = CardListing(
            id=listing_id,  # Include the listing ID
            owner_reference=updated_listing_data["owner_reference"],
            card_reference=updated_listing_data["card_reference"],
            collection_id=updated_listing_data["collection_id"],
            quantity=updated_listing_data["quantity"],
            createdAt=updated_listing_data["createdAt"],
            pricePoints=updated_listing_data.get("pricePoints"),
            priceCash=updated_listing_data.get("priceCash"),
            expiresAt=updated_listing_data.get("expiresAt"),
            highestOfferPoints=updated_listing_data.get("highestOfferPoints"),
            highestOfferCash=updated_listing_data.get("highestOfferCash"),
            image_url=updated_listing_data.get("image_url")
        )

        logger.info(f"Successfully created offer for listing {listing_id} by user {user_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error offering points for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to offer points for listing: {str(e)}")

async def update_point_offer(
    user_id: str,
    listing_id: str,
    offer_id: str,
    update_request: UpdatePointOfferRequest,
    db_client: AsyncClient
) -> CardListing:
    """
    Update a point offer for a listing with a higher amount.

    This function:
    1. Verifies the user exists
    2. Verifies the listing exists
    3. Verifies the offer exists and belongs to the user
    4. Verifies the new amount is higher than the current amount
    5. Updates the offer document in the "point_offers" subcollection under the listing
    6. Updates the corresponding offer in the user's "my_point_offers" subcollection
    7. If it becomes the highest offer, updates the highestOfferPoints field in the listing document
    8. Returns the updated listing

    Args:
        user_id: The ID of the user updating the offer
        listing_id: The ID of the listing the offer was made for
        offer_id: The ID of the offer to update
        update_request: The UpdatePointOfferRequest containing the new points to offer
        db_client: Firestore async client

    Returns:
        CardListing: The updated listing

    Raises:
        HTTPException: If there's an error updating the point offer
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Verify offer exists and belongs to the user
        offer_ref = listing_ref.collection('point_offers').document(offer_id)
        offer_doc = await offer_ref.get()
        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Point offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()
        expected_offerer_path = f"{settings.firestore_collection_users}/{user_id}"
        if offer_data.get("offererRef", "") != expected_offerer_path:
            raise HTTPException(status_code=403, detail="You are not authorized to update this offer")

        # 4. Verify the new amount is higher than the current amount
        current_amount = offer_data.get("amount", 0)
        if update_request.points <= current_amount:
            raise HTTPException(status_code=400, detail="New offer amount must be higher than the current amount")

        # 5. Find the corresponding offer in the user's my_point_offers subcollection
        my_point_offers_ref = user_ref.collection('my_point_offers')
        my_point_offers_query = my_point_offers_ref.where("listingId", "==", listing_id)
        my_point_offers_docs = await my_point_offers_query.get()

        my_offer_ref = None
        for doc in my_point_offers_docs:
            my_offer_data = doc.to_dict()
            # Check if this is the same offer by comparing offerreference
            if my_offer_data.get("offerreference") == offer_id:
                my_offer_ref = doc.reference
                break

        if not my_offer_ref:
            logger.warning(f"Could not find corresponding my_offer for offer {offer_id} in user {user_id}'s my_point_offers collection")
            raise HTTPException(status_code=404, detail=f"Could not find corresponding my_offer for offer {offer_id}")

        # 6. Update the offer data with the new amount
        now = datetime.now()
        updated_offer_data = {
            **offer_data,
            "amount": update_request.points,
            "at": now  # Update the timestamp
        }

        # Create updated my_offer_data with the new amount
        updated_my_offer_data = {
            **offer_data,
            "amount": update_request.points,
            "at": now,
            "listingId": listing_id,
            "card_reference": listing_data.get("card_reference", ""),
            "collection_id": listing_data.get("collection_id", ""),
            "image_url": listing_data.get("image_url", "")
        }

        # 7. Check if this will be the highest offer
        current_highest_offer = listing_data.get("highestOfferPoints", None)
        is_highest_offer = False

        if current_highest_offer is None or update_request.points > current_highest_offer.get("amount", 0):
            is_highest_offer = True
        elif current_highest_offer.get("offerreference") == offer_id and update_request.points > current_amount:
            # This is already the highest offer and we're increasing the amount
            is_highest_offer = True

        # 8. Update the offers and possibly the listing in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Update the offer in the listing's point_offers subcollection
            tx.update(offer_ref, {
                "amount": update_request.points,
                "at": now
            })

            # Update the offer in the user's my_point_offers subcollection
            tx.update(my_offer_ref, {
                "amount": update_request.points,
                "at": now
            })

            # If this will be the highest offer, update the listing
            if is_highest_offer:
                tx.update(listing_ref, {
                    "highestOfferPoints": updated_offer_data
                })

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 9. Get the updated listing
        updated_listing_doc = await listing_ref.get()
        updated_listing_data = updated_listing_doc.to_dict()

        # 10. Create and return a CardListing object
        listing = CardListing(
            id=listing_id,  # Include the listing ID
            owner_reference=updated_listing_data["owner_reference"],
            card_reference=updated_listing_data["card_reference"],
            collection_id=updated_listing_data["collection_id"],
            quantity=updated_listing_data["quantity"],
            createdAt=updated_listing_data["createdAt"],
            pricePoints=updated_listing_data.get("pricePoints"),
            priceCash=updated_listing_data.get("priceCash"),
            expiresAt=updated_listing_data.get("expiresAt"),
            highestOfferPoints=updated_listing_data.get("highestOfferPoints"),
            highestOfferCash=updated_listing_data.get("highestOfferCash"),
            image_url=updated_listing_data.get("image_url")
        )

        logger.info(f"Successfully updated point offer {offer_id} for listing {listing_id} by user {user_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating point offer {offer_id} for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update point offer: {str(e)}")

async def update_cash_offer(
    user_id: str,
    listing_id: str,
    offer_id: str,
    update_request: UpdateCashOfferRequest,
    db_client: AsyncClient
) -> CardListing:
    """
    Update a cash offer for a listing with a higher amount.

    This function:
    1. Verifies the user exists
    2. Verifies the listing exists
    3. Verifies the offer exists and belongs to the user
    4. Verifies the new amount is higher than the current amount
    5. Updates the offer document in the "cash_offers" subcollection under the listing
    6. Updates the corresponding offer in the user's "my_cash_offers" subcollection
    7. If it becomes the highest offer, updates the highestOfferCash field in the listing document
    8. Returns the updated listing

    Args:
        user_id: The ID of the user updating the offer
        listing_id: The ID of the listing the offer was made for
        offer_id: The ID of the offer to update
        update_request: The UpdateCashOfferRequest containing the new cash amount to offer
        db_client: Firestore async client

    Returns:
        CardListing: The updated listing

    Raises:
        HTTPException: If there's an error updating the cash offer
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Verify offer exists and belongs to the user
        offer_ref = listing_ref.collection('cash_offers').document(offer_id)
        offer_doc = await offer_ref.get()
        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Cash offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()
        expected_offerer_path = f"{settings.firestore_collection_users}/{user_id}"
        if offer_data.get("offererRef", "") != expected_offerer_path:
            raise HTTPException(status_code=403, detail="You are not authorized to update this offer")

        # 4. Verify the new amount is higher than the current amount
        current_amount = offer_data.get("amount", 0)
        if update_request.cash <= current_amount:
            raise HTTPException(status_code=400, detail="New offer amount must be higher than the current amount")

        # 5. Find the corresponding offer in the user's my_cash_offers subcollection
        my_cash_offers_ref = user_ref.collection('my_cash_offers')
        my_cash_offers_query = my_cash_offers_ref.where("listingId", "==", listing_id)
        my_cash_offers_docs = await my_cash_offers_query.get()

        my_offer_ref = None
        for doc in my_cash_offers_docs:
            my_offer_data = doc.to_dict()
            # Check if this is the same offer by comparing offerreference
            if my_offer_data.get("offerreference") == offer_id:
                my_offer_ref = doc.reference
                break

        if not my_offer_ref:
            logger.warning(f"Could not find corresponding my_offer for offer {offer_id} in user {user_id}'s my_cash_offers collection")
            raise HTTPException(status_code=404, detail=f"Could not find corresponding my_offer for offer {offer_id}")

        # 6. Update the offer data with the new amount
        now = datetime.now()
        updated_offer_data = {
            **offer_data,
            "amount": update_request.cash,
            "at": now  # Update the timestamp
        }

        # Create updated my_offer_data with the new amount
        updated_my_offer_data = {
            **offer_data,
            "amount": update_request.cash,
            "at": now,
            "listingId": listing_id,
            "card_reference": listing_data.get("card_reference", ""),
            "collection_id": listing_data.get("collection_id", ""),
            "image_url": listing_data.get("image_url", "")
        }

        # 7. Check if this will be the highest offer
        current_highest_offer = listing_data.get("highestOfferCash", None)
        is_highest_offer = False

        if current_highest_offer is None or update_request.cash > current_highest_offer.get("amount", 0):
            is_highest_offer = True
        elif current_highest_offer.get("offerreference") == offer_id and update_request.cash > current_amount:
            # This is already the highest offer and we're increasing the amount
            is_highest_offer = True

        # 8. Update the offers and possibly the listing in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Update the offer in the listing's cash_offers subcollection
            tx.update(offer_ref, {
                "amount": update_request.cash,
                "at": now
            })

            # Update the offer in the user's my_cash_offers subcollection
            tx.update(my_offer_ref, {
                "amount": update_request.cash,
                "at": now
            })

            # If this will be the highest offer, update the listing
            if is_highest_offer:
                tx.update(listing_ref, {
                    "highestOfferCash": updated_offer_data
                })

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 9. Get the updated listing
        updated_listing_doc = await listing_ref.get()
        updated_listing_data = updated_listing_doc.to_dict()

        # 10. Create and return a CardListing object
        listing = CardListing(
            id=listing_id,  # Include the listing ID
            owner_reference=updated_listing_data["owner_reference"],
            card_reference=updated_listing_data["card_reference"],
            collection_id=updated_listing_data["collection_id"],
            quantity=updated_listing_data["quantity"],
            createdAt=updated_listing_data["createdAt"],
            pricePoints=updated_listing_data.get("pricePoints"),
            priceCash=updated_listing_data.get("priceCash"),
            expiresAt=updated_listing_data.get("expiresAt"),
            highestOfferPoints=updated_listing_data.get("highestOfferPoints"),
            highestOfferCash=updated_listing_data.get("highestOfferCash"),
            image_url=updated_listing_data.get("image_url")
        )

        logger.info(f"Successfully updated cash offer {offer_id} for listing {listing_id} by user {user_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating cash offer {offer_id} for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update cash offer: {str(e)}")


async def accept_offer(
    user_id: str,
    listing_id: str,
    offer_type: str,
    db_client: AsyncClient
) -> CardListing:
    """
    Accept the highest offer (cash or point) for a listing.

    This function:
    1. Verifies the listing exists and belongs to the user
    2. Finds the highest offer of the specified type
    3. Updates the status of the offer to "accepted"
    4. Sets the payment_due date to 2 days after the accept time
    5. Returns the updated listing

    Args:
        user_id: The ID of the user accepting the offer (must be the listing owner)
        listing_id: The ID of the listing
        offer_type: The type of offer to accept ("cash" or "point")
        db_client: Firestore async client

    Returns:
        CardListing: The updated listing

    Raises:
        HTTPException: If there's an error accepting the offer
    """
    logger = get_logger(__name__)
    try:
        # 1. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 2. Verify the user is the owner of the listing
        owner_reference = listing_data.get("owner_reference", "")
        expected_owner_path = f"{settings.firestore_collection_users}/{user_id}"

        if owner_reference != expected_owner_path:
            raise HTTPException(status_code=403, detail="You can only accept offers for your own listings")

        # 3. Find the highest offer of the specified type
        # Handle both singular and plural forms of "point"
        if offer_type.lower() == "point":
            highest_offer_field = "highestOfferPoints"
        else:
            highest_offer_field = f"highestOffer{offer_type.capitalize()}"

        highest_offer = listing_data.get(highest_offer_field, None)

        if not highest_offer:
            raise HTTPException(status_code=404, detail=f"No {offer_type} offers found for this listing")

        # 4. Set the accept time and payment due date
        now = datetime.now()
        payment_due = now + timedelta(days=2)  # Payment due in 2 days

        # 5. Update the offer status in the listing
        highest_offer["status"] = "accepted"
        highest_offer["payment_due"] = payment_due

        # 6. Find the offer in the user's offers collection and update it
        offerer_ref_path = highest_offer.get("offererRef", "")
        offer_reference = highest_offer.get("offerreference", "")

        if not offerer_ref_path or not offer_reference:
            raise HTTPException(status_code=500, detail="Invalid offer data")

        # Get the offer subcollection reference
        offers_subcollection = f"{offer_type}_offers"
        offer_ref = listing_ref.collection(offers_subcollection).document(offer_reference)

        # Get the user's my_offers subcollection reference
        offerer_ref = db_client.document(offerer_ref_path)
        my_offers_subcollection = f"my_{offer_type}_offers"

        # Find the offer in the user's my_offers subcollection
        my_offers_query = offerer_ref.collection(my_offers_subcollection).where("listingId", "==", listing_id)
        my_offers_docs = await my_offers_query.get()

        if not my_offers_docs:
            logger.warning(f"No matching offer found in user's my_{offer_type}_offers collection")

        # 7. Update the listing and the offer in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Update the listing with the accepted offer
            tx.update(listing_ref, {
                highest_offer_field: highest_offer,
                "status": "accepted",
                "payment_due": payment_due
            })

            # Update the offer in the listing's offers subcollection
            tx.update(offer_ref, {
                "status": "accepted",
                "payment_due": payment_due
            })

            # Update the offer in the user's my_offers subcollection if found
            for doc in my_offers_docs:
                tx.update(doc.reference, {
                    "status": "accepted",
                    "payment_due": payment_due
                })

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 8. Get the updated listing
        updated_listing_doc = await listing_ref.get()
        updated_listing_data = updated_listing_doc.to_dict()
        updated_listing_data['id'] = listing_id  # Add the listing ID to the data

        logger.info(f"Successfully accepted {offer_type} offer for listing {listing_id}")
        return CardListing(**updated_listing_data)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error accepting {offer_type} offer for listing {listing_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to accept offer: {str(e)}")

async def offer_cash(
    user_id: str,
    listing_id: str,
    offer_request: OfferCashRequest,
    db_client: AsyncClient,
    expired: int = 7
) -> CardListing:
    """
    Offer cash for a listing.

    This function:
    1. Verifies the listing exists
    2. Verifies the user exists
    3. Creates a new offer document in the "cash_offers" subcollection under the listing
    4. Creates a new offer document in the "my_cash_offers" subcollection under the user
    5. If it's the highest offer, updates the highestOfferCash field in the listing document
    6. Returns the updated listing

    Args:
        user_id: The ID of the user making the offer
        listing_id: The ID of the listing to offer cash for
        offer_request: The OfferCashRequest containing the cash amount to offer
        db_client: Firestore async client
        expired: Number of days until the offer expires (default: 7)

    Returns:
        CardListing: The updated listing

    Raises:
        HTTPException: If there's an error offering cash for the listing
    """
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Check if the user is the owner of the listing
        owner_reference = listing_data.get("owner_reference", "")
        expected_owner_path = f"{settings.firestore_collection_users}/{user_id}"

        if owner_reference == expected_owner_path:
            raise HTTPException(status_code=400, detail="You cannot offer cash for your own listing")

        # 4. Create a new offer document in the "cash_offers" subcollection
        now = datetime.now()
        expires_at = now + timedelta(days=expired)  # Calculate expiration date

        # Get the cash_offers subcollection reference
        cash_offers_ref = listing_ref.collection('cash_offers')
        new_offer_ref = cash_offers_ref.document()  # Auto-generate ID

        offer_data = {
            "offererRef": user_ref.path,  # Reference to the user making the offer
            "amount": offer_request.cash,  # Cash offered
            "at": now,  # Timestamp of the offer
            "offerreference": new_offer_ref.id,  # Reference to this offer
            "type": "cash",  # Indicate this is a cash offer
            "expiresAt": expires_at  # Add expiration date
        }

        # Get the user's my_cash_offers subcollection reference
        my_cash_offers_ref = user_ref.collection('my_cash_offers')
        new_my_offer_ref = my_cash_offers_ref.document()  # Auto-generate ID

        # Create my_offer_data with additional listing information
        my_offer_data = {
            **offer_data,  # Include all offer data
            "listingId": listing_id,  # Reference to the listing
            "card_reference": listing_data.get("card_reference", ""),  # Card reference from the listing
            "collection_id": listing_data.get("collection_id", ""),  # Collection ID from the listing
            "image_url": listing_data.get("image_url", "")  # Image URL from the listing
        }

        # 5. Check if this is the highest offer
        highest_offer_field = "highestOfferCash"
        current_highest_offer = listing_data.get(highest_offer_field, None)
        is_highest_offer = False

        if current_highest_offer is None or offer_request.cash > current_highest_offer.get("amount", 0):
            is_highest_offer = True

        # 6. Update the listing and create the offer in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Create the offer in the listing's cash_offers subcollection
            tx.set(new_offer_ref, offer_data)

            # Create the offer in the user's my_offers subcollection
            tx.set(new_my_offer_ref, my_offer_data)

            # If this is the highest offer, update the listing
            if is_highest_offer:
                tx.update(listing_ref, {
                    "highestOfferCash": offer_data
                })

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 7. Get the updated listing
        updated_listing_doc = await listing_ref.get()
        updated_listing_data = updated_listing_doc.to_dict()

        # 8. Create and return a CardListing object
        listing = CardListing(
            id=listing_id,  # Include the listing ID
            owner_reference=updated_listing_data["owner_reference"],
            card_reference=updated_listing_data["card_reference"],
            collection_id=updated_listing_data["collection_id"],
            quantity=updated_listing_data["quantity"],
            createdAt=updated_listing_data["createdAt"],
            pricePoints=updated_listing_data.get("pricePoints"),
            priceCash=updated_listing_data.get("priceCash"),
            expiresAt=updated_listing_data.get("expiresAt"),
            highestOfferPoints=updated_listing_data.get("highestOfferPoints"),
            highestOfferCash=updated_listing_data.get("highestOfferCash"),
            image_url=updated_listing_data.get("image_url")
        )

        logger.info(f"Successfully created cash offer for listing {listing_id} by user {user_id}")
        return listing

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error offering cash for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to offer cash for listing: {str(e)}")


async def get_all_offers(user_id: str, offer_type: str, db_client: AsyncClient) -> List[Dict[str, Any]]:
    """
    Get all offers for a specific user (regardless of status).

    Args:
        user_id: The ID of the user to get offers for
        offer_type: The type of offer to get (cash or point)
        db_client: Firestore client

    Returns:
        List of all offers for the specified user

    Raises:
        HTTPException: If there's an error getting the offers or if the user doesn't exist
    """
    logger = get_logger(__name__)
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Directly access the user's offers subcollection
        subcollection_name = "my_cash_offers" if offer_type == "cash" else "my_point_offers"
        offers_ref = user_ref.collection(subcollection_name)

        all_offers = []
        async for offer_doc in offers_ref.stream():
            offer_data = offer_doc.to_dict()

            # Get the listing details to include card information
            listing_ref = db_client.collection('listings').document(offer_data.get('listingId', ''))
            listing_doc = await listing_ref.get()

            if listing_doc.exists:
                listing_data = listing_doc.to_dict()

                # Create the offer object with all required fields
                offer_obj = {
                    'amount': offer_data.get('amount', 0),
                    'at': offer_data.get('createdAt', datetime.now()),
                    'card_reference': listing_data.get('card_reference', ''),
                    'collection_id': listing_data.get('collection_id', ''),
                    'expiresAt': offer_data.get('expiresAt', datetime.now() + timedelta(days=7)),
                    'image_url': listing_data.get('image_url', ''),
                    'listingId': offer_data.get('listingId', ''),
                    'offererRef': offer_data.get('offererRef', ''),
                    'offerreference': offer_doc.id,
                    'payment_due': offer_data.get('payment_due', datetime.now() + timedelta(days=3)),
                    'status': offer_data.get('status', ''),
                    'type': offer_data.get('type', 'cash')
                }

                all_offers.append(offer_obj)

        logger.info(f"Retrieved {len(all_offers)} {offer_type} offers for user {user_id}")
        return all_offers
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting {offer_type} offers for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get {offer_type} offers: {str(e)}")

async def get_all_listings(
    db_client: AsyncClient
) -> List[CardListing]:
    """
    Get all listings in the marketplace.

    This function:
    1. Queries the listings collection for all documents
    2. Converts the Firestore documents to CardListing objects
    3. Returns a list of CardListing objects

    Args:
        db_client: Firestore async client

    Returns:
        List[CardListing]: A list of all CardListing objects

    Raises:
        HTTPException: If there's an error getting the listings
    """
    try:
        # 1. Query the listings collection for all documents
        listings_ref = db_client.collection('listings')

        # 2. Execute the query
        listings_docs = await listings_ref.get()

        # 3. Convert the Firestore documents to CardListing objects
        listings = []
        for doc in listings_docs:
            listing_data = doc.to_dict()
            listing_data['id'] = doc.id  # Add the document ID to the data

            # Generate signed URL for the card image if it exists
            if 'image_url' in listing_data and listing_data['image_url']:
                try:
                    listing_data['image_url'] = await generate_signed_url(listing_data['image_url'])
                except Exception as sign_error:
                    logger.error(f"Failed to generate signed URL for {listing_data['image_url']}: {sign_error}")
                    # Keep the original URL if signing fails

            # Create a CardListing object
            listing = CardListing(
                id=listing_data["id"],  # Include the listing ID
                owner_reference=listing_data["owner_reference"],
                card_reference=listing_data["card_reference"],
                collection_id=listing_data.get("collection_id", ""),
                quantity=listing_data["quantity"],
                createdAt=listing_data["createdAt"],
                pricePoints=listing_data.get("pricePoints"),
                priceCash=listing_data.get("priceCash"),
                expiresAt=listing_data.get("expiresAt"),
                highestOfferPoints=listing_data.get("highestOfferPoints"),
                highestOfferCash=listing_data.get("highestOfferCash"),
                image_url=listing_data.get("image_url")
            )
            listings.append(listing)

        logger.info(f"Successfully retrieved {len(listings)} listings")
        return listings

    except Exception as e:
        logger.error(f"Error getting all listings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get listings: {str(e)}")

async def get_accepted_offers(user_id: str, offer_type: str, db_client: AsyncClient) -> List[Dict[str, Any]]:
    """
    Get all accepted offers for a specific user.

    Args:
        user_id: The ID of the user to get accepted offers for
        offer_type: The type of offer to get (cash or point)
        db_client: Firestore client

    Returns:
        List of accepted offers for the specified user

    Raises:
        HTTPException: If there's an error getting the accepted offers or if the user doesn't exist
    """
    logger = get_logger(__name__)
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Directly access the user's offers subcollection
        subcollection_name = "my_cash_offers" if offer_type == "cash" else "my_point_offers"
        offers_ref = user_ref.collection(subcollection_name)

        accepted_offers = []
        async for offer_doc in offers_ref.stream():
            offer_data = offer_doc.to_dict()

            # Check if this offer has 'accepted' status
            if offer_data.get('status') == 'accepted':
                # Get the listing details to include card information
                listing_ref = db_client.collection('listings').document(offer_data.get('listingId', ''))
                listing_doc = await listing_ref.get()

                if listing_doc.exists:
                    listing_data = listing_doc.to_dict()

                    # Create the accepted offer object with all required fields
                    accepted_offer = {
                        'amount': offer_data.get('amount', 0),
                        'at': offer_data.get('createdAt', datetime.now()),
                        'card_reference': listing_data.get('card_reference', ''),
                        'collection_id': listing_data.get('collection_id', ''),
                        'expiresAt': offer_data.get('expiresAt', datetime.now() + timedelta(days=7)),
                        'image_url': listing_data.get('image_url', ''),
                        'listingId': offer_data.get('listingId', ''),
                        'offererRef': offer_data.get('offererRef', ''),
                        'offerreference': offer_doc.id,
                        'payment_due': offer_data.get('payment_due', datetime.now() + timedelta(days=3)),
                        'status': offer_data.get('status', 'accepted'),
                        'type': offer_data.get('type', 'cash')
                    }

                    accepted_offers.append(accepted_offer)

        logger.info(f"Retrieved {len(accepted_offers)} accepted {offer_type} offers for user {user_id}")
        return accepted_offers
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting accepted {offer_type} offers for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get accepted {offer_type} offers: {str(e)}")


async def pay_point_offer(
    user_id: str,
    listing_id: str,
    offer_id: str,
    db_client: AsyncClient
) -> Dict[str, Any]:
    """
    Pay for a point offer, which will:
    1. Deduct points from the user's account
    2. Add points to the seller's account
    3. Add the card to the user's collection
    4. Deduct quantity from the listing
    5. Delete the listing if quantity becomes zero
    6. Deduct locked_quantity from the seller's card
    7. Delete the seller's card if both quantity and locked_quantity are zero
    8. Insert data into the marketplace_transactions Firestore collection
    9. Insert data into the marketplace_transactions SQL table
    10. Delete the user's offer from their my_point_offers collection

    Args:
        user_id: The ID of the user paying for the offer (must be the offer creator)
        listing_id: The ID of the listing
        offer_id: The ID of the offer to pay
        db_client: Firestore async client

    Returns:
        Dictionary with success message and details

    Raises:
        HTTPException: If there's an error paying for the offer
    """
    logger = get_logger(__name__)
    try:
        # 1. Verify user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()

        # 2. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()
        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 3. Verify the offer exists
        point_offers_ref = listing_ref.collection('point_offers')
        offer_ref = point_offers_ref.document(offer_id)
        offer_doc = await offer_ref.get()

        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()

        # 4. Verify the user is the offer creator
        offerer_ref_path = offer_data.get("offererRef", "")
        expected_offerer_path = f"{settings.firestore_collection_users}/{user_id}"

        if offerer_ref_path != expected_offerer_path:
            raise HTTPException(status_code=403, detail="You can only pay for your own offers")

        # 5. Get the seller information
        seller_ref_path = listing_data.get("owner_reference", "")
        if not seller_ref_path:
            raise HTTPException(status_code=500, detail="Invalid listing data: missing owner reference")

        seller_id = seller_ref_path.split('/')[-1]

        # 6. Verify the user has enough points
        points_to_pay = offer_data.get("amount", 0)
        user_points = user_data.get("pointsBalance", 0)

        if user_points < points_to_pay:
            raise HTTPException(status_code=400, detail=f"Insufficient points. You have {user_points} points, but {points_to_pay} are required.")

        # 7. Get card information
        card_reference = listing_data.get("card_reference", "")
        collection_id = listing_data.get("collection_id", "")

        if not card_reference or not collection_id:
            raise HTTPException(status_code=500, detail="Invalid listing data: missing card reference or collection ID")

        # 8. Get the quantity to deduct from the listing
        quantity_to_deduct = 1  # Default to 1

        # 9. Find the user's offer in their my_point_offers subcollection
        my_point_offers_ref = user_ref.collection('my_point_offers')
        my_point_offers_query = my_point_offers_ref.where("listingId", "==", listing_id)
        my_point_offers_docs = await my_point_offers_query.get()

        my_offer_ref = None
        for doc in my_point_offers_docs:
            my_offer_data = doc.to_dict()
            # Check if this is the same offer by comparing offerreference
            if my_offer_data.get("offerreference") == offer_id:
                my_offer_ref = doc.reference
                break

        if not my_offer_ref:
            logger.warning(f"Could not find corresponding my_offer for offer {offer_id} in user {user_id}'s my_point_offers collection")

        # 10. Create a transaction ID
        transaction_id = f"tx_{listing_id}_{offer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # 11. Execute the transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # a. Deduct points from the user
            tx.update(user_ref, {
                "pointsBalance": firestore.Increment(-points_to_pay)
            })

            # Add points to the seller (list owner)
            seller_ref = db_client.collection(settings.firestore_collection_users).document(seller_id)
            tx.update(seller_ref, {
                "pointsBalance": firestore.Increment(points_to_pay)
            })

            # b. Update the listing quantity
            current_quantity = listing_data.get("quantity", 0)
            new_quantity = current_quantity - quantity_to_deduct

            if new_quantity <= 0:
                # Delete the listing if quantity becomes zero
                tx.delete(listing_ref)
            else:
                # Update the listing quantity
                tx.update(listing_ref, {
                    "quantity": new_quantity
                })

            # c. Deduct locked_quantity from the seller's card
            try:
                # Parse card_reference to get collection_id and card_id
                card_id = card_reference.split('/')[-1]

                # Get reference to the seller's card
                seller_card_ref = seller_ref.collection('cards').document('cards').collection(collection_id).document(card_id)

                # Get the seller's card to check current values
                seller_card_doc = await seller_card_ref.get()

                if seller_card_doc.exists:
                    seller_card_data = seller_card_doc.to_dict()
                    current_locked_quantity = seller_card_data.get('locked_quantity', 0)
                    current_card_quantity = seller_card_data.get('quantity', 0)

                    # Ensure we don't go below zero for locked_quantity
                    new_locked_quantity = max(0, current_locked_quantity - quantity_to_deduct)

                    # Check if both quantity and locked_quantity will be zero
                    if current_card_quantity == 0 and new_locked_quantity == 0:
                        # Delete the card from the seller's collection
                        tx.delete(seller_card_ref)
                        logger.info(f"Deleted card {card_id} from seller {seller_id}'s collection as both quantity and locked_quantity are zero")
                    else:
                        # Update the card with decremented locked_quantity
                        tx.update(seller_card_ref, {
                            'locked_quantity': new_locked_quantity
                        })
                        logger.info(f"Updated locked_quantity for card {card_id} in seller {seller_id}'s collection to {new_locked_quantity}")
            except Exception as e:
                logger.error(f"Error updating seller's card: {e}", exc_info=True)
                # Continue with the transaction even if updating the seller's card fails
                # This ensures the main transaction still completes

            # d. Create a marketplace transaction record
            transaction_ref = db_client.collection('marketplace_transactions').document(transaction_id)
            transaction_data = {
                "id": transaction_id,
                "listing_id": listing_id,
                "seller_id": seller_id,
                "buyer_id": user_id,
                "card_id": card_reference.split('/')[-1],
                "quantity": quantity_to_deduct,
                "price_points": points_to_pay,
                "price_card_id": None,
                "price_card_qty": None,
                "traded_at": datetime.now()
            }
            tx.set(transaction_ref, transaction_data)

            # d. Update the offer status
            tx.update(offer_ref, {
                "status": "paid",
                "paid_at": datetime.now()
            })

            # e. Delete the user's offer from their my_point_offers collection
            if my_offer_ref:
                tx.delete(my_offer_ref)

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 12. Insert data into the marketplace_transactions SQL table
        # Use a single database connection for the SQL operation to ensure transaction integrity
        with db_connection() as conn:
            cursor = conn.cursor()
            try:
                # Begin transaction
                conn.autocommit = False

                # Record the transaction in marketplace_transactions table
                cursor.execute(
                    """
                    INSERT INTO marketplace_transactions (listing_id, seller_id, buyer_id, card_id, quantity, price_points, price_card_id, price_card_qty, traded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (listing_id, seller_id, user_id, card_reference.split('/')[-1], quantity_to_deduct, points_to_pay, None, None, datetime.now())
                )
                sql_transaction_id = cursor.fetchone()[0]
                logger.info(f"Created marketplace transaction record with ID {sql_transaction_id}")

                # Commit the transaction
                conn.commit()
                logger.info(f"Successfully committed SQL database transaction for marketplace transaction {transaction_id}")
                logger.info(f"Recorded marketplace transaction: listing {listing_id}, seller {seller_id}, buyer {user_id}, points {points_to_pay}")

            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"SQL database transaction failed, rolling back: {str(e)}", exc_info=True)
                # Continue with the response - we've already completed the Firestore transaction,
                # so we don't want to fail the whole operation just because of a database issue
                logger.warning("SQL database transaction failed but Firestore transaction was successful")

            finally:
                # Close cursor (connection will be closed by context manager)
                cursor.close()

        # 13. Add the card to the user's collection
        try:
            await add_card_to_user(
                user_id=user_id,
                card_reference=card_reference,
                db_client=db_client,
                collection_metadata_id=collection_id,
                from_marketplace=True
            )
        except Exception as e:
            logger.error(f"Error adding card to user {user_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Transaction completed but failed to add card to user: {str(e)}")

        logger.info(f"Successfully paid for point offer {offer_id} for listing {listing_id} by user {user_id}")
        return {
            "message": f"Successfully paid for point offer",
            "transaction_id": transaction_id,
            "listing_id": listing_id,
            "offer_id": offer_id,
            "points_paid": points_to_pay,
            "remaining_points": user_points - points_to_pay
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error paying for point offer {offer_id} for listing {listing_id} by user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to pay for point offer: {str(e)}")
