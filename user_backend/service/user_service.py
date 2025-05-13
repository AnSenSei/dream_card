from typing import Optional, Dict, List, Tuple
from uuid import UUID
import random
import math
from datetime import datetime, timedelta

from fastapi import HTTPException
import httpx
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient

from config import get_logger, settings
from models.schemas import User, UserCard, PaginationInfo, AppliedFilters, UserCardListResponse, UserCardsResponse
from utils.gcs_utils import generate_signed_url

logger = get_logger(__name__)

async def get_collection_metadata_from_service(collection_name: str) -> Dict:
    """
    Fetches collection metadata from the storage service via HTTP.

    Args:
        collection_name: The name of the collection to fetch metadata for

    Returns:
        The collection metadata as a dictionary

    Raises:
        HTTPException: If there's an error fetching the collection metadata
    """
    url = f"{settings.storage_service_url}/gacha/api/v1/storage/collection-metadata/{collection_name}"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)

        if response.status_code != 200:
            error_detail = response.json().get("detail", "Unknown error")
            logger.error(f"Error fetching metadata for collection {collection_name} from storage service: {error_detail}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch collection metadata: {error_detail}"
            )

        return response.json()
    except httpx.RequestError as e:
        logger.error(f"Request error fetching metadata for collection {collection_name}: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Could not connect to storage service"
        )

async def get_card_by_id_from_service(card_id: str, collection_name: str = None) -> dict:
    """
    Fetches card data from the storage service via HTTP.

    Args:
        card_id: The ID of the card to fetch
        collection_name: The collection where the card is stored (used as collection_metadata_id)

    Returns:
        The card data as a dictionary

    Raises:
        HTTPException: If there's an error fetching the card
    """
    url = f"{settings.storage_service_url}/gacha/api/v1/storage/cards/{card_id}"
    params = {}
    if collection_name:
        params["collection_metadata_id"] = collection_name

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=10.0)

        if response.status_code != 200:
            error_detail = response.json().get("detail", "Unknown error")
            logger.error(f"Error fetching card {card_id} from storage service: {error_detail}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to fetch card details: {error_detail}"
            )

        return response.json()
    except httpx.RequestError as e:
        logger.error(f"Request error fetching card {card_id}: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Could not connect to storage service"
        )

async def get_user_by_id(user_id: str, db_client: AsyncClient) -> Optional[User]:
    """
    Get a user by ID from Firestore.

    Args:
        user_id: The ID of the user to get
        db_client: Firestore client

    Returns:
        The user if found, None otherwise

    Raises:
        HTTPException: If there's an error getting the user
    """
    try:
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            return None

        user_data = user_doc.to_dict()
        return User(**user_data)
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get user: {str(e)}")

async def draw_card_from_pack(collection_id: str, pack_id: str, db_client: AsyncClient) -> dict:
    """
    Draw a card from a pack based on probabilities.

    This function:
    1. Gets all probabilities from cards.values() in the pack
    2. Randomly chooses a card id based on these probabilities
    3. Retrieves the card information from the cards subcollection
    4. Logs the card information and returns a success message

    Args:
        collection_id: The ID of the collection containing the pack
        pack_id: The ID of the pack to draw from
        db_client: Firestore client

    Returns:
        A dictionary with a success message

    Raises:
        HTTPException: If there's an error drawing the card
    """
    try:
        logger.info(f"Drawing card from pack '{pack_id}' in collection '{collection_id}'")

        # Construct the reference to the pack document
        pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {pack_id} in collection {collection_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found in collection '{collection_id}'")

        # Get the cards map from the pack document
        pack_data = pack_snap.to_dict()
        cards_map = pack_data.get('cards', {})

        if not cards_map:
            logger.error(f"No cards found in pack '{pack_id}' in collection '{collection_id}'")
            raise HTTPException(status_code=404, detail=f"No cards found in pack '{pack_id}' in collection '{collection_id}'")

        # Get all probabilities from the cards map
        card_ids = list(cards_map.keys())
        probabilities = list(cards_map.values())

        # Randomly choose a card id based on probabilities
        chosen_card_id = random.choices(card_ids, weights=probabilities, k=1)[0]
        logger.info(f"Randomly selected card '{chosen_card_id}' from pack '{pack_id}' in collection '{collection_id}'")

        # Get the card information from the cards subcollection
        card_ref = pack_ref.collection('cards').document(chosen_card_id)
        card_snap = await card_ref.get()

        if not card_snap.exists:
            logger.error(f"Card '{chosen_card_id}' not found in pack '{pack_id}' in collection '{collection_id}'")
            raise HTTPException(status_code=404, detail=f"Card '{chosen_card_id}' not found in pack '{pack_id}' in collection '{collection_id}'")

        # Get the card data
        card_data = card_snap.to_dict()

        # Log the card information
        logger.info(f"Card drawn from pack '{pack_id}' in collection '{collection_id}':")
        logger.info(f"  Card ID: {chosen_card_id}")
        logger.info(f"  Card Name: {card_data.get('name', '')}")
        logger.info(f"  Card Reference: {str(card_data.get('globalRef').path) if 'globalRef' in card_data else ''}")
        logger.info(f"  Image URL: {card_data.get('image_url', '')}")
        logger.info(f"  Point Worth: {card_data.get('point', 0)}")
        logger.info(f"  Quantity: {card_data.get('quantity', 0)}")
        logger.info(f"  Rarity: {card_data.get('rarity', 1)}")

        # Generate a signed URL for the card image if it's a GCS URI
        image_url = card_data.get('image_url', '')
        signed_url = image_url
        if image_url and image_url.startswith('gs://'):
            signed_url = await generate_signed_url(image_url)
            logger.info(f"  Generated signed URL for image: {signed_url}")

        # Return a dictionary with the signed URL and point_worth
        return {
            "message": f"Successfully drew card '{chosen_card_id}' from pack '{pack_id}' in collection '{collection_id}'",
            "image_url": signed_url,
            "point_worth": card_data.get('point', 0)
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error drawing card from pack '{pack_id}' in collection '{collection_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to draw card from pack '{pack_id}' in collection '{collection_id}': {str(e)}")

async def get_user_cards(
    user_id: str, 
    db_client: AsyncClient, 
    page: int = 1, 
    per_page: int = 10, 
    sort_by: str = "date_got", 
    sort_order: str = "desc", 
    search_query: str | None = None,
    subcollection_name: str | None = None
) -> UserCardsResponse:
    """
    Get all cards for a user, separated by subcollection with pagination.

    Args:
        user_id: The ID of the user to get cards for
        db_client: Firestore client
        page: The page number to get (default: 1)
        per_page: The number of items per page (default: 10)
        sort_by: The field to sort by (default: "date_got")
        sort_order: The sort order ("asc" or "desc", default: "desc")
        search_query: Optional search query to filter cards by name
        subcollection_name: Optional subcollection name to filter by

    Returns:
        A UserCardsResponse with a list of UserCardListResponse objects, one for each subcollection

    Raises:
        HTTPException: If there's an error getting the cards
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the 'cards' document in the user's 'cards' collection
        cards_doc_ref = user_ref.collection('cards').document('cards')
        cards_doc = await cards_doc_ref.get()

        # Check if there are any cards directly under the 'cards' collection
        cards_collection_ref = user_ref.collection('cards')
        cards_query = cards_collection_ref.limit(1)
        cards_docs = await cards_query.get()
        has_direct_cards = len(cards_docs) > 0

        # Check if there are any subcollections under the 'cards' document even if it doesn't exist
        # This is a special case where the document might not exist but subcollections do
        collections = []
        async for collection in cards_doc_ref.collections():
            collections.append(collection)
        has_subcollections = len(collections) > 0

        logger.info(f"Checking cards for user {user_id}: 'cards' document exists: {cards_doc.exists}, direct cards exist: {has_direct_cards}, subcollections exist: {has_subcollections}")

        if not cards_doc.exists and not has_direct_cards and not has_subcollections:
            # If the 'cards' document doesn't exist, there are no direct cards, and no subcollections, the user has no cards
            logger.info(f"User {user_id} has no cards")
            return UserCardsResponse(subcollections=[])

        # Get all subcollections under the 'cards' document
        subcollections = []

        # Get subcollections regardless of whether the 'cards' document exists
        # We already checked for subcollections earlier
        all_subcollections = [collection.id for collection in collections]

        if subcollection_name:
            # If a specific subcollection is requested, check if it exists
            logger.info(f"Available subcollections for user {user_id}: {all_subcollections}")

            if subcollection_name in all_subcollections:
                # If the requested subcollection exists, use only that one
                logger.info(f"Subcollection {subcollection_name} found for user {user_id}")
                subcollections = [subcollection_name]
            else:
                # If the requested subcollection doesn't exist, return empty response
                logger.warning(f"Subcollection {subcollection_name} not found for user {user_id}. Available subcollections: {all_subcollections}")
                return UserCardsResponse(subcollections=[])
        else:
            # Otherwise, use all subcollections
            subcollections = all_subcollections

        if not cards_doc.exists and has_subcollections:
            logger.info(f"'cards' document doesn't exist for user {user_id}, but subcollections exist: {subcollections}")

        if not subcollections:
            logger.info(f"User {user_id} has no card subcollections")
            return UserCardsResponse(subcollections=[])

        # For each subcollection, get the cards with pagination
        subcollection_responses = []
        for subcoll_name in subcollections:
            # Get cards from this subcollection with pagination
            subcoll_response = await get_user_subcollection_cards(
                user_id=user_id,
                subcollection_name=subcoll_name,
                db_client=db_client,
                page=page,
                per_page=per_page,
                sort_by=sort_by,
                sort_order=sort_order,
                search_query=search_query
            )
            subcollection_responses.append(subcoll_response)

        return UserCardsResponse(subcollections=subcollection_responses)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting cards for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get cards for user: {str(e)}")

async def get_user_subcollection_cards(
    user_id: str,
    subcollection_name: str,
    db_client: AsyncClient,
    page: int = 1,
    per_page: int = 10,
    sort_by: str = "date_got",
    sort_order: str = "desc",
    search_query: str | None = None
) -> UserCardListResponse:
    """
    Get cards for a user from a specific subcollection with pagination.

    Args:
        user_id: The ID of the user to get cards for
        subcollection_name: The name of the subcollection to get cards from
        db_client: Firestore client
        page: The page number to get (default: 1)
        per_page: The number of items per page (default: 10)
        sort_by: The field to sort by (default: "date_got")
        sort_order: The sort order ("asc" or "desc", default: "desc")
        search_query: Optional search query to filter cards by name

    Returns:
        A UserCardListResponse with the cards, pagination info, and applied filters

    Raises:
        HTTPException: If there's an error getting the cards
    """
    try:
        # Get the subcollection reference
        subcoll_ref = db_client.collection(settings.firestore_collection_users).document(user_id) \
            .collection('cards').document('cards').collection(subcollection_name)

        # Create the base query
        query = subcoll_ref

        # Apply search query if provided
        if search_query and search_query.strip():
            stripped_search_query = search_query.strip()
            logger.info(f"Applying search filter for card_name: >='{stripped_search_query}' and <='{stripped_search_query}\uf8ff'")
            query = query.where("card_name", ">=", stripped_search_query)
            query = query.where("card_name", "<=", stripped_search_query + "\uf8ff")

        # Count total items matching the query
        count_agg_query = query.count()
        count_snapshot = await count_agg_query.get()
        total_items = count_snapshot[0][0].value if count_snapshot and count_snapshot[0] else 0

        if total_items == 0:
            logger.info(f"No cards found for user {user_id} in subcollection {subcollection_name}")
            return UserCardListResponse(
                subcollection_name=subcollection_name,
                cards=[],
                pagination=PaginationInfo(
                    total_items=0,
                    items_per_page=per_page,
                    current_page=page,
                    total_pages=0
                ),
                filters=AppliedFilters(sort_by=sort_by, sort_order=sort_order, search_query=search_query)
            )

        # Determine sort direction
        if sort_order.lower() == "desc":
            direction = firestore.Query.DESCENDING
        elif sort_order.lower() == "asc":
            direction = firestore.Query.ASCENDING
        else:
            logger.warning(f"Invalid sort_order '{sort_order}'. Defaulting to DESCENDING.")
            direction = firestore.Query.DESCENDING
            sort_order = "desc"  # Ensure applied filter reflects actual sort

        # Apply sorting
        query_with_filters = query  # query already has search filters if any

        if search_query and search_query.strip() and sort_by != "card_name":
            # If searching and sorting by a different field, ensure card_name is the first sort key
            logger.warning(f"Search query on 'card_name' is active while sorting by '{sort_by}'. Firestore requires ordering by 'card_name' first.")
            query_with_sort = query_with_filters.order_by("card_name").order_by(sort_by, direction=direction)
        else:
            query_with_sort = query_with_filters.order_by(sort_by, direction=direction)

        # Apply pagination
        current_page_query = max(1, page)
        per_page_query = max(1, per_page)
        offset = (current_page_query - 1) * per_page_query

        paginated_query = query_with_sort.limit(per_page_query).offset(offset)

        # Execute the query
        logger.info(f"Executing Firestore query for user {user_id}, subcollection {subcollection_name} with pagination and sorting")
        stream = paginated_query.stream()

        # Process the results
        cards_list = []
        async for doc in stream:
            try:
                card_data = doc.to_dict()
                if not card_data:  # Skip empty documents
                    logger.warning(f"Skipping empty document with ID: {doc.id} in subcollection {subcollection_name}")
                    continue

                # Ensure ID is part of the data
                if 'id' not in card_data:
                    card_data['id'] = doc.id

                # Generate signed URL for the card image
                if 'image_url' in card_data and card_data['image_url']:
                    try:
                        card_data['image_url'] = await generate_signed_url(card_data['image_url'])
                        logger.debug(f"Generated signed URL for image: {card_data['image_url']}")
                    except Exception as sign_error:
                        logger.error(f"Failed to generate signed URL for {card_data['image_url']}: {sign_error}")
                        # Keep the original URL if signing fails

                cards_list.append(UserCard(**card_data))
            except Exception as e:
                logger.error(f"Error processing document {doc.id} from subcollection {subcollection_name}: {e}", exc_info=True)
                # Skip this card and continue
                continue

        # Calculate total pages
        total_pages = math.ceil(total_items / per_page_query) if per_page_query > 0 else 0

        # Create pagination info
        pagination_info = PaginationInfo(
            total_items=total_items,
            items_per_page=per_page_query,
            current_page=current_page_query,
            total_pages=total_pages
        )

        # Create applied filters info
        applied_filters_info = AppliedFilters(
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )

        logger.info(f"Successfully fetched {len(cards_list)} cards for user {user_id} from subcollection {subcollection_name}")
        return UserCardListResponse(
            subcollection_name=subcollection_name,
            cards=cards_list,
            pagination=pagination_info,
            filters=applied_filters_info
        )
    except Exception as e:
        logger.error(f"Error getting cards for user {user_id} from subcollection {subcollection_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get cards from subcollection {subcollection_name}: {str(e)}")

async def add_card_to_user(user_id: str, card_reference: str, db_client: AsyncClient, collection_metadata_id: str = None) -> UserCard:
    """
    Add a card to a user's cards subcollection.

    Args:
        user_id: The ID of the user to add the card to
        card_reference: The reference to the card to add (e.g., 'GlobalCards/checkout')
        db_client: Firestore client
        collection_metadata_id: The ID of the collection metadata to use for the subcollection

    Returns:
        The added card

    Raises:
        HTTPException: If there's an error adding the card
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Parse the card reference to get the collection and card ID
        try:
            parts = card_reference.split('/')
            if len(parts) != 2:
                raise ValueError(f"Invalid card reference format: {card_reference}. Expected format: 'collection/card_id'")

            collection_id, card_id = parts
            logger.info(f"Parsed card reference '{card_reference}' into collection_id='{collection_id}' and card_id='{card_id}'")
        except Exception as e:
            logger.error(f"Failed to parse card reference '{card_reference}': {str(e)}")
            raise HTTPException(status_code=400, detail=f"Invalid card reference format: {card_reference}. Expected format: 'collection/card_id'")

        # Get the card document directly from Firestore
        try:
            card_ref = db_client.collection(collection_id).document(card_id)
            card_doc = await card_ref.get()

            if not card_doc.exists:
                logger.error(f"Card not found: {card_reference}")
                raise HTTPException(status_code=404, detail=f"Card '{card_reference}' not found")

            card_data = card_doc.to_dict()
            logger.info(f"Retrieved card data for '{card_reference}': {card_data}")
        except HTTPException as e:
            raise e
        except Exception as e:
            logger.error(f"Error retrieving card '{card_reference}' from Firestore: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve card '{card_reference}' from Firestore: {str(e)}")

        # Prepare card data for user's collection
        user_card_data = {
            "card_reference": card_reference,
            "card_name": card_data.get("card_name", ""),
            "date_got": firestore.SERVER_TIMESTAMP,
            "id": card_id,
            "image_url": card_data.get("image_url", ""),
            "point_worth": card_data.get("point_worth", 0),
            "quantity": 1,  # Initial quantity is 1
            "rarity": card_data.get("rarity", 1)
        }

        # Add expireAt and buybackexpiresAt fields if point_worth is less than 1000
        point_worth = card_data.get("point_worth", 0)
        if point_worth < 1000:
            logger.info(f"Card '{card_reference}' has point_worth {point_worth} < 1000, adding expiration fields")
            # Calculate expiration dates based on settings
            now = datetime.now()
            expire_at = now + timedelta(days=settings.card_expire_days)
            buyback_expires_at = now + timedelta(days=settings.card_buyback_expire_days)

            logger.info(f"Setting expireAt to {expire_at} and buybackexpiresAt to {buyback_expires_at}")
            user_card_data["expireAt"] = expire_at
            user_card_data["buybackexpiresAt"] = buyback_expires_at

        # Add card to user's cards subcollection
        cards_ref = user_ref.collection('cards').document(card_id)

        # Check if card already exists in user's collection
        card_doc = await cards_ref.get()
        if card_doc.exists:
            # If card already exists, increment quantity
            await cards_ref.update({"quantity": firestore.Increment(1)})
            # Get the updated card data
            updated_card_doc = await cards_ref.get()
            updated_card_data_for_subcollection = updated_card_doc.to_dict()
            # Use the updated data for the subcollection
            user_card_data = updated_card_data_for_subcollection
        else:
            # If card doesn't exist, create it
            await cards_ref.set(user_card_data)

        # Create a document called 'cards' under the user's cards collection
        cards_doc_ref = user_ref.collection('cards').document('cards')

        # Create a subcollection under the 'cards' document with the collection name
        subcollection_name = collection_metadata_id if collection_metadata_id else collection_id
        subcollection_ref = cards_doc_ref.collection(subcollection_name).document(card_id)
        await subcollection_ref.set(user_card_data)

        logger.info(f"Added card '{card_id}' to user '{user_id}' with path 'cards/{subcollection_name}/{card_id}'")

        # Get the added/updated card
        updated_card_doc = await cards_ref.get()
        updated_card_data = updated_card_doc.to_dict()

        # Generate signed URL for the image if it's a GCS URI
        if 'image_url' in updated_card_data and updated_card_data['image_url'] and updated_card_data['image_url'].startswith('gs://'):
            try:
                updated_card_data['image_url'] = await generate_signed_url(updated_card_data['image_url'])
                logger.debug(f"Generated signed URL for image: {updated_card_data['image_url']}")
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {updated_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Convert to UserCard model
        user_card = UserCard(
            card_reference=updated_card_data["card_reference"],
            card_name=updated_card_data["card_name"],
            date_got=updated_card_data["date_got"],
            id=updated_card_data["id"],
            image_url=updated_card_data["image_url"],
            point_worth=updated_card_data["point_worth"],
            quantity=updated_card_data["quantity"],
            rarity=updated_card_data["rarity"]
        )

        # Add expireAt and buybackexpiresAt if they exist in the data
        if "expireAt" in updated_card_data:
            user_card.expireAt = updated_card_data["expireAt"]
        if "buybackexpiresAt" in updated_card_data:
            user_card.buybackexpiresAt = updated_card_data["buybackexpiresAt"]

        return user_card
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding card to user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add card to user: {str(e)}")

async def destroy_card(user_id: str, card_id: str, subcollection_name: str, db_client: AsyncClient, quantity: int = 1) -> Tuple[User, UserCard]:
    """
    Destroy a card from a user's collection and add its point_worth to the user's pointsBalance.
    If quantity is less than the card's quantity, only reduce the quantity.
    Only remove the card if the remaining quantity is 0.

    Args:
        user_id: The ID of the user who owns the card
        card_id: The ID of the card to destroy
        subcollection_name: The name of the subcollection where the card is stored
        db_client: Firestore client
        quantity: The quantity to destroy (default: 1)

    Returns:
        A tuple containing the updated user and the destroyed card

    Raises:
        HTTPException: If there's an error destroying the card
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()
        user = User(**user_data)

        # Get the card from the subcollection
        card_ref = user_ref.collection('cards').document('cards').collection(subcollection_name).document(card_id)
        card_doc = await card_ref.get()

        if not card_doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in subcollection {subcollection_name}")

        card_data = card_doc.to_dict()
        card = UserCard(**card_data)

        # Get the card's quantity and point_worth
        card_quantity = card.quantity
        point_worth_per_card = card.point_worth

        # Ensure quantity is valid
        if quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

        if quantity > card_quantity:
            raise HTTPException(status_code=400, detail=f"Cannot destroy {quantity} cards, only {card_quantity} available")

        # Calculate points to add based on quantity being destroyed
        points_to_add = point_worth_per_card * quantity

        # Calculate remaining quantity
        remaining_quantity = card_quantity - quantity

        # Start a transaction to update the user's pointsBalance and update or delete the card
        transaction = db_client.transaction()

        @firestore.async_transactional
        async def update_user_and_card(transaction, user_ref, card_ref, points_to_add, remaining_quantity):
            # Update the user's pointsBalance
            transaction.update(user_ref, {"pointsBalance": firestore.Increment(points_to_add)})

            # Get the main card reference
            main_card_ref = user_ref.collection('cards').document(card_id)
            main_card_doc = await main_card_ref.get()

            if remaining_quantity <= 0:
                # Delete the card from the subcollection if quantity is 0
                transaction.delete(card_ref)

                # Also delete from the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.delete(main_card_ref)

                logger.info(f"Deleted card {card_id} from user {user_id}'s subcollection {subcollection_name}")
            else:
                # Update the quantity in the subcollection
                transaction.update(card_ref, {"quantity": remaining_quantity})

                # Also update in the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.update(main_card_ref, {"quantity": remaining_quantity})

                logger.info(f"Updated card {card_id} quantity to {remaining_quantity} in user {user_id}'s subcollection {subcollection_name}")

        # Execute the transaction
        await update_user_and_card(transaction, user_ref, card_ref, points_to_add, remaining_quantity)

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        # Update the card object to reflect the new quantity or mark it as destroyed
        if remaining_quantity <= 0:
            # For complete destruction, we keep the original card object for the response
            # but it's already deleted from the database
            logger.info(f"Completely destroyed card {card_id} from user {user_id}'s subcollection {subcollection_name} and added {points_to_add} points to balance")
        else:
            # For partial destruction, update the card object's quantity for the response
            card.quantity = remaining_quantity
            logger.info(f"Partially destroyed {quantity} of card {card_id} from user {user_id}'s subcollection {subcollection_name} and added {points_to_add} points to balance")

        return updated_user, card
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error destroying card {card_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to destroy card: {str(e)}")
