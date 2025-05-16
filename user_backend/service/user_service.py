from typing import Optional, Dict, List, Tuple
from uuid import UUID
import random
import math
import re
from datetime import datetime, timedelta

from fastapi import HTTPException
import httpx
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, SERVER_TIMESTAMP, async_transactional, Increment

from config import get_logger, settings
from models.schemas import User, UserCard, PaginationInfo, AppliedFilters, UserCardListResponse, UserCardsResponse, Address, CreateAccountRequest, PerformFusionResponse, RandomFusionRequest, CardListing, CreateCardListingRequest, OfferPointsRequest, OfferCashRequest, UpdatePointOfferRequest, UpdateCashOfferRequest
from utils.gcs_utils import generate_signed_url, upload_avatar_to_gcs

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
    5. Increments the popularity field of the pack by 1

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

        # Increment the popularity field of the pack by 1
        try:
            await pack_ref.update({"popularity": Increment(1)})
            logger.info(f"Incremented popularity for pack '{pack_id}' in collection '{collection_id}'")
        except Exception as e:
            logger.error(f"Failed to increment popularity for pack '{pack_id}' in collection '{collection_id}': {e}")
            # Continue even if updating popularity fails

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

async def draw_multiple_cards_from_pack(collection_id: str, pack_id: str, user_id: str, db_client: AsyncClient, count: int = 5) -> list:
    """
    Draw multiple cards (1,5 or 10) from a pack based on probabilities.

    This function:
    1. Gets all probabilities from cards.values() in the pack
    2. Randomly chooses multiple card ids based on these probabilities
    3. Retrieves the card information from the cards subcollection for each card
    4. Logs the card information and returns the list of drawn cards
    5. Increments the popularity field of the pack by the number of cards drawn

    Args:
        collection_id: The ID of the collection containing the pack
        pack_id: The ID of the pack to draw from
        user_id: The ID of the user (used only for validation)
        db_client: Firestore client
        count: The number of cards to draw (default: 1, should be 1, 5 or 10)

    Returns:
        A list of dictionaries containing the drawn card data

    Raises:
        HTTPException: If there's an error drawing the cards
    """
    try:
        # Validate count parameter
        if count not in [1,5, 10]:
            logger.error(f"Invalid count parameter: {count}. Must be 1,5 or 10.")
            raise HTTPException(status_code=400, detail=f"Invalid count parameter: {count}. Must be 5 or 10.")

        logger.info(f"Drawing {count} cards from pack '{pack_id}' in collection '{collection_id}' for user '{user_id}'")

        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

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

        # Check if there are enough cards in the pack
        if len(card_ids) < count:
            logger.warning(f"Not enough cards in pack '{pack_id}' in collection '{collection_id}'. Requested {count} but only {len(card_ids)} available.")
            # We'll still draw as many as possible, but with replacement

        # Randomly choose multiple card ids based on probabilities (with replacement)
        chosen_card_ids = random.choices(card_ids, weights=probabilities, k=count)
        logger.info(f"Randomly selected {count} cards from pack '{pack_id}' in collection '{collection_id}'")

        # List to store all drawn cards data for adding to user
        cards_to_add = []

        # Pre-fetch card data outside the transaction
        for chosen_card_id in chosen_card_ids:
            # Get the card information from the cards subcollection
            card_ref = pack_ref.collection('cards').document(chosen_card_id)
            card_snap = await card_ref.get()

            if not card_snap.exists:
                logger.error(f"Card '{chosen_card_id}' not found in pack '{pack_id}' in collection '{collection_id}'")
                # Skip this card and continue with others
                continue

            # Get the card data
            card_data = card_snap.to_dict()

            # Log the card information
            logger.info(f"Card drawn from pack '{pack_id}' in collection '{collection_id}':")
            logger.info(f"  Card ID: {chosen_card_id}")
            logger.info(f"  Card Name: {card_data.get('name', '')}")
            # Log card reference, prioritizing card_reference field over globalRef
            if 'card_reference' in card_data:
                logger.info(f"  Card Reference: {card_data.get('card_reference')}")
            elif 'globalRef' in card_data:
                logger.info(f"  Card Reference: {str(card_data.get('globalRef').path)}")
            else:
                logger.info(f"  Card Reference: ")
            logger.info(f"  Image URL: {card_data.get('image_url', '')}")
            logger.info(f"  Point Worth: {card_data.get('point', 0)}")
            logger.info(f"  Quantity: {card_data.get('quantity', 0)}")
            logger.info(f"  Rarity: {card_data.get('rarity', 1)}")

            # Add card to the list of cards to add to user
            # Use card_reference directly if available, otherwise fall back to globalRef
            if 'card_reference' in card_data:
                card_reference = card_data.get('card_reference')
                cards_to_add.append({
                    'card_reference': card_reference,
                    'collection_id': collection_id,
                    'card_id': chosen_card_id
                })
            elif 'globalRef' in card_data:
                card_reference = str(card_data.get('globalRef').path)
                cards_to_add.append({
                    'card_reference': card_reference,
                    'collection_id': collection_id,
                    'card_id': chosen_card_id
                })

        if not cards_to_add:
            raise HTTPException(status_code=404, detail=f"No valid cards found in pack '{pack_id}' in collection '{collection_id}'")

        # Helper function to convert Firestore references to strings
        def convert_references_to_strings(data):
            if isinstance(data, dict):
                result = {}
                for key, value in data.items():
                    # Check for Firestore reference by checking for path attribute or _document_path attribute
                    if hasattr(value, 'path') and callable(getattr(value, 'path', None)):
                        # This is likely a Firestore reference with a callable path method
                        result[key] = str(value.path)
                    elif hasattr(value, '_document_path'):
                        # This is likely a Firestore reference with a _document_path attribute
                        result[key] = str(value._document_path)
                    elif str(type(value)).find('google.cloud.firestore_v1.async_document.AsyncDocumentReference') != -1:
                        # Direct check for AsyncDocumentReference type
                        result[key] = str(value)
                    elif isinstance(value, (dict, list)):
                        result[key] = convert_references_to_strings(value)
                    else:
                        result[key] = value
                return result
            elif isinstance(data, list):
                return [convert_references_to_strings(item) for item in data]
            else:
                # Check if the data itself is a Firestore reference
                if hasattr(data, 'path') and callable(getattr(data, 'path', None)):
                    return str(data.path)
                elif hasattr(data, '_document_path'):
                    return str(data._document_path)
                elif str(type(data)).find('google.cloud.firestore_v1.async_document.AsyncDocumentReference') != -1:
                    return str(data)
                return data

        # Create a list of drawn cards with detailed information
        drawn_cards = []
        for card_data in cards_to_add:
            # Get the card information from the cards subcollection
            card_ref = pack_ref.collection('cards').document(card_data['card_id'])
            card_snap = await card_ref.get()
            card_dict = card_snap.to_dict()

            # Convert any Firestore references to strings
            card_dict = convert_references_to_strings(card_dict)

            # Generate signed URL for the card image if it's a GCS URI
            image_url = card_dict.get('image_url', '')
            if image_url and image_url.startswith('gs://'):
                try:
                    card_dict['image_url'] = await generate_signed_url(image_url)
                except Exception as e:
                    logger.error(f"Failed to sign URL {image_url}: {e}")

            # Create a new dictionary with only the fields we need
            simplified_card = {
                'id': card_data['card_id'],
                'collection_id': collection_id,
                'card_reference': card_data['card_reference'],
                'image_url': card_dict.get('image_url', ''),
                'card_name': card_dict.get('card_name', ''),
                'point_worth': card_dict.get('point_worth', 0),
                'quantity': card_dict.get('quantity', 0),
                'rarity': card_dict.get('rarity', 0)
            }

            drawn_cards.append(simplified_card)

        logger.info(f"Successfully drew {len(drawn_cards)} cards from pack '{pack_id}' in collection '{collection_id}'")

        # Increment the popularity field of the pack by the number of cards drawn
        try:
            await pack_ref.update({"popularity": Increment(len(drawn_cards))})
            logger.info(f"Incremented popularity for pack '{pack_id}' in collection '{collection_id}' by {len(drawn_cards)}")
        except Exception as e:
            logger.error(f"Failed to increment popularity for pack '{pack_id}' in collection '{collection_id}': {e}")
            # Continue even if updating popularity fails

        return drawn_cards
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error drawing multiple cards from pack '{pack_id}' in collection '{collection_id}' for user '{user_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to draw multiple cards from pack '{pack_id}' in collection '{collection_id}': {str(e)}")

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


async def add_card_to_user(
    user_id: str,
    card_reference: str,
    db_client: AsyncClient,
    collection_metadata_id: str = None
) -> str:
    """
    Add a card to a user's cards subcollection under the deepest nested path.
    Decreases the quantity of the original card by 1 (allows negative quantity).

    Args:
        user_id: The ID of the user
        card_reference: Reference to master card ("collection/card_id")
        db_client: Firestore async client
        collection_metadata_id: Optional override for subcollection name

    Returns:
        Success message

    Raises:
        HTTPException on errors
    """
    # 1. Verify user exists
    user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
    user_doc = await user_ref.get()
    if not user_doc.exists:
        raise HTTPException(status_code=404,
                            detail=f"User with ID {user_id} not found")

    # 2. Parse card_reference
    try:
        collection_id, card_id = card_reference.split('/')
    except ValueError:
        raise HTTPException(status_code=400,
                            detail=f"Invalid card reference format: {card_reference}. "
                                   "Expected 'collection/card_id'.")

    # 3. Fetch master card data
    card_ref = db_client.collection(collection_id).document(card_id)
    card_doc = await card_ref.get()
    if not card_doc.exists:
        raise HTTPException(status_code=404,
                            detail=f"Card '{card_reference}' not found")
    card_data = card_doc.to_dict()

    # 4. Prepare payload
    now = datetime.now()
    user_card_data = {
        "card_reference": card_reference,
        "card_name":      card_data.get("card_name", ""),
        "date_got":       firestore.SERVER_TIMESTAMP,
        "id":             card_id,
        "image_url":      card_data.get("image_url", ""),
        "point_worth":    card_data.get("point_worth", 0),
        "quantity":       1,
        "rarity":         card_data.get("rarity", 1),
        "buybackexpiresAt": now + timedelta(days=settings.card_buyback_expire_days)
    }
    if user_card_data["point_worth"] < 1000:
        user_card_data["expireAt"] = now + timedelta(days=settings.card_expire_days)

    # 5. Set up references
    cards_container = user_ref.collection('cards').document('cards')
    subcol = collection_metadata_id or collection_id
    deep_ref = cards_container.collection(subcol).document(card_id)

    # Pre-fetch existence flags
    existing_deep = await deep_ref.get()

    @firestore.async_transactional
    async def _txn(tx: firestore.AsyncTransaction):
        # Decrease the quantity of the original card by 1 (allows negative quantity)
        tx.update(card_ref, {
            "quantity": firestore.Increment(-1)
        })

        if existing_deep.exists:
            # Increment quantity and refresh buyback timestamp
            tx.update(deep_ref, {
                "quantity": firestore.Increment(1),
                "buybackexpiresAt": now + timedelta(days=settings.card_buyback_expire_days)
            })
        else:
            # First-time set
            tx.set(deep_ref, user_card_data)

        logger.info(f"Card stored at users/{user_id}/cards/cards/{subcol}/{card_id}")
        logger.info(f"Decreased quantity of original card {card_reference} by 1")

    # Execute the transaction
    txn = db_client.transaction()
    await _txn(txn)

    # 6. Fetch updated data, sign URL, build UserCard
    updated_doc = await deep_ref.get()
    data = updated_doc.to_dict()
    if data.get("image_url", "").startswith('gs://'):
        try:
            data["image_url"] = await generate_signed_url(data["image_url"])
        except Exception:
            pass

    user_card = UserCard(
        card_reference = data["card_reference"],
        card_name      = data["card_name"],
        date_got       = data["date_got"],
        id             = data["id"],
        image_url      = data["image_url"],
        point_worth    = data["point_worth"],
        quantity       = data["quantity"],
        rarity         = data["rarity"]
    )
    if "expireAt" in data:
        user_card.expireAt = data["expireAt"]
    if "buybackexpiresAt" in data:
        user_card.buybackexpiresAt = data["buybackexpiresAt"]

    return f"Card {card_reference} successfully added to user {user_id}"

async def add_multiple_cards_to_user(
    user_id: str,
    card_references: List[str],
    db_client: AsyncClient,
    collection_metadata_id: str = None
) -> str:
    """
    Add multiple cards to a user's cards subcollection under the deepest nested path.

    Args:
        user_id: The ID of the user
        card_references: List of references to master cards (["collection/card_id", ...])
        db_client: Firestore async client
        collection_metadata_id: Optional override for subcollection name

    Returns:
        Success message

    Raises:
        HTTPException on errors
    """
    # 1. Verify user exists
    user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
    user_doc = await user_ref.get()
    if not user_doc.exists:
        raise HTTPException(status_code=404,
                            detail=f"User with ID {user_id} not found")

    # 2. Process each card
    results = []
    for card_reference in card_references:
        try:
            result = await add_card_to_user(user_id, card_reference, db_client, collection_metadata_id)
            results.append(result)
        except HTTPException as e:
            # Log the error but continue processing other cards
            logger.error(f"Error adding card {card_reference} to user {user_id}: {e.detail}")
            results.append(f"Failed to add card {card_reference}: {e.detail}")
        except Exception as e:
            # Log the error but continue processing other cards
            logger.error(f"Error adding card {card_reference} to user {user_id}: {str(e)}")
            results.append(f"Failed to add card {card_reference}: {str(e)}")

    # 3. Return a summary message
    success_count = sum(1 for result in results if not result.startswith("Failed"))
    return f"Added {success_count} out of {len(card_references)} cards to user {user_id}"




async def destroy_card(
    user_id: str,
    card_id: str,
    subcollection_name: str,
    db_client: AsyncClient,
    quantity: int = 1
) -> tuple[User, UserCard]:
    """
    Destroy a card from a user's collection and add its point_worth to the user's pointsBalance.
    If quantity is less than the card's quantity, only reduce the quantity.
    Only remove the card if the remaining quantity is 0.

    Returns:
        Tuple of (updated_user_model, destroyed_card_model)

    Raises:
        HTTPException on errors
    """
    # 1. Verify user exists
    user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
    user_snap = await user_ref.get()
    if not user_snap.exists:
        raise HTTPException(404, f"User with ID {user_id} not found")
    user = User(**user_snap.to_dict())

    # 2. Load deep subcollection card
    deep_ref = (
        user_ref
        .collection('cards')
        .document('cards')
        .collection(subcollection_name)
        .document(card_id)
    )
    deep_snap = await deep_ref.get()
    if not deep_snap.exists:
        raise HTTPException(404, f"Card {card_id} not found in subcollection {subcollection_name}")
    card = UserCard(**deep_snap.to_dict())

    # 3. Validate quantity
    if quantity <= 0:
        raise HTTPException(400, "Quantity must be greater than 0")
    if quantity > card.quantity:
        raise HTTPException(400, f"Cannot destroy {quantity}, only {card.quantity} available")

    # Calculate points and remaining
    points_to_add = card.point_worth * quantity
    remaining = card.quantity - quantity

    # 4. Pre-fetch main card exists flag
    main_ref = user_ref.collection('cards').document(card_id)
    main_snap = await main_ref.get()

    @firestore.async_transactional
    async def _txn(tx: firestore.AsyncTransaction):
        # Add points to user balance
        tx.update(user_ref, {"pointsBalance": firestore.Increment(points_to_add)})

        if remaining <= 0:
            tx.delete(deep_ref)
            if main_snap.exists:
                tx.delete(main_ref)
            logger.info(f"Deleted card {card_id} for user {user_id}")
        else:
            tx.update(deep_ref, {"quantity": remaining})
            if main_snap.exists:
                tx.update(main_ref, {"quantity": remaining})
            logger.info(f"Updated card {card_id} to quantity {remaining} for user {user_id}")

    txn = db_client.transaction()
    await _txn(txn)

    # 5. Fetch the updated user model
    updated_user_snap = await user_ref.get()
    updated_user = User(**updated_user_snap.to_dict())

    # 6. Return updated user and the card model (with original point_worth)
    return updated_user, card


async def update_user_email_and_address(user_id: str, email: str, db_client: AsyncClient, avatar: Optional[str] = None, addresses: Optional[List[Address]] = None) -> str:
    """
    Update a user's email and avatar fields.

    Args:
        user_id: The ID of the user to update
        email: The email address to set
        db_client: Firestore client
        avatar: Optional image data for user's avatar (can be base64 encoded string or binary data)
        addresses: Optional list of address objects with id, street, city, state, zip, and country

    Returns:
        A success message

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Validate email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            raise HTTPException(status_code=400, detail="Invalid email format")

        # Prepare update data
        update_data = {
            "email": email
        }

        # Convert Address objects to dictionaries for Firestore if provided
        if addresses is not None:
            address_dicts = [address.model_dump() for address in addresses]
            update_data["addresses"] = address_dicts

        # Handle avatar upload if provided
        if avatar is not None:
            try:
                # Upload avatar to GCS
                avatar_gcs_uri = await upload_avatar_to_gcs(avatar, user_id)
                update_data["avatar"] = avatar_gcs_uri
            except HTTPException as e:
                # Re-raise the exception from upload_avatar_to_gcs
                raise e
            except Exception as e:
                logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

        # Update the user's fields
        await user_ref.update(update_data)

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Updated email, addresses, and avatar for user {user_id}")
        return f"Successfully updated email, addresses, and avatar for user {user_id}"
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating email, addresses, and avatar for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update user: {str(e)}")

async def add_user_address(user_id: str, address: Address, db_client: AsyncClient) -> str:
    """
    Add a new address to a user's addresses list.

    Args:
        user_id: The ID of the user to update
        address: The address object to add
        db_client: Firestore client

    Returns:
        A success message

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get current user data
        user_data = user_doc.to_dict()

        # Get current addresses or initialize empty list
        current_addresses = user_data.get("addresses", [])

        # Convert Address object to dictionary for Firestore
        address_dict = address.model_dump()

        # If address doesn't have an ID, generate one
        if not address_dict.get("id"):
            address_dict["id"] = f"address_{len(current_addresses) + 1}"

        # Add the new address to the list
        current_addresses.append(address_dict)

        # Update the user's addresses field
        await user_ref.update({"addresses": current_addresses})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added address with ID {address_dict['id']} to user {user_id}")
        return f"Successfully added address with ID {address_dict['id']} to user {user_id}"
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding address for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add address: {str(e)}")

async def add_points_to_user(user_id: str, points: int, db_client: AsyncClient) -> User:
    """
    Add points to a user's pointsBalance.

    Args:
        user_id: The ID of the user to update
        points: The number of points to add (must be greater than 0)
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Validate points
        if points <= 0:
            raise HTTPException(status_code=400, detail="Points must be greater than 0")

        # Update the user's pointsBalance
        await user_ref.update({"pointsBalance": firestore.Increment(points)})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added {points} points to user {user_id}. New balance: {updated_user.pointsBalance}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding points to user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add points to user: {str(e)}")

async def create_account(request: CreateAccountRequest, db_client: AsyncClient, user_id: Optional[str] = None) -> User:
    """
    Create a new user account with the specified fields and default values.

    Args:
        request: The CreateAccountRequest object containing user data
        db_client: Firestore client
        user_id: Optional user ID. If not provided, a new UUID will be generated.

    Returns:
        The created User object

    Raises:
        HTTPException: If there's an error creating the user
    """
    try:
        # Generate a unique user ID if not provided
        if not user_id:
            user_id = str(UUID.uuid4())

        # Validate email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, request.email):
            raise HTTPException(status_code=400, detail="Invalid email format")

        # Get current timestamp
        now = datetime.now()

        # Generate current month key and last month key if not provided
        current_month_key = request.currentMonthKey or f"{now.year}-{now.month:02d}"
        last_month_key = request.lastMonthKey
        if not last_month_key:
            # Calculate last month
            if now.month == 1:
                last_month = 12
                last_year = now.year - 1
            else:
                last_month = now.month - 1
                last_year = now.year
            last_month_key = f"{last_year}-{last_month:02d}"

        # Handle avatar upload if provided
        avatar_url = request.avatar
        if avatar_url and not avatar_url.startswith(('http://', 'https://', 'gs://')):
            try:
                # Upload avatar to GCS
                avatar_url = await upload_avatar_to_gcs(avatar_url, user_id)
            except Exception as e:
                logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
                # Continue with account creation even if avatar upload fails
                avatar_url = None

        # Convert Address objects to dictionaries for Firestore
        addresses = [address.model_dump() for address in request.addresses]

        # Create user data
        user_data = {
            "createdAt": now,
            "currentMonthCash": 0,
            "currentMonthKey": current_month_key,
            "displayName": request.displayName,
            "email": request.email,
            "addresses": addresses,
            "avatar": avatar_url,
            "lastMonthCash": 0,
            "lastMonthKey": last_month_key,
            "level": 1,
            "pointsBalance": 0,
            "totalCashRecharged": 0,
            "totalPointsSpent": 0
        }

        # Create user document in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        await user_ref.set(user_data)

        # Get the created user
        user_doc = await user_ref.get()
        user_data = user_doc.to_dict()
        user = User(**user_data)

        logger.info(f"Created new user account with ID {user_id}")
        return user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating user account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create user account: {str(e)}")

async def perform_fusion(
    user_id: str,
    result_card_id: str,
    db_client: AsyncClient
) -> PerformFusionResponse:
    """
    Perform a fusion operation for a user.

    This function:
    1. Retrieves the fusion recipe from Firestore
    2. Checks if the user has all required ingredients
    3. Removes the ingredient cards from the user's collection
    4. Adds the result card to the user's collection

    Args:
        user_id: The ID of the user performing the fusion
        result_card_id: The ID of the fusion recipe to use
        db_client: Firestore client

    Returns:
        PerformFusionResponse with success status, message, and the resulting card

    Raises:
        HTTPException: If there's an error performing the fusion
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the fusion recipe
        recipe_ref = db_client.collection('fusion_recipes').document(result_card_id)
        recipe_doc = await recipe_ref.get()

        if not recipe_doc.exists:
            raise HTTPException(status_code=404, detail=f"Fusion recipe with ID '{result_card_id}' not found")

        recipe_data = recipe_doc.to_dict()

        # Check if the user has all required ingredients
        ingredients = recipe_data.get('ingredients', [])
        missing_ingredients = []

        for ingredient in ingredients:
            card_collection_id = ingredient.get('card_collection_id')
            card_id = ingredient.get('card_id')
            required_quantity = ingredient.get('quantity', 1)

            # Check if the user has this card in their collection
            card_ref = user_ref.collection('cards').document('cards').collection(card_collection_id).document(card_id)
            card_doc = await card_ref.get()

            if not card_doc.exists:
                missing_ingredients.append(f"{card_collection_id}/{card_id}")
                continue

            # Check if the user has enough quantity
            card_data = card_doc.to_dict()
            user_quantity = card_data.get('quantity', 0)

            if user_quantity < required_quantity:
                missing_ingredients.append(f"{card_collection_id}/{card_id} (have {user_quantity}, need {required_quantity})")

        # If there are missing ingredients, return an error
        if missing_ingredients:
            return PerformFusionResponse(
                success=False,
                message=f"Missing ingredients for fusion: {', '.join(missing_ingredients)}",
                result_card=None
            )

        # All ingredients are available, perform the fusion
        # 1. Remove the ingredients from the user's collection
        for ingredient in ingredients:
            card_collection_id = ingredient.get('card_collection_id')
            card_id = ingredient.get('card_id')
            required_quantity = ingredient.get('quantity', 1)

            # Get the card from the user's collection
            card_ref = user_ref.collection('cards').document('cards').collection(card_collection_id).document(card_id)
            card_doc = await card_ref.get()
            card_data = card_doc.to_dict()

            user_quantity = card_data.get('quantity', 0)
            remaining_quantity = user_quantity - required_quantity

            if remaining_quantity <= 0:
                # Delete the card if no quantity remains
                await card_ref.delete()

                # Also delete from the main cards collection if it exists
                main_card_ref = user_ref.collection('cards').document(card_id)
                main_card_doc = await main_card_ref.get()
                if main_card_doc.exists:
                    await main_card_ref.delete()
            else:
                # Update the quantity
                await card_ref.update({"quantity": remaining_quantity})

                # Also update in the main cards collection if it exists
                main_card_ref = user_ref.collection('cards').document(card_id)
                main_card_doc = await main_card_ref.get()
                if main_card_doc.exists:
                    await main_card_ref.update({"quantity": remaining_quantity})

        # 2. Add the result card to the user's collection
        card_collection_id = recipe_data.get('card_collection_id')
        card_reference = recipe_data.get('card_reference')

        # Parse the card_reference to get the actual collection name
        try:
            collection_name, card_id = card_reference.split('/')
            logger.info(f"Parsed card_reference '{card_reference}' to collection '{collection_name}' and card_id '{card_id}'")
        except ValueError:
            logger.error(f"Invalid card reference format: {card_reference}. Expected 'collection/card_id'.")
            raise HTTPException(status_code=400, detail=f"Invalid card reference format: {card_reference}")

        # Add the result card to the user's collection
        # Use the collection name from the card_reference as the subcollection name
        await add_card_to_user(
            user_id=user_id,
            card_reference=card_reference,
            db_client=db_client,
            collection_metadata_id=card_collection_id
        )

        # 3. Get the added card to return in the response
        # Use the collection name from the card_reference as the subcollection name
        result_card_ref = user_ref.collection('cards').document('cards').collection(card_collection_id).document(card_id)
        result_card_doc = await result_card_ref.get()

        if not result_card_doc.exists:
            # This shouldn't happen, but just in case
            return PerformFusionResponse(
                success=True,
                message=f"Fusion successful, but couldn't retrieve the result card",
                result_card=None
            )

        result_card_data = result_card_doc.to_dict()

        # Generate signed URL for the card image
        if 'image_url' in result_card_data and result_card_data['image_url']:
            try:
                result_card_data['image_url'] = await generate_signed_url(result_card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {result_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Create a UserCard object from the result card data
        result_card = UserCard(**result_card_data)

        return PerformFusionResponse(
            success=True,
            message=f"Fusion successful! Created {result_card_id}",
            result_card=result_card
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error performing fusion for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to perform fusion: {str(e)}")

async def perform_random_fusion(
    user_id: str,
    fusion_request: RandomFusionRequest,
    db_client: AsyncClient
) -> PerformFusionResponse:
    """
    Perform a random fusion operation for a user.

    This function:
    1. Verifies the user has both cards and they have point_worth < 500
    2. Calculates the combined point_worth and determines the valid range (0.75-0.90)
    3. Queries Firestore for cards in the same collection with point_worth in that range
    4. Randomly selects one of those cards as the result
    5. Removes the ingredient cards from the user's collection
    6. Adds the result card to the user's collection

    Args:
        user_id: The ID of the user performing the fusion
        fusion_request: The RandomFusionRequest containing card IDs and collection
        db_client: Firestore client

    Returns:
        PerformFusionResponse with success status, message, and the resulting card

    Raises:
        HTTPException: If there's an error performing the fusion
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the cards from the user's collection
        card1_ref = user_ref.collection('cards').document('cards').collection(fusion_request.collection_id).document(fusion_request.card_id1)
        card2_ref = user_ref.collection('cards').document('cards').collection(fusion_request.collection_id).document(fusion_request.card_id2)

        card1_doc = await card1_ref.get()
        card2_doc = await card2_ref.get()

        # Check if the cards exist
        if not card1_doc.exists:
            return PerformFusionResponse(
                success=False,
                message=f"Card {fusion_request.card_id1} not found in your collection",
                result_card=None
            )

        if not card2_doc.exists:
            return PerformFusionResponse(
                success=False,
                message=f"Card {fusion_request.card_id2} not found in your collection",
                result_card=None
            )

        # Check if the cards are different
        if fusion_request.card_id1 == fusion_request.card_id2:
            return PerformFusionResponse(
                success=False,
                message="Cannot fuse the same card with itself",
                result_card=None
            )

        # Get the card data
        card1_data = card1_doc.to_dict()
        card2_data = card2_doc.to_dict()

        # Check if the cards have point_worth < 500
        card1_point_worth = card1_data.get('point_worth', 0)
        card2_point_worth = card2_data.get('point_worth', 0)

        if card1_point_worth >= 500:
            return PerformFusionResponse(
                success=False,
                message=f"Card {fusion_request.card_id1} has point_worth {card1_point_worth}, which is >= 500",
                result_card=None
            )

        if card2_point_worth >= 500:
            return PerformFusionResponse(
                success=False,
                message=f"Card {fusion_request.card_id2} has point_worth {card2_point_worth}, which is >= 500",
                result_card=None
            )

        # Calculate the combined point_worth and determine the valid range
        combined_point_worth = card1_point_worth + card2_point_worth
        min_point_worth = int(combined_point_worth * 0.75)
        max_point_worth = int(combined_point_worth * 0.90)

        logger.info(f"Combined point_worth: {combined_point_worth}, valid range: {min_point_worth} - {max_point_worth}")

        # Extract the collection name from the card_reference of one of the cards
        collection_name = fusion_request.collection_id  # Default fallback

        # Try to get card_reference from card1 first
        if 'card_reference' in card1_data:
            try:
                # Parse the card_reference to get the collection name
                collection_name, _ = card1_data['card_reference'].split('/')
                logger.info(f"Extracted collection name '{collection_name}' from card1 reference: {card1_data['card_reference']}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to extract collection name from card1 reference: {e}")

        # If we couldn't get it from card1, try card2
        elif 'card_reference' in card2_data:
            try:
                # Parse the card_reference to get the collection name
                collection_name, _ = card2_data['card_reference'].split('/')
                logger.info(f"Extracted collection name '{collection_name}' from card2 reference: {card2_data['card_reference']}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to extract collection name from card2 reference: {e}")

        # Query Firestore for cards in the extracted collection with point_worth in the valid range
        collection_ref = db_client.collection(collection_name)
        logger.info(f"Querying collection: {collection_name}")

        # Use where method for filtering
        query = collection_ref.where("point_worth", ">=", min_point_worth).where("point_worth", "<=", max_point_worth)

        # Execute the query
        cards_stream = query.stream()

        # Collect all valid cards
        valid_cards = []
        async for card_doc in cards_stream:
            # Print out the document for debugging
            logger.info(f"Found document: {card_doc.id}, data: {card_doc.to_dict()}")
            card_data = card_doc.to_dict()
            # Only include cards with point_worth > 0
            if card_data.get('point_worth', 0) > 0:
                valid_cards.append({
                    'id': card_doc.id,
                    'data': card_data
                })

        # Check if there are any valid cards
        if not valid_cards:
            return PerformFusionResponse(
                success=False,
                message=f"No valid cards found in collection {collection_name} with point_worth between {min_point_worth} and {max_point_worth}",
                result_card=None
            )

        # Randomly select one of the valid cards
        result_card_info = random.choice(valid_cards)
        result_card_id = result_card_info['id']
        result_card_data = result_card_info['data']

        logger.info(f"Randomly selected card {result_card_id} with point_worth {result_card_data.get('point_worth', 0)}")

        # Remove the ingredient cards from the user's collection
        # First card
        card1_quantity = card1_data.get('quantity', 0)
        remaining_quantity1 = card1_quantity - 1

        if remaining_quantity1 <= 0:
            # Delete the card if no quantity remains
            await card1_ref.delete()

            # Also delete from the main cards collection if it exists
            main_card1_ref = user_ref.collection('cards').document(fusion_request.card_id1)
            main_card1_doc = await main_card1_ref.get()
            if main_card1_doc.exists:
                await main_card1_ref.delete()
        else:
            # Update the quantity
            await card1_ref.update({"quantity": remaining_quantity1})

            # Also update in the main cards collection if it exists
            main_card1_ref = user_ref.collection('cards').document(fusion_request.card_id1)
            main_card1_doc = await main_card1_ref.get()
            if main_card1_doc.exists:
                await main_card1_ref.update({"quantity": remaining_quantity1})

        # Second card
        card2_quantity = card2_data.get('quantity', 0)
        remaining_quantity2 = card2_quantity - 1

        if remaining_quantity2 <= 0:
            # Delete the card if no quantity remains
            await card2_ref.delete()

            # Also delete from the main cards collection if it exists
            main_card2_ref = user_ref.collection('cards').document(fusion_request.card_id2)
            main_card2_doc = await main_card2_ref.get()
            if main_card2_doc.exists:
                await main_card2_ref.delete()
        else:
            # Update the quantity
            await card2_ref.update({"quantity": remaining_quantity2})

            # Also update in the main cards collection if it exists
            main_card2_ref = user_ref.collection('cards').document(fusion_request.card_id2)
            main_card2_doc = await main_card2_ref.get()
            if main_card2_doc.exists:
                await main_card2_ref.update({"quantity": remaining_quantity2})

        # Add the result card to the user's collection
        card_reference = f"{collection_name}/{result_card_id}"

        await add_card_to_user(
            user_id=user_id,
            card_reference=card_reference,
            db_client=db_client,
            collection_metadata_id=collection_name
        )

        # Get the added card to return in the response
        result_card_ref = user_ref.collection('cards').document('cards').collection(collection_name).document(result_card_id)
        result_card_doc = await result_card_ref.get()

        if not result_card_doc.exists:
            # This shouldn't happen, but just in case
            return PerformFusionResponse(
                success=True,
                message=f"Random fusion successful, but couldn't retrieve the result card",
                result_card=None
            )

        result_card_data = result_card_doc.to_dict()

        # Generate signed URL for the card image
        if 'image_url' in result_card_data and result_card_data['image_url']:
            try:
                result_card_data['image_url'] = await generate_signed_url(result_card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {result_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Create a UserCard object from the result card data
        result_card = UserCard(**result_card_data)

        return PerformFusionResponse(
            success=True,
            message=f"Random fusion successful! Created {result_card_id} with point_worth {result_card.point_worth}",
            result_card=result_card
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error performing random fusion for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to perform random fusion: {str(e)}")

async def delete_user_address(user_id: str, address_id: str, db_client: AsyncClient) -> str:
    """
    Delete an address from a user's addresses list.

    Args:
        user_id: The ID of the user to update
        address_id: The ID of the address to delete
        db_client: Firestore client

    Returns:
        A success message

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get current user data
        user_data = user_doc.to_dict()

        # Get current addresses
        current_addresses = user_data.get("addresses", [])

        # Find the address with the given ID
        address_found = False
        updated_addresses = []
        for addr in current_addresses:
            if addr.get("id") != address_id:
                updated_addresses.append(addr)
            else:
                address_found = True

        if not address_found:
            raise HTTPException(status_code=404, detail=f"Address with ID {address_id} not found for user {user_id}")

        # Update the user's addresses field
        await user_ref.update({"addresses": updated_addresses})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Deleted address with ID {address_id} from user {user_id}")
        return f"Successfully deleted address with ID {address_id} from user {user_id}"
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting address for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete address: {str(e)}")

async def withdraw_ship_card(user_id: str, card_id: str, subcollection_name: str, db_client: AsyncClient, quantity: int = 1) -> UserCard:
    """
    Withdraw or ship a card from a user's collection by moving it to the "shipped" subcollection.
    If quantity is less than the card's quantity, only move the specified quantity.
    Only remove the card from the original subcollection if the remaining quantity is 0.

    Args:
        user_id: The ID of the user who owns the card
        card_id: The ID of the card to withdraw/ship
        subcollection_name: The name of the subcollection where the card is currently stored
        db_client: Firestore client
        quantity: The quantity to withdraw/ship (default: 1)

    Returns:
        The updated shipped card as a UserCard object

    Raises:
        HTTPException: If there's an error withdrawing/shipping the card
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the card from the source subcollection
        source_card_ref = user_ref.collection('cards').document('cards').collection(subcollection_name).document(card_id)
        source_card_doc = await source_card_ref.get()

        if not source_card_doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in subcollection {subcollection_name}")

        source_card_data = source_card_doc.to_dict()
        card = UserCard(**source_card_data)

        # Get the card's quantity
        card_quantity = card.quantity

        # Ensure quantity is valid
        if quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

        if quantity > card_quantity:
            raise HTTPException(status_code=400, detail=f"Cannot withdraw/ship {quantity} cards, only {card_quantity} available")

        # Calculate remaining quantity
        remaining_quantity = card_quantity - quantity

        # Get the "shipped" subcollection reference
        shipped_subcoll_ref = user_ref.collection('cards').document('cards').collection('shipped')
        shipped_card_ref = shipped_subcoll_ref.document(card_id)

        # Pre-fetch document snapshots outside the transaction
        shipped_card_doc = await shipped_card_ref.get()

        # Get the main card reference and pre-fetch it
        main_card_ref = user_ref.collection('cards').document(card_id)
        main_card_doc = await main_card_ref.get()

        @firestore.async_transactional
        async def move_card_to_shipped(transaction, source_card_ref, shipped_card_ref, remaining_quantity, quantity_to_ship):
            # Prepare the card data for the shipped subcollection
            shipped_card_data = source_card_data.copy()

            # Add request date timestamp
            shipped_card_data['request_date'] = datetime.now()

            # Now perform all writes
            if shipped_card_doc.exists:
                # If the card already exists in shipped, update its quantity
                existing_shipped_data = shipped_card_doc.to_dict()
                existing_quantity = existing_shipped_data.get('quantity', 0)
                shipped_card_data['quantity'] = existing_quantity + quantity_to_ship
                transaction.update(shipped_card_ref, shipped_card_data)
            else:
                # If the card doesn't exist in shipped, set its quantity to the shipped amount
                shipped_card_data['quantity'] = quantity_to_ship
                transaction.set(shipped_card_ref, shipped_card_data)

            if remaining_quantity <= 0:
                # Delete the card from the source subcollection if quantity is 0
                transaction.delete(source_card_ref)

                # Also delete from the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.delete(main_card_ref)

                logger.info(f"Moved all {quantity_to_ship} of card {card_id} from user {user_id}'s subcollection {subcollection_name} to 'shipped'")
            else:
                # Update the quantity in the source subcollection
                transaction.update(source_card_ref, {"quantity": remaining_quantity})

                # Also update in the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.update(main_card_ref, {"quantity": remaining_quantity})

                logger.info(f"Moved {quantity_to_ship} of card {card_id} from user {user_id}'s subcollection {subcollection_name} to 'shipped', {remaining_quantity} remaining")

        # Execute the transaction
        transaction = db_client.transaction()
        await move_card_to_shipped(transaction, source_card_ref, shipped_card_ref, remaining_quantity, quantity)

        # Get the updated shipped card
        updated_shipped_card_doc = await shipped_card_ref.get()
        updated_shipped_card_data = updated_shipped_card_doc.to_dict()

        # Ensure ID is part of the data
        if 'id' not in updated_shipped_card_data:
            updated_shipped_card_data['id'] = card_id

        # Generate signed URL for the card image
        if 'image_url' in updated_shipped_card_data and updated_shipped_card_data['image_url']:
            try:
                updated_shipped_card_data['image_url'] = await generate_signed_url(updated_shipped_card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {updated_shipped_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Create and return a UserCard object from the updated shipped card data
        shipped_card = UserCard(
            card_reference=updated_shipped_card_data.get("card_reference", ""),
            card_name=updated_shipped_card_data.get("card_name", ""),
            date_got=updated_shipped_card_data.get("date_got"),
            id=updated_shipped_card_data.get("id", card_id),
            image_url=updated_shipped_card_data.get("image_url", ""),
            point_worth=updated_shipped_card_data.get("point_worth", 0),
            quantity=updated_shipped_card_data.get("quantity", 0),
            rarity=updated_shipped_card_data.get("rarity", 1)
        )

        # Add optional fields if they exist in the data
        if "expireAt" in updated_shipped_card_data:
            shipped_card.expireAt = updated_shipped_card_data["expireAt"]
        if "buybackexpiresAt" in updated_shipped_card_data:
            shipped_card.buybackexpiresAt = updated_shipped_card_data["buybackexpiresAt"]
        if "request_date" in updated_shipped_card_data:
            shipped_card.request_date = updated_shipped_card_data["request_date"]

        # Log success message
        if remaining_quantity <= 0:
            logger.info(f"Successfully moved all {quantity} of card {card_id} from user {user_id}'s subcollection {subcollection_name} to 'shipped'")
        else:
            logger.info(f"Successfully moved {quantity} of card {card_id} from user {user_id}'s subcollection {subcollection_name} to 'shipped', {remaining_quantity} remaining")

        return shipped_card

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error withdrawing/shipping card {card_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to withdraw/ship card: {str(e)}")

async def get_user_card(
    user_id: str,
    collection_id: str,
    card_id: str,
    db_client: AsyncClient
) -> UserCard:
    """
    Get a specific card from a user's collection.

    Args:
        user_id: The ID of the user who owns the card
        collection_id: The collection ID of the card (e.g., 'pokemon')
        card_id: The ID of the card to retrieve
        db_client: Firestore client

    Returns:
        UserCard: The requested card

    Raises:
        HTTPException: If the user or card is not found
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the card reference
        card_ref = user_ref.collection('cards').document('cards').collection(collection_id).document(card_id)
        card_doc = await card_ref.get()

        if not card_doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in collection {collection_id}")

        # Get the card data
        card_data = card_doc.to_dict()

        # Generate signed URL for the card image
        if 'image_url' in card_data and card_data['image_url']:
            try:
                card_data['image_url'] = await generate_signed_url(card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Create and return a UserCard object from the card data
        user_card = UserCard(**card_data)

        return user_card

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting card {card_id} from collection {collection_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get card: {str(e)}")

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
        if not card_doc.exists:
            # If the card doesn't exist anymore, just delete the listing
            await listing_ref.delete()
            return {"message": f"Listing {listing_id} withdrawn successfully, but card no longer exists"}

        # Get the current card data
        card_data = card_doc.to_dict()
        current_locked_quantity = card_data.get("locked_quantity", 0)
        current_quantity = card_data.get("quantity", 0)

        # 4. Update the user's card in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Decrease the locked_quantity
            new_locked_quantity = max(0, current_locked_quantity - listing_quantity)

            # Increase the quantity
            new_quantity = current_quantity + listing_quantity

            # Update both locked_quantity and quantity
            tx.update(card_ref, {
                "locked_quantity": new_locked_quantity,
                "quantity": new_quantity
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
    db_client: AsyncClient
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

        # Get the point_offers subcollection reference
        point_offers_ref = listing_ref.collection('point_offers')
        new_offer_ref = point_offers_ref.document()  # Auto-generate ID

        offer_data = {
            "offererRef": user_ref.path,  # Reference to the user making the offer
            "amount": offer_request.points,  # Points offered
            "at": now,  # Timestamp of the offer
            "offerreference": new_offer_ref.id,  # Reference to this offer
            "type": "point"  # Indicate this is a point offer
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

async def offer_cash(
    user_id: str,
    listing_id: str,
    offer_request: OfferCashRequest,
    db_client: AsyncClient
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

        # Get the cash_offers subcollection reference
        cash_offers_ref = listing_ref.collection('cash_offers')
        new_offer_ref = cash_offers_ref.document()  # Auto-generate ID

        offer_data = {
            "offererRef": user_ref.path,  # Reference to the user making the offer
            "amount": offer_request.cash,  # Cash offered
            "at": now,  # Timestamp of the offer
            "offerreference": new_offer_ref.id,  # Reference to this offer
            "type": "cash"  # Indicate this is a cash offer
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
