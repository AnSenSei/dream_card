from typing import Optional, Dict, List, Tuple
from uuid import UUID
import random
import math
import re
from datetime import datetime, timedelta

from fastapi import HTTPException
import httpx
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, SERVER_TIMESTAMP, async_transactional

from config import get_logger, settings
from models.schemas import User, UserCard, PaginationInfo, AppliedFilters, UserCardListResponse, UserCardsResponse, Address
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

async def draw_multiple_cards_from_pack(collection_id: str, pack_id: str, user_id: str, db_client: AsyncClient, count: int = 5) -> list:
    """
    Draw multiple cards (5 or 10) from a pack based on probabilities.

    This function:
    1. Gets all probabilities from cards.values() in the pack
    2. Randomly chooses multiple card ids based on these probabilities
    3. Retrieves the card information from the cards subcollection for each card
    4. Logs the card information and returns the list of drawn cards

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
