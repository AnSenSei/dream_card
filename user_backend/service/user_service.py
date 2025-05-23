from typing import Optional, Dict, List, Tuple, Any
from uuid import UUID
import random
import math
import re
import secrets
import hashlib
import hmac
from datetime import datetime, timedelta

from fastapi import HTTPException
import httpx
import asyncpg
from shippo import Shippo
from shippo.models import components
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, SERVER_TIMESTAMP, async_transactional, Increment

from config import get_logger, settings
from config.db_connection import db_connection
from models.schemas import User, UserCard, PaginationInfo, AppliedFilters, UserCardListResponse, UserCardsResponse, Address, CreateAccountRequest, PerformFusionResponse, RandomFusionRequest, CardListing, CreateCardListingRequest, OfferPointsRequest, OfferCashRequest, UpdatePointOfferRequest, UpdateCashOfferRequest, CheckCardMissingResponse, MissingCard, FusionRecipeMissingCards, RankEntry, WithdrawRequest, WithdrawRequestDetail
from utils.gcs_utils import generate_signed_url, upload_avatar_to_gcs, parse_base64_image

logger = get_logger(__name__)

async def validate_address_with_shippo(address: Address) -> bool:
    """
    Validate an address using the latest Shippo Python SDK.

    Args:
        address: The address object to validate (including name)

    Returns:
        True if the address is valid, False otherwise

    Raises:
        HTTPException: If there's an error validating the address
    """
    try:
        # Configure Shippo SDK
        if not hasattr(settings, 'shippo_api_key') or not settings.shippo_api_key:
            logger.error("Shippo API key not configured")
            raise HTTPException(status_code=500, detail="Address validation service not configured")

        # Initialize the Shippo SDK with API key
        shippo_sdk = Shippo(
            api_key_header=settings.shippo_api_key
        )

        # Create address object for validation using the new SDK structure
        logger.info(f"Validating address for {address.name}: {address.street}, {address.city}, {address.state}, {address.zip}, {address.country}")

        # Create and validate the address using the new SDK
        validation_result = shippo_sdk.addresses.create(
            components.AddressCreateRequest(
                name=address.name,
                street1=address.street,
                city=address.city,
                state=address.state,
                zip=address.zip,
                country=address.country,
                validate=True
            )
        )

        # Check if validation was successful
        if not validation_result:
            raise HTTPException(status_code=400, detail="Address validation failed: No response from service")

        # Access validation results from the response
        validation_results = validation_result.validation_results

        if not validation_results:
            logger.warning(f"No validation results returned for address: {validation_result}")
            return True  # If no validation results, consider it valid (some addresses may not support validation)

        # Check if the address is valid
        is_valid = validation_results.is_valid

        if not is_valid:
            # Get validation messages for detailed error reporting
            error_messages = []

            messages = validation_results.messages or []
            for msg in messages:
                error_text = msg.text if hasattr(msg, 'text') else str(msg)
                error_code = msg.code if hasattr(msg, 'code') else ""
                if error_code:
                    error_messages.append(f"{error_code}: {error_text}")
                else:
                    error_messages.append(error_text)

            error_detail = "; ".join(error_messages) if error_messages else "Address validation failed"
            logger.warning(f"Address validation failed for {address.name}: {error_detail}")
            raise HTTPException(status_code=400, detail=f"Address validation failed: {error_detail}")

        logger.info(f"Address validated successfully for {address.name}")
        return True

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Handle other errors
        logger.error(f"Unexpected error validating address: {e}", exc_info=True)
        error_msg = str(e)

        # Check if it's a Shippo-specific error
        if "shippo" in error_msg.lower() or "api" in error_msg.lower():
            raise HTTPException(status_code=400, detail=f"Address validation failed: {error_msg}")

        # Check if it's a network/connection error
        if any(keyword in error_msg.lower() for keyword in ['connection', 'timeout', 'network', 'dns']):
            raise HTTPException(status_code=503, detail="Address validation service temporarily unavailable")

        # Check if it's an authentication error
        if any(keyword in error_msg.lower() for keyword in ['unauthorized', 'forbidden', 'authentication', 'api key']):
            raise HTTPException(status_code=500, detail="Address validation service configuration error")

        raise HTTPException(status_code=500, detail=f"Failed to validate address: {error_msg}")

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

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

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


async def draw_multiple_cards_from_pack(collection_id: str, pack_id: str, user_id: str, db_client: AsyncClient,
                                        count: int = 5) -> list:
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
        if count not in [1, 5, 10]:
            logger.error(f"Invalid count parameter: {count}. Must be 1,5 or 10.")
            raise HTTPException(status_code=400, detail=f"Invalid count parameter: {count}. Must be 5 or 10.")

        logger.info(f"Drawing {count} cards from pack '{pack_id}' in collection '{collection_id}' for user '{user_id}'")

        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get user data
        user_data = user_doc.to_dict()
        user_points_balance = user_data.get('pointsBalance', 0)

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

        # Check if user has enough points to draw the requested number of cards
        pack_price = pack_data.get('price', 0)
        total_price = pack_price * count

        if user_points_balance < total_price:
            logger.error(
                f"User '{user_id}' has insufficient points balance ({user_points_balance}) to draw {count} cards (cost: {total_price})")
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient points balance. You have {user_points_balance} points, but need {total_price} points to draw {count} cards."
            )

        if not cards_map:
            logger.error(f"No cards found in pack '{pack_id}' in collection '{collection_id}'")
            raise HTTPException(status_code=404,
                                detail=f"No cards found in pack '{pack_id}' in collection '{collection_id}'")

        # Get all probabilities from the cards map
        card_ids = list(cards_map.keys())
        probabilities = list(cards_map.values())

        # Check if there are enough cards in the pack
        if len(card_ids) < count:
            logger.warning(
                f"Not enough cards in pack '{pack_id}' in collection '{collection_id}'. Requested {count} but only {len(card_ids)} available.")
            # We'll still draw as many as possible, but with replacement

        # 3. Firestore transaction to get clientSeed and starting nonce
        user_doc_ref = db_client.collection(settings.firestore_collection_users).document(user_id)

        # Variables to store the values outside the transaction
        client_seed = None
        nonce = None

        # Get user data first, outside the transaction
        user_snap = await user_doc_ref.get()
        user_data = user_snap.to_dict()
        client_seed = user_data.get('clientSeed')
        old_nonce = user_data.get('nonceCounter', 0)
        nonce = old_nonce + 1

        # Update counter in a transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            tx.update(user_doc_ref, {'nonceCounter': firestore.Increment(count)})

        # Execute the transaction
        txn = db_client.transaction()
        await _txn(txn)
        logger.info(f"Retrieved client seed '{client_seed}' and nonce {nonce} for user '{user_id}'")

        # 4. Provably fair seeds and proof
        server_seed = secrets.token_hex(32)
        server_seed_hash = hashlib.sha256(server_seed.encode()).hexdigest()
        # Compute HMAC for the batch
        payload = f"{client_seed}{nonce}".encode()
        random_hash = hmac.new(server_seed.encode(), payload, hashlib.sha256).hexdigest()
        logger.info(
            f"Generated server seed '{server_seed}' with hash '{server_seed_hash}' and random hash '{random_hash}'")

        # 5. Perform deterministic draw using Python RNG seeded by random_hash
        random.seed(int(random_hash, 16))
        chosen_card_ids = random.choices(card_ids, weights=probabilities, k=count)
        logger.info(f"Deterministically selected {count} cards from pack '{pack_id}' in collection '{collection_id}'")

        # 6. Insert into SQL tables
        opening_id = None
        try:
            with db_connection() as conn:
                cursor = conn.cursor()
                # pack_openings
                # Create a full pack reference that includes collection_id and pack_id
                pack_reference = f"packs/{collection_id}/{collection_id}/{pack_id}"
                cursor.execute(
                    """
                    INSERT INTO pack_openings (user_id, pack_type, pack_count, price_points,
                                               client_seed, nonce, server_seed_hash, server_seed, random_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """,
                    (user_id, pack_reference, count, total_price,
                     client_seed, nonce, server_seed_hash, server_seed, random_hash)
                )
                opening_id = cursor.fetchone()[0]

                # transactions
                cursor.execute(
                    """
                    INSERT INTO transactions (user_id, type, points_delta, reference_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (user_id, 'pack_open', -total_price, str(opening_id))
                )
                conn.commit()
                logger.info(f"Inserted pack opening record with ID {opening_id} and transaction record")
        except Exception as e:
            logger.error(f"Failed to insert SQL records: {e}")
            # Continue even if SQL insertion fails

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
            logger.info(f"  Point Worth: {card_data.get('point_worth', 0)}")
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
            raise HTTPException(status_code=404,
                                detail=f"No valid cards found in pack '{pack_id}' in collection '{collection_id}'")

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
        for index, card_data in enumerate(cards_to_add, 1):
            # Get the card information from the cards subcollection
            card_ref = pack_ref.collection('cards').document(card_data['card_id'])
            card_snap = await card_ref.get()
            card_dict = card_snap.to_dict()

            # Convert any Firestore references to strings
            card_dict = convert_references_to_strings(card_dict)

            # Ensure card_reference is a string
            if 'card_reference' in card_data and not isinstance(card_data['card_reference'], str):
                card_data['card_reference'] = convert_references_to_strings(card_data['card_reference'])

            # Generate signed URL for the card image
            image_url = card_dict.get('image_url', '')
            if image_url:
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
                'rarity': card_dict.get('rarity', 0),
                'num_draw': index  # Add the position of the card in the drawing sequence
            }

            drawn_cards.append(simplified_card)

        logger.info(f"Successfully drew {len(drawn_cards)} cards from pack '{pack_id}' in collection '{collection_id}'")

        # Increment the popularity field of the pack by the number of cards drawn
        try:
            await pack_ref.update({"popularity": Increment(len(drawn_cards))})
            logger.info(
                f"Incremented popularity for pack '{pack_id}' in collection '{collection_id}' by {len(drawn_cards)}")
        except Exception as e:
            logger.error(f"Failed to increment popularity for pack '{pack_id}' in collection '{collection_id}': {e}")
            # Continue even if updating popularity fails

        # Increment the total_drawn and totalPointsSpent fields in the user's document by the pack price multiplied by the number of cards drawn
        # Also deduct the points from the user's balance
        try:
            # We already calculated these values earlier
            # pack_price = pack_data.get('price', 0)
            # total_price = pack_price * len(drawn_cards)

            await user_ref.update({
                "total_drawn": Increment(total_price),
                "totalPointsSpent": Increment(total_price),
                "pointsBalance": Increment(-total_price)  # Deduct points from balance
            })
            logger.info(
                f"Incremented total_drawn and totalPointsSpent for user '{user_id}' by pack price ({pack_price}) * number of cards drawn ({len(drawn_cards)}) = {total_price}")
            logger.info(
                f"Deducted {total_price} points from user '{user_id}' balance. New balance: {user_points_balance - total_price}")

            # Update weekly_spent collection
            try:
                # Get the current week's start date (Monday)
                today = datetime.now()
                start_of_week = today - timedelta(days=today.weekday())
                week_id = start_of_week.strftime("%Y-%m-%d")

                # Reference to the weekly_spent collection and this week's document
                weekly_spent_ref = db_client.collection('weekly_spent').document('weekly_spent').collection(
                    week_id).document(user_id)
                weekly_doc = await weekly_spent_ref.get()

                if weekly_doc.exists:
                    # Update existing document
                    await weekly_spent_ref.update({
                        "spent": Increment(total_price),
                        "updatedAt": SERVER_TIMESTAMP
                    })
                else:
                    # Create new document
                    await weekly_spent_ref.set({
                        "spent": total_price,
                        "updatedAt": SERVER_TIMESTAMP
                    })
                logger.info(f"Updated weekly_spent for user '{user_id}' with {total_price} points")
            except Exception as e:
                logger.error(f"Failed to update weekly_spent for user '{user_id}': {e}")
                # Continue even if updating weekly_spent fails
        except Exception as e:
            logger.error(f"Failed to increment total_drawn and totalPointsSpent for user '{user_id}': {e}")
            # Continue even if updating total_drawn and totalPointsSpent fails

        # Add provably fair information to the response
        provably_fair_info = {
            "server_seed_hash": server_seed_hash,
            "client_seed": client_seed,
            "nonce": nonce,
            "opening_id": opening_id
        }

        # Add provably fair info to each card
        for card in drawn_cards:
            card["provably_fair_info"] = provably_fair_info

        return drawn_cards
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(
            f"Error drawing multiple cards from pack '{pack_id}' in collection '{collection_id}' for user '{user_id}': {e}",
            exc_info=True)
        raise HTTPException(status_code=500,
                            detail=f"Failed to draw multiple cards from pack '{pack_id}' in collection '{collection_id}': {str(e)}")

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


async def add_cards_and_deduct_points(
    user_id: str,
    card_references: List[str],
    points_to_deduct: int,
    db_client: AsyncClient,
    collection_metadata_id: str = None
) -> dict:
    """
    Add multiple cards to a user's collection and deduct points in a single atomic transaction.

    Args:
        user_id: The ID of the user
        card_references: List of references to master cards (["collection/card_id", ...])
        points_to_deduct: Number of points to deduct from user's balance
        db_client: Firestore async client
        collection_metadata_id: Optional override for subcollection name

    Returns:
        Dictionary with success message and details

    Raises:
        HTTPException on errors
    """
    logger = get_logger(__name__)

    # 1. Verify user exists and has enough points
    user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
    user_doc = await user_ref.get()
    if not user_doc.exists:
        raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

    user_data = user_doc.to_dict()
    current_points = user_data.get("pointsBalance", 0)

    if current_points < points_to_deduct:
        raise HTTPException(status_code=400, 
                           detail=f"Insufficient points. User has {current_points} points, but {points_to_deduct} are required.")

    # 2. Validate points_to_deduct
    if points_to_deduct <= 0:
        raise HTTPException(status_code=400, detail="Points to deduct must be greater than 0")

    # 3. Deduct points from user

    # 4. Add cards to user's collection by calling add_card_to_user for each card
    results = []
    for card_reference in card_references:
        try:
            result = await add_card_to_user(
                user_id=user_id,
                card_reference=card_reference,
                db_client=db_client,
                collection_metadata_id=collection_metadata_id
            )
            results.append(result)
        except HTTPException as e:
            # Log the error but continue processing other cards
            logger.error(f"Error adding card {card_reference} to user {user_id}: {e.detail}")
            results.append(f"Failed to add card {card_reference}: {e.detail}")
        except Exception as e:
            # Log the error but continue processing other cards
            logger.error(f"Error adding card {card_reference} to user {user_id}: {str(e)}")
            results.append(f"Failed to add card {card_reference}: {str(e)}")

    @firestore.async_transactional
    async def _deduct_points_txn(tx: firestore.AsyncTransaction):
        tx.update(user_ref, {
            "pointsBalance": firestore.Increment(-points_to_deduct)
        })

    # Execute the transaction to deduct points
    txn = db_client.transaction()
    await _deduct_points_txn(txn)

    # 5. Get updated user data
    updated_user_doc = await user_ref.get()
    updated_user_data = updated_user_doc.to_dict()

    # 6. Return success message with details
    success_count = sum(1 for result in results if not result.startswith("Failed"))
    return {
        "message": f"Successfully added {success_count} card(s) and deducted {points_to_deduct} points",
        "remaining_points": updated_user_data.get("pointsBalance", 0),
        "cards_added": success_count
    }




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
                main_card = main_snap.to_dict()
                main_remaining = main_card["quantity"] - quantity
                tx.update(main_ref, {"quantity": main_remaining})
            logger.info(f"Updated card {card_id} to quantity {remaining} for user {user_id}")

    txn = db_client.transaction()
    await _txn(txn)

    # 5. Fetch the updated user model
    updated_user_snap = await user_ref.get()
    updated_user = User(**updated_user_snap.to_dict())

    # 6. Return updated user and the card model (with original point_worth)
    return updated_user, card


async def update_user_email_and_address(user_id: str, email: Optional[str] = None, db_client: AsyncClient = None, avatar: Optional[Any] = None, addresses: Optional[List[Address]] = None) -> User:
    """
    Update a user's email and avatar fields.

    Args:
        user_id: The ID of the user to update
        email: Optional email address to update
        db_client: Firestore client
        avatar: Optional image data for user's avatar (can be base64 encoded string or binary data as bytes)
        addresses: Optional list of address objects with id, street, city, state, zip, and country

    Returns:
        The updated User object with a signed URL for the avatar if it exists

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get user data for display name
        user_data = user_doc.to_dict()
        user_name = user_data.get("displayName", "User")

        # Prepare update data
        update_data = {}

        # Only update email if provided
        if email is not None:
            # Validate email format
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, email):
                raise HTTPException(status_code=400, detail="Invalid email format")
            update_data["email"] = email

        # Convert Address objects to dictionaries for Firestore if provided
        if addresses is not None:
            # Validate each address with Shippo before updating
            for address in addresses:
                await validate_address_with_shippo(address)

            address_dicts = [address.model_dump() for address in addresses]
            update_data["addresses"] = address_dicts

        # Handle avatar upload if provided
        if avatar is not None:
            try:
                import base64

                if isinstance(avatar, str) and avatar.strip():  # String avatar
                    if avatar.startswith('data:'):
                        # Handle base64 encoded data URI
                        content_type, base64_data = parse_base64_image(avatar)
                        avatar_bytes = base64.b64decode(base64_data)
                        avatar_gcs_uri = await upload_avatar_to_gcs(avatar_bytes, user_id, content_type)
                    else:
                        # Assume it's a base64 string without data URI prefix
                        try:
                            avatar_bytes = base64.b64decode(avatar)
                            avatar_gcs_uri = await upload_avatar_to_gcs(avatar_bytes, user_id)
                        except Exception:
                            # If it's not base64, treat it as a URL/string
                            update_data["avatar"] = avatar
                            avatar_gcs_uri = None
                elif isinstance(avatar, bytes):  # Binary data
                    # Handle bytes directly
                    avatar_gcs_uri = await upload_avatar_to_gcs(avatar, user_id)
                else:
                    # Unsupported avatar type
                    logger.warning(f"Unsupported avatar type: {type(avatar)}")
                    raise HTTPException(status_code=400, detail="Unsupported avatar format")

                if avatar_gcs_uri:
                    update_data["avatar"] = avatar_gcs_uri
            except HTTPException as e:
                # Re-raise the exception from upload_avatar_to_gcs
                raise e
            except Exception as e:
                logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

        # Only update if there's something to update
        if not update_data:
            raise HTTPException(status_code=400, detail="No fields provided to update")

        # Update the user's fields
        await user_ref.update(update_data)

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = updated_user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                updated_user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

        updated_user = User(**updated_user_data)

        logger.info(f"Updated user {user_id} with fields: {list(update_data.keys())}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update user: {str(e)}")

async def add_user_address(user_id: str, address: Address, db_client: AsyncClient) -> User:
    """
    Add a new address to a user's addresses.

    Args:
        user_id: The ID of the user to update
        address: The Address object to add
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error adding the address
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()
        current_addresses = user_data.get("addresses", [])

        # Convert Address object to dictionary for Firestore
        address_dict = address.model_dump()

        # If address doesn't have an ID, generate one
        if not address_dict.get("id"):
            address_dict["id"] = f"address_{len(current_addresses) + 1}"

        # Validate address with Shippo
        await validate_address_with_shippo(address)

        # Add the new address to the list
        current_addresses.append(address_dict)

        # Update the user's addresses field
        await user_ref.update({"addresses": current_addresses})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added address with ID {address_dict['id']} to user {user_id}")
        return updated_user
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
        await user_ref.update({
            "pointsBalance": firestore.Increment(points)
        })

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

async def add_points_and_update_cash_recharged(user_id: str, points: int, amount_dollars: float, db_client: AsyncClient) -> User:
    """
    Add points to a user's pointsBalance and update totalCashRecharged.

    Args:
        user_id: The ID of the user to update
        points: The number of points to add (must be greater than 0)
        amount_dollars: The amount of cash recharged in dollars
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

        # Validate amount
        if amount_dollars <= 0:
            raise HTTPException(status_code=400, detail="Amount must be greater than 0")

        # Convert amount_dollars to int for totalCashRecharged
        amount_int = int(amount_dollars)

        # Update the user's pointsBalance and totalCashRecharged
        await user_ref.update({
            "pointsBalance": firestore.Increment(points),
            "totalCashRecharged": firestore.Increment(amount_int)
        })

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added {points} points to user {user_id}. New balance: {updated_user.pointsBalance}")
        logger.info(f"Updated totalCashRecharged for user {user_id} by ${amount_int}. New total: {updated_user.totalCashRecharged}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update user: {str(e)}")

async def update_seed(user_id: str, db_client: AsyncClient) -> User:
    """
    Update a user's clientSeed with a new random value.

    Args:
        user_id: The ID of the user to update
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

        # Generate a new clientSeed
        new_seed = secrets.token_hex(16)

        # Update the user's clientSeed
        await user_ref.update({
            "clientSeed": new_seed
        })

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Updated clientSeed for user {user_id}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating clientSeed for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update clientSeed: {str(e)}")

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

        # No need to generate month keys anymore

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

        # Validate addresses with Shippo if any are provided
        if request.addresses:
            for address in request.addresses:
                await validate_address_with_shippo(address)

        # Convert Address objects to dictionaries for Firestore
        addresses = [address.model_dump() for address in request.addresses]

        # Generate clientSeed
        clientSeed = secrets.token_hex(16)

        # Create user data
        user_data = {
            "createdAt": now,
            "displayName": request.displayName,
            "email": request.email,
            "addresses": addresses,
            "avatar": avatar_url,
            "level": 1,
            "pointsBalance": 0,
            "totalCashRecharged": 0,
            "totalPointsSpent": 0,
            "totalFusion": request.totalFusion,
            "clientSeed": clientSeed
        }

        # Create user document in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        await user_ref.set(user_data)

        # Generate a random 6-8 character referral code (including numbers and letters)
        code_length = random.randint(6, 8)
        refer_code = ''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', k=code_length))

        # Create a document in the refer_codes collection with the referral code as the document ID
        refer_code_data = {
            "user": user_id,
            "referer_id": user_id
        }

        # Create refer_codes document in Firestore
        refer_code_ref = db_client.collection('refer_codes').document(refer_code)
        await refer_code_ref.set(refer_code_data)
        logger.info(f"Created referral code {refer_code} for user {user_id}")

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

        # Increment the user's totalFusion counter
        await user_ref.update({"totalFusion": firestore.Increment(1)})

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

async def withdraw_ship_multiple_cards(user_id: str, cards_to_withdraw: List[Dict[str, Any]], address_id: str, phone_number: str, db_client: AsyncClient) -> List[UserCard]:
    """
    Withdraw or ship multiple cards from a user's collection by creating a single withdraw request.
    The withdraw request contains fields for request date and status, and a subcollection "cards" 
    that contains all withdrawn cards.
    For each card, if quantity is less than the card's quantity, only move the specified quantity.
    Only remove a card from the original subcollection if the remaining quantity is 0.

    This function also creates a shipment using the Shippo API and stores the shipping information
    in the withdraw request document.

    Args:
        user_id: The ID of the user who owns the cards
        cards_to_withdraw: List of dictionaries containing card_id, quantity, and subcollection_name for each card to withdraw
        address_id: The ID of the address to ship the cards to
        phone_number: The phone number of the recipient for shipping purposes
        db_client: Firestore client

    Returns:
        List of the updated withdrawn cards as UserCard objects

    Raises:
        HTTPException: If there's an error withdrawing the cards
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get user data to find the address
        user_data = user_doc.to_dict()
        user_addresses = user_data.get('addresses', [])

        # Find the address with the given ID
        shipping_address = None
        for address in user_addresses:
            if address.get('id') == address_id:
                shipping_address = address
                break

        if not shipping_address:
            raise HTTPException(status_code=404, detail=f"Address with ID {address_id} not found for user {user_id}")

        # Validate cards to withdraw
        if not cards_to_withdraw:
            raise HTTPException(status_code=400, detail="No cards specified for withdrawal")

        # Create a new withdraw request
        withdraw_requests_ref = user_ref.collection('withdraw_requests')
        new_request_ref = withdraw_requests_ref.document()  # Auto-generate ID
        request_cards_ref = new_request_ref.collection('cards')

        # Prepare data for transaction
        cards_data = []
        for card_info in cards_to_withdraw:
            card_id = card_info.get('card_id')
            quantity = card_info.get('quantity', 1)
            subcollection_name = card_info.get('subcollection_name')

            if not card_id:
                raise HTTPException(status_code=400, detail="Card ID is required for each card")

            if not subcollection_name:
                raise HTTPException(status_code=400, detail=f"Subcollection name is required for card {card_id}")

            if quantity <= 0:
                raise HTTPException(status_code=400, detail=f"Quantity must be greater than 0 for card {card_id}")

            # Get the card from the source subcollection
            source_card_ref = user_ref.collection('cards').document('cards').collection(subcollection_name).document(card_id)
            source_card_doc = await source_card_ref.get()

            if not source_card_doc.exists:
                raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in subcollection {subcollection_name}")

            source_card_data = source_card_doc.to_dict()
            card = UserCard(**source_card_data)
            card_quantity = card.quantity

            if quantity > card_quantity:
                raise HTTPException(status_code=400, detail=f"Cannot withdraw/ship {quantity} of card {card_id}, only {card_quantity} available")

            # Get the main card reference and pre-fetch it
            main_card_ref = user_ref.collection('cards').document(card_id)
            main_card_doc = await main_card_ref.get()

            # Add card data to the list
            cards_data.append({
                'card_id': card_id,
                'quantity': quantity,
                'subcollection_name': subcollection_name,
                'source_card_ref': source_card_ref,
                'source_card_data': source_card_data,
                'remaining_quantity': card_quantity - quantity,
                'main_card_ref': main_card_ref,
                'main_card_doc': main_card_doc,
                'request_card_ref': request_cards_ref.document(card_id)
            })

        # Create transaction to process all cards
        @firestore.async_transactional
        async def create_withdraw_request(transaction):
            # Create the withdraw request document
            now = datetime.now()
            request_data = {
                'request_date': now,
                'status': 'pending',  # Initial status is pending
                'user_id': user_id,
                'created_at': now,
                'card_count': len(cards_data),  # Add count of cards in this request
                'shipping_address': shipping_address,  # Add shipping address
                'shipping_status': 'pending'  # Initial shipping status
            }
            transaction.set(new_request_ref, request_data)

            # Process each card
            for card_data in cards_data:
                card_id = card_data['card_id']
                quantity_to_withdraw = card_data['quantity']
                source_card_ref = card_data['source_card_ref']
                source_card_data = card_data['source_card_data']
                remaining_quantity = card_data['remaining_quantity']
                main_card_ref = card_data['main_card_ref']
                main_card_doc = card_data['main_card_doc']
                request_card_ref = card_data['request_card_ref']

                # Prepare the card data for the request cards subcollection
                request_card_data = source_card_data.copy()
                request_card_data['quantity'] = quantity_to_withdraw

                # Add the card to the request cards subcollection
                transaction.set(request_card_ref, request_card_data)

                if remaining_quantity <= 0:
                    # Delete the card from the source subcollection if quantity is 0
                    transaction.delete(source_card_ref)

                    # Also delete from the main cards collection if it exists
                    if main_card_doc.exists:
                        transaction.delete(main_card_ref)

                    subcollection_name = card_data['subcollection_name']
                    logger.info(f"Created withdraw request for all {quantity_to_withdraw} of card {card_id} from user {user_id}'s subcollection {subcollection_name}")
                else:
                    # Update the quantity in the source subcollection
                    transaction.update(source_card_ref, {"quantity": remaining_quantity})

                    # Also update in the main cards collection if it exists
                    if main_card_doc.exists:
                        transaction.update(main_card_ref, {"quantity": remaining_quantity})

                    subcollection_name = card_data['subcollection_name']
                    logger.info(f"Created withdraw request for {quantity_to_withdraw} of card {card_id} from user {user_id}'s subcollection {subcollection_name}, {remaining_quantity} remaining")

        # Execute the transaction
        transaction = db_client.transaction()
        await create_withdraw_request(transaction)

        # Get the updated cards from the withdraw request
        withdrawn_cards = []
        for card_data in cards_data:
            card_id = card_data['card_id']
            request_card_ref = card_data['request_card_ref']

            updated_request_card_doc = await request_card_ref.get()
            updated_request_card_data = updated_request_card_doc.to_dict()

            # Ensure ID is part of the data
            if 'id' not in updated_request_card_data:
                updated_request_card_data['id'] = card_id

            # Generate signed URL for the card image
            if 'image_url' in updated_request_card_data and updated_request_card_data['image_url']:
                try:
                    updated_request_card_data['image_url'] = await generate_signed_url(updated_request_card_data['image_url'])
                except Exception as sign_error:
                    logger.error(f"Failed to generate signed URL for {updated_request_card_data['image_url']}: {sign_error}")
                    # Keep the original URL if signing fails

            # Create a UserCard object from the updated request card data
            withdrawn_card = UserCard(
                card_reference=updated_request_card_data.get("card_reference", ""),
                card_name=updated_request_card_data.get("card_name", ""),
                date_got=updated_request_card_data.get("date_got"),
                id=updated_request_card_data.get("id", card_id),
                image_url=updated_request_card_data.get("image_url", ""),
                point_worth=updated_request_card_data.get("point_worth", 0),
                quantity=updated_request_card_data.get("quantity", 0),
                rarity=updated_request_card_data.get("rarity", 1)
            )

            # Add optional fields if they exist in the data
            if "expireAt" in updated_request_card_data:
                withdrawn_card.expireAt = updated_request_card_data["expireAt"]
            if "buybackexpiresAt" in updated_request_card_data:
                withdrawn_card.buybackexpiresAt = updated_request_card_data["buybackexpiresAt"]
            if "request_date" in updated_request_card_data:
                withdrawn_card.request_date = updated_request_card_data["request_date"]

            withdrawn_cards.append(withdrawn_card)

        # Now that the transaction is complete, create a shipment using Shippo API
        try:
            # Initialize the Shippo SDK with API key
            if not hasattr(settings, 'shippo_api_key') or not settings.shippo_api_key:
                logger.error("Shippo API key not configured")
                raise HTTPException(status_code=500, detail="Shipping service not configured")

            shippo_sdk = Shippo(
                api_key_header=settings.shippo_api_key
            )

            # Create a Shippo address object for the shipping address
            shippo_address = shippo_sdk.addresses.create(
                components.AddressCreateRequest(
                    name=shipping_address.get('name', ''),
                    street1=shipping_address.get('street', ''),
                    city=shipping_address.get('city', ''),
                    state=shipping_address.get('state', ''),
                    zip=shipping_address.get('zip', ''),
                    country=shipping_address.get('country', ''),
                    phone=phone_number,
                    validate=True
                )
            )

            # Create a Shippo parcel object for the package
            shippo_parcel = shippo_sdk.parcels.create(
                components.ParcelCreateRequest(
                    length="8",
                    width="6",
                    height="2",
                    distance_unit="in",
                    weight="16",
                    mass_unit="oz"
                )
            )

            # Create a Shippo shipment object
            shippo_shipment = shippo_sdk.shipments.create(
                components.ShipmentCreateRequest(
                    address_from=components.AddressCreateRequest(
                        name="Chouka Cards",
                        street1="123 Main St",
                        city="San Francisco",
                        state="CA",
                        zip="94105",
                        country="US",
                        phone="+14155559999",
                        email="support@choukacards.com"
                    ),
                    address_to=components.AddressCreateRequest(
                        name=shipping_address.get('name', ''),
                        street1=shipping_address.get('street', ''),
                        city=shipping_address.get('city', ''),
                        state=shipping_address.get('state', ''),
                        zip=shipping_address.get('zip', ''),
                        country=shipping_address.get('country', ''),
                        phone=phone_number
                    ),
                    parcels=[components.ParcelCreateRequest(
                        length="6",
                        width="4",
                        height="1",
                        distance_unit="in",
                        weight="4",
                        mass_unit="oz"
                    )],
                    async_=False
                )
            )

            cheapest_rate = min(
                shippo_shipment.rates,
                key=lambda r: float(r.amount)
            )

            # Create a Shippo transaction (purchase a label)
            shippo_transaction = shippo_sdk.transactions.create(
                components.TransactionCreateRequest(
                    rate=cheapest_rate.object_id,
                    label_file_type="PDF",
                    async_=False,
                    insurance_amount=100
                )
            )

            # Update the withdraw request document with the Shippo-related information
            await new_request_ref.update({
                'shippo_address_id': shippo_address.object_id,
                'shippo_parcel_id': shippo_parcel.object_id,
                'shippo_shipment_id': shippo_shipment.object_id,
                'shippo_transaction_id': shippo_transaction.object_id,
                'shippo_label_url': shippo_transaction.label_url,
                'tracking_number': shippo_transaction.tracking_number,
                'tracking_url': shippo_transaction.tracking_url_provider,
                'shipping_status': 'label_created'
            })

            logger.info(f"Successfully created shipment for withdraw request {new_request_ref.id}")
        except Exception as e:
            logger.error(f"Error creating shipment for withdraw request {new_request_ref.id}: {e}", exc_info=True)
            # Don't raise an exception here, just log the error and continue
            # The withdraw request was created successfully, but the shipment creation failed
            # The shipment can be created manually later

        logger.info(f"Successfully created withdraw request for {len(withdrawn_cards)} cards from user {user_id}'s subcollection {subcollection_name}")
        return withdrawn_cards

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating withdraw request for multiple cards for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create withdraw request: {str(e)}")

async def add_card_to_highlights(
    user_id: str,
    card_collection_id: str,
    card_id: str,
    db_client: AsyncClient
) -> UserCard:
    """
    Add a card to the user's highlights subcollection.
    This function finds the card in the user's collection and adds it to the highlights subcollection.

    Args:
        user_id: The ID of the user who owns the card
        card_collection_id: The collection ID of the card (e.g., 'pokemon')
        card_id: The ID of the card to add to highlights
        db_client: Firestore client

    Returns:
        The card that was added to highlights as a UserCard object

    Raises:
        HTTPException: If there's an error adding the card to highlights
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the card from the source collection
        source_card_ref = user_ref.collection('cards').document('cards').collection(card_collection_id).document(card_id)
        source_card_doc = await source_card_ref.get()

        if not source_card_doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in collection {card_collection_id}")

        # Get the card data
        source_card_data = source_card_doc.to_dict()

        # Set up the highlights subcollection reference
        highlights_ref = user_ref.collection('highlights').document(card_id)

        # Check if the card already exists in highlights
        highlights_doc = await highlights_ref.get()

        if highlights_doc.exists:
            # Card already exists in highlights, just return it
            highlights_data = highlights_doc.to_dict()

            # Generate signed URL for the card image
            if 'image_url' in highlights_data and highlights_data['image_url']:
                try:
                    highlights_data['image_url'] = await generate_signed_url(highlights_data['image_url'])
                except Exception as sign_error:
                    logger.error(f"Failed to generate signed URL for {highlights_data['image_url']}: {sign_error}")
                    # Keep the original URL if signing fails

            return UserCard(**highlights_data)

        # Add the card to the highlights subcollection
        await highlights_ref.set(source_card_data)

        logger.info(f"Added card {card_id} from collection {card_collection_id} to highlights for user {user_id}")

        # Generate signed URL for the card image
        if 'image_url' in source_card_data and source_card_data['image_url']:
            try:
                source_card_data['image_url'] = await generate_signed_url(source_card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {source_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Return the card that was added to highlights
        return UserCard(**source_card_data)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding card {card_id} from collection {card_collection_id} to highlights for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add card to highlights: {str(e)}")

async def delete_card_from_highlights(
    user_id: str,
    card_id: str,
    db_client: AsyncClient
) -> dict:
    """
    Delete a card from the user's highlights collection.

    Args:
        user_id: The ID of the user who owns the card
        card_id: The ID of the card to delete from highlights
        db_client: Firestore client

    Returns:
        A dictionary with a success message

    Raises:
        HTTPException: If there's an error deleting the card from highlights
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Set up the highlights reference
        highlights_ref = user_ref.collection('highlights').document(card_id)

        # Check if the card exists in highlights
        highlights_doc = await highlights_ref.get()

        if not highlights_doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {card_id} not found in highlights")

        # Get the card data before deleting it (for logging or returning)
        highlights_data = highlights_doc.to_dict()

        # Delete the card from highlights
        await highlights_ref.delete()

        logger.info(f"Deleted card {card_id} from highlights for user {user_id}")

        # Return a success message
        return {"message": f"Card {card_id} successfully deleted from highlights for user {user_id}"}

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting card {card_id} from highlights for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete card from highlights: {str(e)}")

async def withdraw_ship_card(user_id: str, card_id: str, subcollection_name: str, db_client: AsyncClient, quantity: int = 1) -> UserCard:
    """
    Withdraw a card from a user's collection by creating a withdraw request.
    The withdraw request contains fields for request date and status, and a subcollection "cards" 
    that contains all withdrawn cards.
    If quantity is less than the card's quantity, only move the specified quantity.
    Only remove the card from the original subcollection if the remaining quantity is 0.

    Args:
        user_id: The ID of the user who owns the card
        card_id: The ID of the card to withdraw
        subcollection_name: The name of the subcollection where the card is currently stored
        db_client: Firestore client
        quantity: The quantity to withdraw (default: 1)

    Returns:
        The updated withdrawn card as a UserCard object

    Raises:
        HTTPException: If there's an error withdrawing the card
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

        # Create a new withdraw request
        withdraw_requests_ref = user_ref.collection('withdraw_requests')
        new_request_ref = withdraw_requests_ref.document()  # Auto-generate ID

        # Get the cards subcollection reference for this withdraw request
        request_cards_ref = new_request_ref.collection('cards')
        request_card_ref = request_cards_ref.document(card_id)

        # Get the main card reference and pre-fetch it
        main_card_ref = user_ref.collection('cards').document(card_id)
        main_card_doc = await main_card_ref.get()

        @firestore.async_transactional
        async def create_withdraw_request(transaction, source_card_ref, request_card_ref, remaining_quantity, quantity_to_withdraw):
            # Create the withdraw request document
            now = datetime.now()
            request_data = {
                'request_date': now,
                'status': 'pending',  # Initial status is pending
                'user_id': user_id,
                'created_at': now
            }
            transaction.set(new_request_ref, request_data)

            # Prepare the card data for the request cards subcollection
            request_card_data = source_card_data.copy()
            request_card_data['quantity'] = quantity_to_withdraw

            # Add the card to the request cards subcollection
            transaction.set(request_card_ref, request_card_data)

            if remaining_quantity <= 0:
                # Delete the card from the source subcollection if quantity is 0
                transaction.delete(source_card_ref)

                # Also delete from the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.delete(main_card_ref)

                logger.info(f"Created withdraw request for all {quantity_to_withdraw} of card {card_id} from user {user_id}'s subcollection {subcollection_name}")
            else:
                # Update the quantity in the source subcollection
                transaction.update(source_card_ref, {"quantity": remaining_quantity})

                # Also update in the main cards collection if it exists
                if main_card_doc.exists:
                    transaction.update(main_card_ref, {"quantity": remaining_quantity})

                logger.info(f"Created withdraw request for {quantity_to_withdraw} of card {card_id} from user {user_id}'s subcollection {subcollection_name}, {remaining_quantity} remaining")

        # Execute the transaction
        transaction = db_client.transaction()
        await create_withdraw_request(transaction, source_card_ref, request_card_ref, remaining_quantity, quantity)

        # Get the updated card from the withdraw request
        updated_request_card_doc = await request_card_ref.get()
        updated_request_card_data = updated_request_card_doc.to_dict()

        # Ensure ID is part of the data
        if 'id' not in updated_request_card_data:
            updated_request_card_data['id'] = card_id

        # Generate signed URL for the card image
        if 'image_url' in updated_request_card_data and updated_request_card_data['image_url']:
            try:
                updated_request_card_data['image_url'] = await generate_signed_url(updated_request_card_data['image_url'])
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {updated_request_card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        # Create and return a UserCard object from the updated request card data
        withdrawn_card = UserCard(
            card_reference=updated_request_card_data.get("card_reference", ""),
            card_name=updated_request_card_data.get("card_name", ""),
            date_got=updated_request_card_data.get("date_got"),
            id=updated_request_card_data.get("id", card_id),
            image_url=updated_request_card_data.get("image_url", ""),
            point_worth=updated_request_card_data.get("point_worth", 0),
            quantity=updated_request_card_data.get("quantity", 0),
            rarity=updated_request_card_data.get("rarity", 1)
        )

        # Add optional fields if they exist in the data
        if "expireAt" in updated_request_card_data:
            withdrawn_card.expireAt = updated_request_card_data["expireAt"]
        if "buybackexpiresAt" in updated_request_card_data:
            withdrawn_card.buybackexpiresAt = updated_request_card_data["buybackexpiresAt"]
        if "request_date" in updated_request_card_data:
            withdrawn_card.request_date = updated_request_card_data["request_date"]

        # Log success message
        if remaining_quantity <= 0:
            logger.info(f"Successfully created withdraw request for all {quantity} of card {card_id} from user {user_id}'s subcollection {subcollection_name}")
        else:
            logger.info(f"Successfully created withdraw request for {quantity} of card {card_id} from user {user_id}'s subcollection {subcollection_name}, {remaining_quantity} remaining")

        return withdrawn_card

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating withdraw request for card {card_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create withdraw request: {str(e)}")

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
        if user_card.exists:
            # Card exists, update quantity
            user_card_data = user_card.to_dict()
            current_quantity = user_card_data.get('quantity', 0)

            # Update the card with incremented quantity
            transaction.update(user_card_ref, {
                'quantity': current_quantity + 1,
                # Update expiration dates only if they're newer
                'expireAt': expireAt if (expireAt and (not user_card_data.get('expireAt') or 
                                        expireAt > user_card_data.get('expireAt'))) 
                            else user_card_data.get('expireAt'),
                'buybackexpiresAt': buybackexpiresAt if (buybackexpiresAt and (not user_card_data.get('buybackexpiresAt') or 
                                                    buybackexpiresAt > user_card_data.get('buybackexpiresAt'))) 
                                else user_card_data.get('buybackexpiresAt')
            })

            # Record the updated card details
            updated_card = {**user_card_data, 'quantity': current_quantity + 1}
            cards_added.append(updated_card)

        else:
            # Card doesn't exist, create new entry
            new

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

async def check_card_missing(
    user_id: str,
    fusion_recipe_ids: List[str],
    db_client: AsyncClient
) -> CheckCardMissingResponse:
    """
    Check which cards are missing for a user to perform fusion with specified recipes.

    This function:
    1. Retrieves the fusion recipes from Firestore
    2. For each recipe, checks which ingredients the user is missing
    3. Returns detailed information about missing cards for each recipe

    Args:
        user_id: The ID of the user to check cards for
        fusion_recipe_ids: List of fusion recipe IDs to check
        db_client: Firestore client

    Returns:
        CheckCardMissingResponse with details about missing cards for each recipe

    Raises:
        HTTPException: If there's an error checking the cards
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Initialize response
        recipes_missing_cards = []

        # Process each fusion recipe
        for recipe_id in fusion_recipe_ids:
            # Get the fusion recipe
            recipe_ref = db_client.collection('fusion_recipes').document(recipe_id)
            recipe_doc = await recipe_ref.get()

            if not recipe_doc.exists:
                # Skip non-existent recipes and continue with the next one
                logger.warning(f"Fusion recipe with ID '{recipe_id}' not found")
                continue

            recipe_data = recipe_doc.to_dict()

            # Initialize recipe missing cards info
            recipe_missing_cards = FusionRecipeMissingCards(
                recipe_id=recipe_id,
                recipe_name=recipe_data.get('name'),
                result_card_name=recipe_data.get('result_card_name'),
                result_card_image=recipe_data.get('result_card_image'),
                missing_cards=[]
            )

            # Check each ingredient
            ingredients = recipe_data.get('ingredients', [])
            all_cards_available = True

            for ingredient in ingredients:
                card_collection_id = ingredient.get('card_collection_id')
                card_id = ingredient.get('card_id')
                required_quantity = ingredient.get('quantity', 1)

                # Check if the user has this card in their collection
                card_ref = user_ref.collection('cards').document('cards').collection(card_collection_id).document(card_id)
                card_doc = await card_ref.get()

                user_quantity = 0
                if card_doc.exists:
                    card_data = card_doc.to_dict()
                    user_quantity = card_data.get('quantity', 0)

                # If user doesn't have enough, add to missing cards
                if not card_doc.exists or user_quantity < required_quantity:
                    all_cards_available = False

                    # Try to get additional card info from the storage service
                    card_name = ingredient.get('card_name')
                    image_url = ingredient.get('image_url')

                    try:
                        # Only fetch card details if we don't already have them
                        if not card_name or not image_url:
                            card_details = await get_card_by_id_from_service(card_id, card_collection_id)
                            card_name = card_details.get('name', card_name)
                            image_url = card_details.get('image_url', image_url)
                    except Exception as e:
                        logger.warning(f"Could not fetch card details for {card_collection_id}/{card_id}: {e}")

                    missing_card = MissingCard(
                        card_collection_id=card_collection_id,
                        card_id=card_id,
                        required_quantity=required_quantity,
                        user_quantity=user_quantity,
                        card_name=card_name,
                        image_url=image_url
                    )
                    recipe_missing_cards.missing_cards.append(missing_card)

            # Update has_all_cards flag
            recipe_missing_cards.has_all_cards = all_cards_available

            # Add to response
            recipes_missing_cards.append(recipe_missing_cards)

        return CheckCardMissingResponse(recipes=recipes_missing_cards)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error checking missing cards for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to check missing cards: {str(e)}")

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

async def get_weekly_spending_rank(db_client: AsyncClient, week_id: Optional[str] = None, limit: int = 100) -> List[RankEntry]:
    """
    Get the top users by weekly spending for a specific week.

    Args:
        db_client: Firestore async client
        week_id: The week ID in the format 'YYYY-MM-DD' (default: current week)
        limit: The maximum number of users to return (default: 100)

    Returns:
        List[RankEntry]: A list of RankEntry objects containing user_id and spent amount

    Raises:
        HTTPException: If there's an error getting the weekly spending rank
    """
    try:
        # If week_id is not provided, calculate the current week's ID
        if not week_id:
            today = datetime.now()
            start_of_week = today - timedelta(days=today.weekday())
            week_id = start_of_week.strftime("%Y-%m-%d")

        # Reference to the weekly_spent collection for the specified week
        weekly_spent_ref = db_client.collection('weekly_spent').document('weekly_spent').collection(week_id)

        # Query the collection, order by 'spent' in descending order, and limit to the specified number
        query = weekly_spent_ref.order_by('spent', direction=firestore.Query.DESCENDING).limit(limit)

        # Execute the query
        docs = await query.get()

        # Convert the documents to RankEntry objects
        result = []
        for doc in docs:
            data = doc.to_dict()
            spent = data.get('spent', 0)
            result.append(RankEntry(user_id=doc.id, spent=spent))

        return result
    except Exception as e:
        logger.error(f"Error getting weekly spending rank for week {week_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get weekly spending rank: {str(e)}")


async def update_user_avatar(user_id: str, avatar: bytes, content_type: str, db_client: AsyncClient) -> User:
    """
    Update a user's avatar.

    Args:
        user_id: The ID of the user to update
        avatar: Binary image data for user's avatar
        content_type: The content type of the image (e.g., "image/jpeg")
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

        # Handle avatar upload
        try:
            # Upload avatar to GCS
            avatar_gcs_uri = await upload_avatar_to_gcs(avatar, user_id, content_type)

            # Update the user's avatar field
            await user_ref.update({"avatar": avatar_gcs_uri})
        except HTTPException as e:
            # Re-raise the exception from upload_avatar_to_gcs
            raise e
        except Exception as e:
            logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = updated_user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                updated_user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

        updated_user = User(**updated_user_data)

        logger.info(f"Updated avatar for user {user_id}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating avatar for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update avatar: {str(e)}")


async def get_all_withdraw_requests(user_id: str, db_client: AsyncClient) -> List[WithdrawRequest]:
    """
    Get all withdraw requests for a specific user.

    Args:
        user_id: The ID of the user to get withdraw requests for
        db_client: Firestore client

    Returns:
        List of WithdrawRequest objects for the specified user

    Raises:
        HTTPException: If there's an error getting the withdraw requests or if the user doesn't exist
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get withdraw requests for this user
        withdraw_requests_ref = user_ref.collection('withdraw_requests')
        withdraw_requests = await withdraw_requests_ref.get()

        user_withdraw_requests = []

        # Convert each withdraw request to a WithdrawRequest object
        for request in withdraw_requests:
            request_data = request.to_dict()
            withdraw_request = WithdrawRequest(
                id=request.id,
                created_at=request_data.get('created_at'),
                request_date=request_data.get('request_date'),
                status=request_data.get('status', 'pending'),
                user_id=request_data.get('user_id', user_id),
                card_count=request_data.get('card_count')
            )
            user_withdraw_requests.append(withdraw_request)

        logger.info(f"Retrieved {len(user_withdraw_requests)} withdraw requests for user {user_id}")
        return user_withdraw_requests
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting withdraw requests for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get withdraw requests for user {user_id}: {str(e)}")


async def get_withdraw_request_by_id(request_id: str, user_id: str, db_client: AsyncClient) -> WithdrawRequestDetail:
    """
    Get a specific withdraw request by ID.

    Args:
        request_id: The ID of the withdraw request to get
        user_id: The ID of the user who made the withdraw request
        db_client: Firestore client

    Returns:
        WithdrawRequestDetail object containing the withdraw request details and cards

    Raises:
        HTTPException: If there's an error getting the withdraw request
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get the withdraw request
        withdraw_request_ref = user_ref.collection('withdraw_requests').document(request_id)
        withdraw_request_doc = await withdraw_request_ref.get()

        if not withdraw_request_doc.exists:
            raise HTTPException(status_code=404, detail=f"Withdraw request with ID {request_id} not found for user {user_id}")

        # Get the withdraw request data
        withdraw_request_data = withdraw_request_doc.to_dict()

        # Get the cards in the withdraw request
        cards_ref = withdraw_request_ref.collection('cards')
        cards_docs = await cards_ref.get()

        cards = []
        for card_doc in cards_docs:
            card_data = card_doc.to_dict()
            card = UserCard(**card_data)
            cards.append(card)

        # Create and return the WithdrawRequestDetail object
        withdraw_request_detail = WithdrawRequestDetail(
            id=withdraw_request_doc.id,
            created_at=withdraw_request_data.get('created_at'),
            request_date=withdraw_request_data.get('request_date'),
            status=withdraw_request_data.get('status', 'pending'),
            user_id=withdraw_request_data.get('user_id', user_id),
            card_count=withdraw_request_data.get('card_count', len(cards)),
            cards=cards
        )

        logger.info(f"Retrieved withdraw request {request_id} for user {user_id} with {len(cards)} cards")
        return withdraw_request_detail
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting withdraw request {request_id} for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get withdraw request: {str(e)}")


async def get_user_pack_opening_history(user_id: str, page: int = 1, per_page: int = 10) -> Dict[str, Any]:
    """
    Get a user's pack opening history.

    Args:
        user_id: The ID of the user
        page: The page number (default: 1)
        per_page: The number of items per page (default: 10)

    Returns:
        A dictionary containing the pack opening history and total count
    """
    try:
        logger.info(f"Getting pack opening history for user {user_id}, page {page}, per_page {per_page}")

        # Calculate offset
        offset = (page - 1) * per_page

        # Query to get total count
        count_query = "SELECT COUNT(*) FROM pack_openings WHERE user_id = %s"

        # Query to get pack openings with pagination
        query = """
            SELECT id, user_id, pack_type, pack_count, price_points, 
                   client_seed, nonce, server_seed_hash, server_seed, random_hash, 
                   opened_at
            FROM pack_openings 
            WHERE user_id = %s
            ORDER BY opened_at DESC
            LIMIT %s OFFSET %s
        """

        # Execute queries
        with db_connection() as conn:
            cursor = conn.cursor()

            # Get total count
            cursor.execute(count_query, (user_id,))
            total_count = cursor.fetchone()[0]

            # Get pack openings
            cursor.execute(query, (user_id, per_page, offset))
            pack_openings = []

            for row in cursor.fetchall():
                pack_opening = {
                    "id": row[0],
                    "user_id": row[1],
                    "pack_type": row[2],
                    "pack_count": row[3],
                    "price_points": row[4],
                    "client_seed": row[5],
                    "nonce": row[6],
                    "server_seed_hash": row[7],
                    "server_seed": row[8],
                    "random_hash": row[9],
                    "opened_at": row[10]
                }
                pack_openings.append(pack_opening)

        logger.info(f"Found {len(pack_openings)} pack openings for user {user_id} (total: {total_count})")

        return {
            "pack_openings": pack_openings,
            "total_count": total_count
        }
    except Exception as e:
        logger.error(f"Error getting pack opening history for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get pack opening history: {str(e)}")

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
