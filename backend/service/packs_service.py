import uuid
from typing import Dict, List, Optional, Any # Ensure 'Any' is imported

from fastapi import HTTPException, UploadFile
from google.cloud import firestore, storage # firestore.ArrayUnion and firestore.ArrayRemove are part of the firestore module

from config import get_logger
from models.pack_schema import AddPackRequest, CardPack, AddCardToPackRequest
from models.schemas import StoredCardInfo
from utils.gcs_utils import generate_signed_url

from google.cloud.firestore_v1 import AsyncClient, ArrayUnion, ArrayRemove, Increment

from config import settings

# DB_PACKS import is removed as we are moving to Firestore for these functions
# from service.data import DB_PACKS 

logger = get_logger(__name__)

GCS_BUCKET_NAME = settings.PACKS_BUCKET



async def create_pack_in_firestore(
    pack_data: AddPackRequest, 
    db_client: firestore.AsyncClient, 
    storage_client: storage.Client, 
    image_file: Optional[UploadFile] = None,
) -> str:
    """
    Creates a new pack in Firestore within a collection structure.
    First creates a document for the collection_id under the 'packs' collection if it doesn't exist.
    Then creates the pack document in a subcollection named after the collection_id.
    Optionally uploads an image for the pack to Google Cloud Storage.
    Stores the GCS URI (gs://...) of the image in Firestore.

    Args:
        pack_data: The AddPackRequest model containing pack details and collection_id
        db_client: Firestore client
        storage_client: GCS client for image upload
        image_file: Optional image file for the pack

    Returns:
        str: The ID of the created pack

    Raises:
        HTTPException: If there's an error creating the pack
    """
    if not db_client:
        logger.error("Firestore client not provided to create_pack_in_firestore.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")
    if not storage_client and image_file: 
        logger.error("Storage client not provided to create_pack_in_firestore, but an image was supplied.")
        raise HTTPException(status_code=500, detail="Cloud Storage service not configured (client missing for image upload).")

    pack_name = pack_data.pack_name
    collection_id = pack_data.collection_id
    price = pack_data.price
    win_rate = pack_data.win_rate
    is_active = pack_data.is_active
    popularity = pack_data.popularity

    if not pack_name:
        raise HTTPException(status_code=400, detail="Pack name cannot be empty.")
    if '/' in pack_name:
        raise HTTPException(status_code=400, detail="Pack name cannot contain '/' characters.")
    if not collection_id:
        raise HTTPException(status_code=400, detail="Collection ID cannot be empty.")
    if '/' in collection_id:
        raise HTTPException(status_code=400, detail="Collection ID cannot contain '/' characters.")

    # Use pack_name as the ID for the pack, but sanitize it to ensure it's a valid document ID
    # Replace spaces and special characters with underscores
    import re
    pack_id = re.sub(r'[^\w]', '_', pack_name)
    logger.info(f"Using sanitized pack name '{pack_id}' as document ID for pack '{pack_name}'")

    image_gcs_uri_for_firestore = None
    if image_file:
        if not storage_client: 
            logger.error("Storage client is None inside image_file block.")
            raise HTTPException(status_code=500, detail="Storage client error during image processing.")
        try:
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            file_extension = image_file.filename.split('.')[-1] if '.' in image_file.filename else 'png'
            # Include collection_id in the blob path
            unique_blob_name = f"packs/{collection_id}/{pack_id}.{file_extension}"
            blob = bucket.blob(unique_blob_name)

            image_file.file.seek(0)
            blob.upload_from_file(image_file.file, content_type=image_file.content_type)

            image_gcs_uri_for_firestore = f"gs://{GCS_BUCKET_NAME}/{unique_blob_name}"
            logger.info(f"Pack image uploaded to GCS. URI: {image_gcs_uri_for_firestore}")
        except Exception as e:
            logger.error(f"Error uploading pack image to GCS: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Could not upload pack image: {str(e)}")
        finally:
            image_file.file.close()

    try:
        # First, check if the collection document exists
        collection_doc_ref = db_client.collection('packs').document(collection_id)
        collection_doc = await collection_doc_ref.get()

        # If collection document doesn't exist, create it
        if not collection_doc.exists:
            logger.info(f"Creating new collection document '{collection_id}'")
            await collection_doc_ref.set({
                'name': collection_id,
                'created_at': firestore.SERVER_TIMESTAMP
            })

        # Now create the pack in the subcollection
        subcollection_ref = collection_doc_ref.collection(collection_id)
        pack_doc_ref = subcollection_ref.document(pack_name)

        # Check if pack with this ID already exists
        pack_doc = await pack_doc_ref.get()
        if pack_doc.exists:
            logger.warning(f"Pack with ID '{pack_name}' already exists in collection '{collection_id}'")
            raise HTTPException(status_code=409, detail=f"Pack with ID '{pack_name}' already exists in collection '{collection_id}'")

        # Create pack document data
        pack_doc_data = {
            "name": pack_name,
            "id": pack_id,
            "created_at": firestore.SERVER_TIMESTAMP,
            "price": price,
            "win_rate": win_rate,
            "is_active": is_active,
            "popularity": popularity,
        }
        if image_gcs_uri_for_firestore:
            pack_doc_data["image_url"] = image_gcs_uri_for_firestore

        # Set the pack document
        await pack_doc_ref.set(pack_doc_data)
        logger.info(f"Created pack document '{pack_name}' (ID: {pack_id}) in collection '{collection_id}'. Image URI: {image_gcs_uri_for_firestore or 'None'}")

        # Create empty rarities subcollection
        # Rarities will be populated when cards are added to the pack

        return pack_id
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error creating pack in Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error creating pack in Firestore: {str(e)}")

async def get_all_packs_from_firestore(db_client: firestore.AsyncClient) -> List[CardPack]:
    """
    Fetches all packs from Firestore 'packs' collection.
    Generates signed URLs for pack images if available.
    """
    logger.info("Fetching all packs from Firestore.")
    packs_list = []
    try:
        packs_stream = db_client.collection('packs').stream()
        async for doc in packs_stream:
            pack_data = doc.to_dict()
            doc_id = doc.id
            pack_data['id'] = doc_id

            pack_name = pack_data.get('name')
            if not pack_name:
                logger.warning(f"Pack document with ID '{doc_id}' is missing a name. Using default.")
                pack_name = "Unnamed Pack"

            # Generate signed URL if GCS URI exists
            image_url = pack_data.get('image_url')
            signed_image_url = None
            if image_url and image_url.startswith('gs://'):
                signed_image_url = await generate_signed_url(image_url)
            elif image_url: # If it's not a gs:// URI, use it as is (e.g., old public URL?)
                signed_image_url = image_url
                logger.warning(f"Pack {doc_id} has non-GCS image_url: {image_url}")

            packs_list.append(CardPack(
                id=doc_id,
                name=pack_name, 
                image_url=signed_image_url, # Use signed URL
                description=pack_data.get('description'), 
                rarity_probabilities=pack_data.get('rarity_probabilities'), 
                cards_by_rarity=pack_data.get('cards_by_rarity'),
                popularity=pack_data.get('popularity', 0)
                # rarity_configurations is intentionally omitted here as per user request
            ))
        logger.info(f"Successfully fetched {len(packs_list)} packs from Firestore.")
        return packs_list
    except Exception as e:
        logger.error(f"Error fetching all packs from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve packs from database.")

async def get_packs_collection_from_firestore(collection_id: str, db_client: firestore.AsyncClient) -> List[CardPack]:
    """
    Fetches all packs from a specific collection in Firestore.
    Generates signed URLs for pack images if available.

    Args:
        collection_id: The ID of the collection to fetch packs from
        db_client: Firestore client

    Returns:
        List[CardPack]: List of packs in the collection

    Raises:
        HTTPException: If collection not found or on database error
    """
    logger.info(f"Fetching all packs from collection '{collection_id}' in Firestore.")
    packs_list = []

    try:
        # Check if the collection exists
        collection_ref = db_client.collection('packs').document(collection_id)
        collection_doc = await collection_ref.get()

        if not collection_doc.exists:
            logger.warning(f"Collection '{collection_id}' not found in Firestore.")
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")

        # Get all packs in the collection
        packs_ref = collection_ref.collection(collection_id)
        packs_stream = packs_ref.stream()

        async for pack_doc in packs_stream:
            pack_data = pack_doc.to_dict()
            pack_id = pack_doc.id
            pack_data['id'] = pack_id

            pack_name = pack_data.get('name', pack_id)

            # Generate signed URL if GCS URI exists
            image_url = pack_data.get('image_url')
            signed_image_url = None
            if image_url and image_url.startswith('gs://'):
                signed_image_url = await generate_signed_url(image_url)
            elif image_url:
                signed_image_url = image_url

            # Create CardPack object
            pack = CardPack(
                id=pack_id,
                name=pack_name,
                image_url=signed_image_url,
                win_rate=pack_data.get('win_rate'),
                popularity=pack_data.get('popularity', 0)
            )
            packs_list.append(pack)

        logger.info(f"Successfully fetched {len(packs_list)} packs from collection '{collection_id}'.")
        return packs_list

    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error fetching packs from collection '{collection_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve packs from collection '{collection_id}'.")

async def get_pack_by_id_from_firestore(
    pack_id: str, 
    db_client: firestore.AsyncClient, 
    collection_id: Optional[str] = None
) -> CardPack:
    """
    Fetches a specific pack by its ID from Firestore.
    If collection_id is provided, looks in that specific collection.
    Otherwise, searches across all collections.
    Generates a signed URL for the pack image if available.
    Includes rarity configurations from the 'rarities' subcollection.

    Args:
        pack_id: The ID of the pack to retrieve
        db_client: Firestore client
        collection_id: Optional ID of the collection containing the pack

    Returns:
        CardPack: The requested pack with all its details

    Raises:
        HTTPException: If pack not found or on database error
    """
    logger.info(f"Fetching pack by ID '{pack_id}'{f' in collection {collection_id}' if collection_id else ''} from Firestore.")

    try:
        # If collection_id is provided, directly get the pack from that collection
        if collection_id:
            doc_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(pack_id)
            doc_snapshot = await doc_ref.get()

            if not doc_snapshot.exists:
                logger.warning(f"Pack with ID '{pack_id}' not found in collection '{collection_id}'.")
                raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found in collection '{collection_id}'")

            # Get pack data and process it
            return await _process_pack_document(doc_snapshot, db_client, collection_id)

        else:
            # If no collection_id provided, need to search across all collections
            logger.info(f"No collection ID provided, searching for pack '{pack_id}' across all collections.")
            collections_ref = db_client.collection('packs')
            collections_docs = await collections_ref.list_documents()

            for collection_doc in collections_docs:
                try:
                    curr_collection_id = collection_doc.id
                    doc_ref = collection_doc.collection(curr_collection_id).document(pack_id)
                    doc_snapshot = await doc_ref.get()

                    if doc_snapshot.exists:
                        logger.info(f"Found pack '{pack_id}' in collection '{curr_collection_id}'.")
                        return await _process_pack_document(doc_snapshot, db_client, curr_collection_id)

                except Exception as e:
                    logger.error(f"Error checking collection '{collection_doc.id}' for pack '{pack_id}': {e}", exc_info=True)
                    continue

            # If we get here, the pack wasn't found in any collection
            logger.warning(f"Pack with ID '{pack_id}' not found in any collection.")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found in any collection")

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error fetching pack '{pack_id}' from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve pack '{pack_id}' from database.")

async def _process_pack_document(doc_snapshot, db_client, collection_id):
    """
    Helper function to process a pack document and create a CardPack object.
    """
    try:
        pack_data = doc_snapshot.to_dict()
        doc_id = doc_snapshot.id
        pack_data['id'] = doc_id
        pack_data['collection_id'] = collection_id

        pack_name = pack_data.get('name')
        if not pack_name:
            logger.warning(f"Pack document with ID '{doc_id}' is missing a name. Using default.")
            pack_name = "Unnamed Pack"

        # Generate signed URL if GCS URI exists
        image_url = pack_data.get('image_url')
        signed_image_url = None
        if image_url and image_url.startswith('gs://'):
            signed_image_url = await generate_signed_url(image_url)
        elif image_url: # Handle non-GCS URLs
            signed_image_url = image_url
            logger.warning(f"Pack {doc_id} has non-GCS image_url: {image_url}")

        # Fetch rarities subcollection
        rarity_configurations = {}
        rarities_col_ref = doc_snapshot.reference.collection('rarities')
        async for rarity_doc in rarities_col_ref.stream():
            rarity_configurations[rarity_doc.id] = rarity_doc.to_dict()

        logger.info(f"Fetched {len(rarity_configurations)} rarities for pack '{doc_id}' in collection '{collection_id}'.")

        return CardPack(
            id=doc_id,
            name=pack_name,
            image_url=signed_image_url, # Use signed URL
            description=pack_data.get('description'),
            rarity_probabilities=pack_data.get('rarity_probabilities'),
            cards_by_rarity=pack_data.get('cards_by_rarity'),
            popularity=pack_data.get('popularity', 0),
            rarity_configurations=rarity_configurations # Add fetched rarities
        )
    except Exception as e:
        logger.error(f"Error processing pack document: {e}", exc_info=True)
        raise

async def add_card_to_pack_rarity(
    pack_id: str,
    rarity_id: str,
    card_id: str,
    card_data: AddCardToPackRequest,
    db_client: AsyncClient
) -> bool:
    """
    Adds a card to a specific rarity in a pack.
    The card is stored as a document under /packs/{packId}/rarities/{rarityId}/cards/{cardId}
    with fields:
    - globalRef: DocumentReference pointing to the global card
    - name: Card name
    - quantity: Card quantity (updated after each draw)
    - point: Card point value (updated after each draw)
    - image_url: URL to the card image
    """
    try:
        # Check if pack exists
        pack_ref = db_client.collection('packs').document(pack_id)
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Check if rarity exists
        rarity_ref = pack_ref.collection('rarities').document(rarity_id)
        rarity_snap = await rarity_ref.get()
        if not rarity_snap.exists:
            raise HTTPException(status_code=404, detail=f"Rarity '{rarity_id}' not found in pack '{pack_id}'")

        # Create global card reference
        global_card_ref = db_client.collection('GlobalCards').document(card_id)

        # Prepare card data
        card_doc_data = {
            "globalRef": global_card_ref,
            "name": card_data.name,
            "quantity": card_data.quantity,
            "point": card_data.point,
            "image_url": card_data.image_url
        }

        # Add card to the rarity
        card_ref = rarity_ref.collection('cards').document(card_id)
        await card_ref.set(card_doc_data)

        # Optionally, update card list in rarity document if needed
        # This would depend on your specific requirements
        # We're not doing this here as cards are now stored in a subcollection

        logger.info(f"Successfully added card '{card_id}' to rarity '{rarity_id}' in pack '{pack_id}'")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding card to pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add card to pack: {str(e)}")

async def add_card_from_storage_to_pack(
    pack_id: str,
    rarity_id: str,
    card_id: str, 
    card_collection: str,
    db_client: AsyncClient
) -> bool:
    """
    Adds a card from the storage service to a specific rarity in a pack.
    Fetches card details from storage_service and creates a document in 
    /packs/{packId}/rarities/{rarityId}/cards/{cardId} with fields:
    - globalRef: DocumentReference to the global card
    - name, quantity, point, image_url: copied from the storage card
    """
    from service.storage_service import get_card_by_id

    try:
        # Check if pack exists
        pack_ref = db_client.collection('packs').document(pack_id)
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Check if rarity exists
        rarity_ref = pack_ref.collection('rarities').document(rarity_id)
        rarity_snap = await rarity_ref.get()
        if not rarity_snap.exists:
            raise HTTPException(status_code=404, detail=f"Rarity '{rarity_id}' not found in pack '{pack_id}'")

        # Fetch card data from storage service
        try:
            card_data = await get_card_by_id(card_id, collection_name=card_collection)
        except HTTPException as e:
            logger.error(f"Failed to fetch card '{card_id}' from collection '{card_collection}': {str(e)}")
            raise HTTPException(
                status_code=e.status_code, 
                detail=f"Failed to fetch card details: {e.detail}"
            )

            # Log the card collection for debugging
            logger.info(f"Fetching card from collection '{card_collection}' for pack '{pack_id}'")

        # Create global card reference
        global_card_ref = db_client.collection('GlobalCards').document(card_id)

        # Prepare card data
        card_doc_data = {
            "globalRef": global_card_ref,
            "name": card_data.card_name,
            "quantity": card_data.quantity,
            "point": card_data.point_worth,
            "image_url": card_data.image_url
        }

        # Add card to the rarity
        card_ref = rarity_ref.collection('cards').document(card_id)
        await card_ref.set(card_doc_data)

        logger.info(f"Successfully added card '{card_id}' to rarity '{rarity_id}' in pack '{pack_id}'")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding card to pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add card to pack: {str(e)}")

async def add_card_direct_to_pack(
    collection_metadata_id: str,
    document_id: str,
    pack_id: str,
    probability: float,
    db_client: AsyncClient
) -> bool:
    """
    Adds a card directly to a pack with its own probability.
    Fetches card details from storage_service using the provided document_id and collection_metadata_id.

    Args:
        collection_metadata_id: The ID of the collection metadata to use for fetching card
        document_id: The ID of the card to add
        pack_id: The ID of the pack to add the card to (can include collection_id, formatted as 'collection_id/pack_id')
        probability: The probability value for the card (0.0 to 1.0)
        db_client: Firestore client

    Returns:
        bool: True if successfully added

    Raises:
        HTTPException: If pack doesn't exist, or if card fetch fails, or other errors
    """
    if probability < 0.0 or probability > 1.0:
        raise HTTPException(status_code=400, detail="Probability must be between 0.0 and 1.0")
    from service.storage_service import get_card_by_id

    try:
        # Parse the pack_id which may include collection_id (format: 'collection_id/pack_id')
        parts = pack_id.split('/')

        if len(parts) == 2:
            # If pack_id includes collection_id (collection_id/pack_id format)
            collection_id, actual_pack_id = parts
            logger.info(f"Parsed pack_id '{pack_id}' into collection_id='{collection_id}' and pack_id='{actual_pack_id}'")

            # Construct the reference to the pack document
            pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(actual_pack_id)
        else:
            # If just a simple pack_id
            logger.warning(f"No collection_id found in pack_id '{pack_id}', using it directly as document ID")
            pack_ref = db_client.collection('packs').document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {pack_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Fetch card information from storage_service
        try:
            card_info = await get_card_by_id(document_id, collection_name=collection_metadata_id)
        except HTTPException as e:
            logger.error(f"Failed to fetch card '{document_id}' from collection '{collection_metadata_id}': {str(e)}")
            raise HTTPException(
                status_code=e.status_code, 
                detail=f"Failed to fetch card details: {e.detail}"
            )

        # Create global card reference using the actual collection path from collection_metadata_id
        # The actual path comes from the collection metadata's firestoreCollection
        from service.storage_service import get_collection_metadata

        try:
            metadata = await get_collection_metadata(collection_metadata_id)
            global_card_collection = metadata.firestoreCollection
            logger.info(f"Using metadata firestoreCollection path: '{global_card_collection}'")
        except HTTPException as e:
            # Default to collection_metadata_id if metadata not found
            global_card_collection = collection_metadata_id
            logger.warning(f"Metadata for '{collection_metadata_id}' not found, using it directly: '{global_card_collection}'")

        global_card_ref = db_client.collection(global_card_collection).document(document_id)

        # Prepare card data with probability
        card_doc_data = {
            "card_reference": global_card_ref,
            "card_name": card_info.card_name,
            "quantity": card_info.quantity,
            "point_worth": card_info.point_worth,
            "rarity":card_info.rarity,
            "probability": probability
        }

        # Add image_url if available
        if hasattr(card_info, 'image_url') and card_info.image_url:
            card_doc_data["image_url"] = card_info.image_url

        # Add card directly to the cards subcollection under the pack
        card_ref = pack_ref.collection('cards').document(document_id)
        await card_ref.set(card_doc_data)

        # Update the pack document to include this card in its cards map if needed
        # This is optional and depends on whether you want to maintain a map of cards in the pack document
        pack_data = pack_snap.to_dict()
        cards_map = pack_data.get('cards', {})

        # Add the card ID to the map with its probability
        cards_map[document_id] = probability
        await pack_ref.set({
            'cards': cards_map
        }, merge=True)

        logger.info(f"Successfully added card '{document_id}' directly to pack '{pack_id}' with probability {probability}")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding card to pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add card to pack: {str(e)}")


async def delete_card_from_pack(
    collection_metadata_id: str,
    document_id: str,
    pack_id: str,
    db_client: AsyncClient
) -> bool:
    """
    Deletes a card directly from a pack.

    Args:
        collection_metadata_id: The ID of the collection metadata for identifying the card
        document_id: The ID of the card to delete
        pack_id: The ID of the pack containing the card (can include collection_id, formatted as 'collection_id/pack_id')
        db_client: Firestore client

    Returns:
        bool: True if successfully deleted

    Raises:
        HTTPException: If pack doesn't exist, or if card doesn't exist, or other errors
    """
    try:
        # Parse the pack_id which may include collection_id (format: 'collection_id/pack_id')
        parts = pack_id.split('/')

        if len(parts) == 2:
            # If pack_id includes collection_id (collection_id/pack_id format)
            collection_id, actual_pack_id = parts
            logger.info(f"Parsed pack_id '{pack_id}' into collection_id='{collection_id}' and pack_id='{actual_pack_id}'")

            # Construct the reference to the pack document
            pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(actual_pack_id)
        else:
            # If just a simple pack_id
            logger.warning(f"No collection_id found in pack_id '{pack_id}', using it directly as document ID")
            pack_ref = db_client.collection('packs').document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {pack_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Check if card exists in the pack
        card_ref = pack_ref.collection('cards').document(document_id)
        card_snap = await card_ref.get()
        if not card_snap.exists:
            logger.error(f"Card '{document_id}' not found in pack '{pack_id}'")
            raise HTTPException(status_code=404, detail=f"Card '{document_id}' not found in pack '{pack_id}'")

        # Delete the card from the cards subcollection
        await card_ref.delete()

        # Now update the pack document to remove this card from its cards map if it exists
        pack_data = pack_snap.to_dict()
        cards_map = pack_data.get('cards', {})

        # Remove the card ID from the map if it's there
        if document_id in cards_map:
            del cards_map[document_id]
            await pack_ref.set({
                'cards': cards_map
            }, merge=True)

        logger.info(f"Successfully deleted card '{document_id}' from pack '{pack_id}'")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting card from pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete card from pack: {str(e)}")

async def update_pack_in_firestore(
    pack_id: str, 
    updates: Dict[str, Any],
    db_client: AsyncClient
) -> bool:
    pack_ref = db_client.collection('packs').document(pack_id)
    pack_snap = await pack_ref.get()
    if not pack_snap.exists:
        raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

    batch = db_client.batch()

    # Handle updates to top-level pack document fields (e.g., name, description, popularity)
    pack_level_updates = {}
    if "name" in updates: # Client might send None if they want to clear a field (if allowed)
        # Firestore behavior with None: can store as null or might be an issue depending on rules/schema.
        # Assuming for now that if 'name' key is present, we try to update it.
        # If None means "delete field", specific logic would be needed.
        # The router currently only adds to updates_dict_for_service if not None.
        pack_level_updates["name"] = updates["name"]
    if "description" in updates:
        pack_level_updates["description"] = updates["description"]
    if "popularity" in updates:
        pack_level_updates["popularity"] = updates["popularity"]

    if pack_level_updates: # If there are any top-level fields to update
        batch.update(pack_ref, pack_level_updates)
        logger.info(f"Scheduled top-level updates for pack '{pack_id}': {pack_level_updates}")

    rarities_ref = pack_ref.collection('rarities')
    card_list_field = "cards"  # Or "cardIds", ensure this matches your Firestore field name

    # 1️⃣ Update (set/overwrite) fields in rarity documents
    # The 'updates["rarities"]' is expected to be Dict[rarity_level_str, Dict_of_fields_to_set]
    # where Dict_of_fields_to_set comes from RarityDetail.data
    for level, data_to_set in updates.get("rarities", {}).items():
        if not isinstance(level, str) or not isinstance(data_to_set, dict) or not data_to_set:
            logger.warning(f"Skipping rarity update for level '{level}' due to invalid data structure or empty data.")
            continue
        rar_doc_ref = rarities_ref.document(level)
        # merge=True ensures this is an upsert, creating the rarity if it doesn't exist
        # or updating existing fields and adding new ones from data_to_set.
        batch.set(rar_doc_ref, data_to_set, merge=True) 
        logger.info(f"Scheduled set/overwrite for rarity '{level}' in pack '{pack_id}' with data: {data_to_set}")

    # 2️⃣ Add cards to a rarity's card list (atomic array union)
    for level, cards_to_add_list in updates.get("cards_to_add", {}).items():
        if not isinstance(cards_to_add_list, list) or not cards_to_add_list:
            continue
        rar_doc_ref = rarities_ref.document(level)
        batch.update(rar_doc_ref, {
            card_list_field: ArrayUnion(cards_to_add_list),
            # Synchronize cardCount if this field exists in your rarity document
            'cardCount': Increment(len(cards_to_add_list))
        })
        logger.info(f"Scheduled to add {len(cards_to_add_list)} cards to rarity '{level}' in pack '{pack_id}'.")

    # 3️⃣ Delete cards from a rarity's card list (atomic array remove)
    for level, cards_to_delete_list in updates.get("cards_to_delete", {}).items():
        if not isinstance(cards_to_delete_list, list) or not cards_to_delete_list:
            continue
        rar_doc_ref = rarities_ref.document(level)
        batch.update(rar_doc_ref, {
            card_list_field: ArrayRemove(cards_to_delete_list),
            # Synchronize cardCount if this field exists
            'cardCount': Increment(-len(cards_to_delete_list))
        })
        logger.info(f"Scheduled to delete {len(cards_to_delete_list)} cards from rarity '{level}' in pack '{pack_id}'.")

    # 4️⃣ Commit all batched writes
    try:
        await batch.commit()
        logger.info(f"Successfully committed all updates for pack '{pack_id}'.")
        return True
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update pack: {e}")

async def activate_pack_in_firestore(
    pack_id: str,
    db_client: AsyncClient
) -> bool:
    """
    Activates a pack by setting its is_active field to True.

    Args:
        pack_id: The ID of the pack to activate
        db_client: Firestore client

    Returns:
        bool: True if the pack was successfully activated

    Raises:
        HTTPException: If the pack doesn't exist or there's an error activating it
    """
    try:
        # Check if pack_id contains a slash (indicating collection_id/pack_id format)
        parts = pack_id.split('/', 1)
        if len(parts) > 1:
            # If pack_id includes collection_id (collection_id/pack_id format)
            collection_id, actual_pack_id = parts
            logger.info(f"Parsed pack_id '{pack_id}' into collection_id='{collection_id}' and pack_id='{actual_pack_id}'")

            # Construct the reference to the pack document
            pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(actual_pack_id)
        else:
            # If just a simple pack_id
            logger.warning(f"No collection_id found in pack_id '{pack_id}', using it directly as document ID")
            pack_ref = db_client.collection('packs').document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {pack_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Update the is_active field to True
        await pack_ref.update({"is_active": True})

        logger.info(f"Successfully activated pack '{pack_id}'")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error activating pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to activate pack: {str(e)}")

async def delete_pack_in_firestore(
    pack_id: str,
    db_client: AsyncClient
) -> bool:
    """
    Deletes a pack from Firestore.

    Args:
        pack_id: The ID of the pack to delete
        db_client: Firestore client

    Returns:
        bool: True if the pack was successfully deleted

    Raises:
        HTTPException: If the pack doesn't exist or there's an error deleting it
    """
    try:
        # Check if pack_id contains a slash (indicating collection_id/pack_id format)
        parts = pack_id.split('/', 1)
        if len(parts) > 1:
            # If pack_id includes collection_id (collection_id/pack_id format)
            collection_id, actual_pack_id = parts
            logger.info(f"Parsed pack_id '{pack_id}' into collection_id='{collection_id}' and pack_id='{actual_pack_id}'")

            # Construct the reference to the pack document
            pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(actual_pack_id)
        else:
            # If just a simple pack_id
            logger.warning(f"No collection_id found in pack_id '{pack_id}', using it directly as document ID")
            pack_ref = db_client.collection('packs').document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {pack_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")

        # Delete all cards in the pack's cards subcollection
        cards_collection = pack_ref.collection('cards')
        cards = await cards_collection.get()

        # Use a batch to delete all cards
        batch = db_client.batch()
        for card in cards:
            batch.delete(card.reference)

        # Delete the pack document itself
        batch.delete(pack_ref)

        # Commit the batch
        await batch.commit()

        logger.info(f"Successfully deleted pack '{pack_id}' and all its cards")
        return True
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete pack: {str(e)}")

async def get_all_cards_in_pack(
    collection_id: str,
    pack_id: str,
    db_client: AsyncClient,
    sort_by: str = "point_worth"
) -> List[StoredCardInfo]:
    """
    Retrieves all cards in a pack and sorts them by the specified field in descending order.
    Default sort is by point_worth in descending order.

    Args:
        collection_id: The ID of the collection containing the pack
        pack_id: The ID of the pack to get cards from
        db_client: Firestore client
        sort_by: Field to sort by, either "point_worth" (default) or "rarity"

    Returns:
        List[StoredCardInfo]: List of all cards in the pack, sorted by the specified field in descending order

    Raises:
        HTTPException: If the pack doesn't exist or there's an error retrieving the cards
    """
    try:
        # Construct the reference to the pack document
        pack_ref = db_client.collection('packs').document(collection_id).collection(collection_id).document(pack_id)

        # Check if pack exists
        pack_snap = await pack_ref.get()
        if not pack_snap.exists:
            logger.error(f"Pack not found: {collection_id}/{pack_id}")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found in collection '{collection_id}'")

        # Get all cards in the pack's cards subcollection
        cards_collection = pack_ref.collection('cards')
        cards = await cards_collection.get()

        # Convert the cards to StoredCardInfo objects
        card_list = []
        for card in cards:
            card_data = card.to_dict()
            card_data['id'] = card.id  # Add the document ID as the card ID

            # Generate signed URL for the image if it's a GCS URI
            if 'image_url' in card_data and card_data['image_url'] and card_data['image_url'].startswith('gs://'):
                try:
                    from utils.gcs_utils import generate_signed_url
                    card_data['image_url'] = await generate_signed_url(card_data['image_url'])
                    logger.debug(f"Generated signed URL for image: {card_data['image_url']}")
                except Exception as sign_error:
                    logger.error(f"Failed to generate signed URL for {card_data['image_url']}: {sign_error}")
                    # Keep the original URL if signing fails

            # Create a StoredCardInfo object from the card data
            # Map the fields from the card document to the StoredCardInfo model
            stored_card = StoredCardInfo(
                id=card.id,
                card_name=card_data.get('card_name', ''),
                rarity=card_data.get('rarity', 0),
                point_worth=card_data.get('point_worth', 0),
                date_got_in_stock=card_data.get('date_got_in_stock', ''),
                image_url=card_data.get('image_url', ''),
                quantity=card_data.get('quantity', 0)
            )
            card_list.append(stored_card)

        # Sort the cards by the specified field in descending order
        if sort_by.lower() == "rarity":
            card_list.sort(key=lambda x: x.rarity, reverse=True)
            logger.info(f"Sorting cards by rarity in descending order")
        else:
            # Default to point_worth
            card_list.sort(key=lambda x: x.point_worth, reverse=True)
            logger.info(f"Sorting cards by point_worth in descending order")

        logger.info(f"Successfully retrieved {len(card_list)} cards from pack '{pack_id}' in collection '{collection_id}'")
        return card_list
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving cards from pack: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve cards from pack: {str(e)}")
