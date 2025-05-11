import uuid
from typing import Dict, List, Optional, Any # Ensure 'Any' is imported

from fastapi import HTTPException, UploadFile
from google.cloud import firestore, storage # firestore.ArrayUnion and firestore.ArrayRemove are part of the firestore module

from config import get_logger
from models.pack_schema import AddPackRequest, CardPack
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
    image_file: Optional[UploadFile] = None
) -> str:
    """
    Creates a new pack in Firestore using the pack_name as the document ID.
    Optionally uploads an image for the pack to Google Cloud Storage.
    Stores the GCS URI (gs://...) of the image in Firestore.
    Uses provided Firestore and Storage clients.
    THIS IS AN ASYNC FUNCTION.
    """
    if not db_client:
        logger.error("Firestore client not provided to create_pack_in_firestore.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")
    if not storage_client and image_file: 
        logger.error("Storage client not provided to create_pack_in_firestore, but an image was supplied.")
        raise HTTPException(status_code=500, detail="Cloud Storage service not configured (client missing for image upload).")

    pack_id = pack_data.pack_name

    if not pack_id:
        raise HTTPException(status_code=400, detail="Pack name cannot be empty and is used as the Pack ID.")
    if '/' in pack_id:
        raise HTTPException(status_code=400, detail="Pack name cannot contain '/' characters when used as Pack ID.")

    image_gcs_uri_for_firestore = None
    if image_file:
        if not storage_client: 
            logger.error("Storage client is None inside image_file block.")
            raise HTTPException(status_code=500, detail="Storage client error during image processing.")
        try:
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            file_extension = image_file.filename.split('.')[-1] if '.' in image_file.filename else 'png'
            unique_blob_name = f"{uuid.uuid4()}.{file_extension}"
            blob = bucket.blob(unique_blob_name)
            
            image_file.file.seek(0)
            blob.upload_from_file(image_file.file, content_type=image_file.content_type)
            
            image_gcs_uri_for_firestore = f"gs://{GCS_BUCKET_NAME}/{unique_blob_name}"
            logger.info(f"Pack image '{unique_blob_name}' uploaded to GCS. URI: {image_gcs_uri_for_firestore}")
        except Exception as e:
            logger.error(f"Error uploading pack image to GCS: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Could not upload pack image: {str(e)}")
        finally:
            image_file.file.close()

    try:
        new_pack_ref = db_client.collection('packs').document(pack_id)
        doc_snapshot = await new_pack_ref.get() 
        if doc_snapshot.exists: 
            logger.warning(f"Attempt to create pack '{pack_id}' which already exists.")
            raise HTTPException(status_code=409, detail=f"Pack with ID (name) '{pack_id}' already exists.")

        pack_doc_data = {"name": pack_data.pack_name, "id": pack_id}
        if image_gcs_uri_for_firestore:
            pack_doc_data["image_url"] = image_gcs_uri_for_firestore
        
        await new_pack_ref.set(pack_doc_data) 
        logger.info(f"Created pack document '{pack_id}'. Image URI: {image_gcs_uri_for_firestore or 'None'}")

        rarities_collection_ref = new_pack_ref.collection('rarities')
        for rarity_level, rarity_detail_model in pack_data.rarities_config.items():
            rarity_doc_ref = rarities_collection_ref.document(rarity_level)
            await rarity_doc_ref.set(rarity_detail_model.data) 
            logger.info(f"Added rarity '{rarity_level}' to pack '{pack_id}'.")
        return pack_id
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
                cards_by_rarity=pack_data.get('cards_by_rarity') 
                # rarity_configurations is intentionally omitted here as per user request
            ))
        logger.info(f"Successfully fetched {len(packs_list)} packs from Firestore.")
        return packs_list
    except Exception as e:
        logger.error(f"Error fetching all packs from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve packs from database.")

async def get_pack_by_id_from_firestore(pack_id: str, db_client: firestore.AsyncClient) -> CardPack:
    """
    Fetches a specific pack by its ID from Firestore 'packs' collection.
    Generates a signed URL for the pack image if available.
    Includes rarity configurations from the 'rarities' subcollection.
    """
    logger.info(f"Fetching pack by ID '{pack_id}' from Firestore.")
    try:
        doc_ref = db_client.collection('packs').document(pack_id)
        doc_snapshot = await doc_ref.get()

        if not doc_snapshot.exists:
            logger.warning(f"Pack with ID '{pack_id}' not found in Firestore.")
            raise HTTPException(status_code=404, detail=f"Pack '{pack_id}' not found")
        
        pack_data = doc_snapshot.to_dict()
        doc_id = doc_snapshot.id
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
        elif image_url: # Handle non-GCS URLs
            signed_image_url = image_url
            logger.warning(f"Pack {doc_id} has non-GCS image_url: {image_url}")

        # Fetch rarities subcollection
        rarity_configurations = {}
        rarities_col_ref = doc_ref.collection('rarities')
        async for rarity_doc in rarities_col_ref.stream():
            rarity_configurations[rarity_doc.id] = rarity_doc.to_dict()
        
        logger.info(f"Fetched {len(rarity_configurations)} rarities for pack '{pack_id}'.")

        return CardPack(
            id=doc_id,
            name=pack_name,
            image_url=signed_image_url, # Use signed URL
            description=pack_data.get('description'),
            rarity_probabilities=pack_data.get('rarity_probabilities'),
            cards_by_rarity=pack_data.get('cards_by_rarity'),
            rarity_configurations=rarity_configurations # Add fetched rarities
        )
    except HTTPException as e:
        raise e 
    except Exception as e:
        logger.error(f"Error fetching pack '{pack_id}' from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve pack '{pack_id}' from database.")

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
    
    # Handle updates to top-level pack document fields (e.g., name, description)
    pack_level_updates = {}
    if "name" in updates: # Client might send None if they want to clear a field (if allowed)
        # Firestore behavior with None: can store as null or might be an issue depending on rules/schema.
        # Assuming for now that if 'name' key is present, we try to update it.
        # If None means "delete field", specific logic would be needed.
        # The router currently only adds to updates_dict_for_service if not None.
        pack_level_updates["name"] = updates["name"]
    if "description" in updates:
        pack_level_updates["description"] = updates["description"]

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