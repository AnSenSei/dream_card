from fastapi import HTTPException, UploadFile
import os
from typing import Dict # Keep for potential future use if DB_STORED_CARDS or similar is ever reinstated for other purposes
from uuid import uuid4
# from datetime import timedelta # No longer needed here
# import google.auth.transport.requests # No longer needed here
# from google.auth import compute_engine # No longer needed here
# from google.oauth2 import service_account # No longer needed here

from models.schemas import StoredCardInfo, PaginationInfo, AppliedFilters, CardListResponse, CollectionMetadata
from config import get_logger, settings, get_storage_client, get_firestore_client
from utils.gcs_utils import generate_signed_url # Import the utility function

# New imports
import math
from google.cloud import firestore # For firestore.Query constants
from pydantic import BaseModel
from typing import List, Optional

logger = get_logger(__name__)

# --- Pydantic models for API response ---
# class PaginationInfo(BaseModel):
#     total_items: int
#     total_pages: int
#     current_page: int
#     per_page: int

# class AppliedFilters(BaseModel):
#     sort_by: str
#     sort_order: str
#     # In the future, could add:
#     # available_sort_options: List[str] = ["point_worth", "card_name", "date_got_in_stock", "quantity"]
#     # search_query: Optional[str] = None

# class CardListResponse(BaseModel):
#     cards: List[StoredCardInfo]
#     pagination: PaginationInfo
#     filters: AppliedFilters

# --- Real Google Cloud Storage and Firestore interactions ---

async def upload_image_to_gcs(
    file: UploadFile, 
    destination_blob_name: str
) -> str:
    """
    Uploads an image to Google Cloud Storage.
    Returns the GCS URI (gs://...) of the uploaded file.
    """
    storage_client = get_storage_client()
    bucket_name = settings.gcs_bucket_name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    logger.info(f"Attempting to upload {file.filename} to GCS bucket {bucket_name} as {destination_blob_name}")
    logger.info(f"Full GCS path will be: gs://{bucket_name}/{destination_blob_name}")

    try:
        contents = await file.read() # Read the contents of the UploadFile
        blob.upload_from_string(
            contents, 
            content_type=file.content_type
        )

        # Construct GCS URI. For public URL, bucket/object needs to be public or signed URL used.
        gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
        logger.info(f"File {file.filename} uploaded to {gcs_uri}")
        logger.info(f"Final GCS URI: {gcs_uri}")
        # Consider returning blob.public_url if the bucket/object is configured for public access
        return gcs_uri 
    except Exception as e:
        logger.error(f"Failed to upload file {file.filename} to GCS: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not upload image: {str(e)}")
    finally:
        await file.close() # Ensure the file is closed


async def save_card_information(card_info: StoredCardInfo, collection_name: str | None = None) -> StoredCardInfo:
    """
    Saves the card information to Firestore, using card_info.card_name as the document ID.
    Checks if a document with this ID (card_name) already exists before saving.
    Returns the saved card information.
    If collection_name is provided, it saves to that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.
    """
    firestore_client = get_firestore_client()
    # The collection_name will be determined by the settings or passed if this function is called from elsewhere with a specific collection.
    # For process_new_card_submission, it uses settings.firestore_collection_cards.
    # This function itself should not default the collection_name, but expect it to be resolved before or passed in.
    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    # Document ID is now the card_name from the StoredCardInfo instance
    # The StoredCardInfo instance's 'id' field should already be populated with card_name by the caller (e.g., process_new_card_submission)
    doc_id = card_info.id # This is card_name
    if not doc_id or not doc_id.strip():
        # This case should ideally be caught before, e.g., in process_new_card_submission
        raise HTTPException(status_code=400, detail="Card name (for use as ID) cannot be empty.")

    doc_ref = firestore_client.collection(effective_collection_name).document(doc_id)

    try:
        # Check if document already exists
        existing_doc = await doc_ref.get()
        if existing_doc.exists:
            logger.warning(f"Attempted to create a card with existing name (ID): '{doc_id}' in collection '{effective_collection_name}'.")
            raise HTTPException(status_code=409, detail=f"A card with the name '{doc_id}' already exists.")

        # Prepare data for Firestore: StoredCardInfo model fields.
        # The 'id' field (which is card_name) will be part of the document data.
        card_data_to_save = card_info.model_dump()

        await doc_ref.set(card_data_to_save)

        logger.info(f"Card information for '{card_info.card_name}' (ID: {doc_id}) saved to Firestore collection '{effective_collection_name}'.")
        return card_info # Return the original card_info object as it was successfully saved
    except HTTPException as http_exc: # Re-raise HTTPException
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to save card information for '{card_info.card_name}' (ID: {doc_id}) to Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not save card information: {str(e)}")


async def process_new_card_submission(
    image_file: UploadFile,
    card_name: str,
    rarity: str,
    point_worth: int,
    date_got_in_stock: str,
    quantity: int = 0,
    collection_metadata_id: str | None = None
) -> StoredCardInfo:
    """
    Orchestrates uploading the image to GCS and then saving the card data to Firestore.
    The card's name will be used as its document ID. Checks for pre-existing card name.
    If collection_metadata_id is provided, it gets the collection metadata and uses
    the firestoreCollection and storagePrefix from it. Otherwise defaults
    to settings.firestore_collection_cards.

    If collection_metadata_id is provided, it will look up the metadata for that collection
    in the metadata collection and use the storagePrefix for the image path and
    the firestoreCollection for the Firestore collection.
    """
    if not image_file.filename:
        raise HTTPException(status_code=400, detail="Image file name is missing.")

    if not card_name or not card_name.strip():
        raise HTTPException(status_code=400, detail="Card name cannot be empty.")

    # Sanitize card_name slightly for use as document ID (e.g., replace slashes if necessary)
    # For now, we'll assume card_name is valid or Firestore handles it.
    # A more robust solution might involve slugifying or more aggressive sanitization.
    # Simple example: doc_id_card_name = card_name.replace("/", "_")
    # However, StoredCardInfo.id should still reflect the original card_name for data integrity.
    # Let's proceed with using card_name directly as ID, assuming it's valid.

    try:
        # If collection_metadata_id is provided, look up the metadata for that collection
        effective_collection_name = settings.firestore_collection_cards
        storage_prefix = "cards"
        logger.info(f"Default storage_prefix set to: '{storage_prefix}'")

        logger.info(f"Collection metadata ID received: '{collection_metadata_id}'")

        # Handle both None and empty string cases
        if collection_metadata_id and collection_metadata_id.strip():
            try:
                # Try to get the metadata for the collection
                logger.info(f"Looking up metadata for collection: '{collection_metadata_id}'")
                metadata = await get_collection_metadata(collection_metadata_id)

                effective_collection_name = metadata.firestoreCollection
                storage_prefix = metadata.storagePrefix
                logger.info(f"Using metadata for collection '{collection_metadata_id}': firestoreCollection='{effective_collection_name}', storagePrefix='{storage_prefix}'")
            except HTTPException as e:
                if e.status_code == 404:
                    # If metadata not found, use the provided collection_metadata_id as is
                    logger.warning(f"Metadata for collection '{collection_metadata_id}' not found. Using provided collection_metadata_id as is.")
                    effective_collection_name = collection_metadata_id
                    # Keep the default storage_prefix as "cards"
                    logger.warning(f"Keeping default storage_prefix as '{storage_prefix}'")
                else:
                    # For other HTTP exceptions, re-raise
                    logger.error(f"Error retrieving collection metadata: {e.detail}")
                    raise e
        else:
            logger.warning(f"No collection metadata ID provided or it was empty. Using default collection: '{effective_collection_name}'")

        file_extension = os.path.splitext(image_file.filename)[1]
        # Use the storage_prefix from the collection metadata for the path
        unique_filename = f"{storage_prefix}/{str(uuid4())}{file_extension}" # Image filename remains unique with UUID
        logger.info(f"Generated unique filename: {unique_filename} with storage_prefix: {storage_prefix}")

        image_url = await upload_image_to_gcs(
            file=image_file,
            destination_blob_name=unique_filename
        )

        # Check if a card with this name already exists in the specified collection
        firestore_client = get_firestore_client()
        doc_ref = firestore_client.collection(effective_collection_name).document(card_name)
        doc_snapshot = await doc_ref.get()

        if doc_snapshot.exists:
            logger.error(f"A card with the name '{card_name}' already exists in collection '{effective_collection_name}'.")
            raise HTTPException(
                status_code=409,  # 409 Conflict is more appropriate than 500
                detail=f"A card with the name '{card_name}' already exists in collection '{effective_collection_name}'."
            )

        # Generate a signed URL for the image
        try:
            signed_image_url = await generate_signed_url(image_url)
            logger.info(f"Generated signed URL for newly uploaded image")
        except Exception as sign_error:
            logger.warning(f"Failed to generate signed URL for {image_url}: {sign_error}")
            signed_image_url = image_url  # Fall back to original URL

        card_data = StoredCardInfo(
            id=card_name,  # Use card_name as the ID for the StoredCardInfo model
            card_name=card_name,
            rarity=rarity,
            point_worth=point_worth,
            date_got_in_stock=date_got_in_stock,
            image_url=signed_image_url,  # Use the signed URL if available
            quantity=quantity
        )

        # save_card_information will now use card_data.card_name for doc ID and check existence
        saved_card_info = await save_card_information(card_data, effective_collection_name)
        return saved_card_info

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error processing new card submission: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

# For backward compatibility
async def process_new_card_submission_with_collection_name(
    image_file: UploadFile,
    card_name: str,
    rarity: str,
    point_worth: int,
    date_got_in_stock: str,
    quantity: int = 0,
    collection_name: str | None = None
) -> StoredCardInfo:
    """
    Backward compatibility wrapper for process_new_card_submission.
    Uses collection_name as collection_metadata_id.
    """
    return await process_new_card_submission(
        image_file=image_file,
        card_name=card_name,
        rarity=rarity,
        point_worth=point_worth,
        date_got_in_stock=date_got_in_stock,
        quantity=quantity,
        collection_metadata_id=collection_name
    )

async def get_all_stored_cards(
    collection_name: str | None = None,
    page: int = 1,
    per_page: int = 10,
    sort_by: str = "point_worth",
    sort_order: str = "desc",
    search_query: str | None = None
) -> CardListResponse:
    """
    Retrieves a paginated and sorted list of card information from Firestore.
    Allows searching by card name (prefix match).
    Generates signed URLs for images.
    If collection_name is provided, it fetches from that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.
    """
    firestore_client = get_firestore_client()

    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
        logger.info(f"collection_name not provided, defaulting to '{effective_collection_name}'.")
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    log_params = {
        "page": page, "per_page": per_page, "sort_by": sort_by, 
        "sort_order": sort_order, "collection": effective_collection_name,
        "search_query": search_query
    }
    logger.info(f"Fetching cards with parameters: {log_params}")

    cards_list = []

    try:
        query = firestore_client.collection(effective_collection_name)

        # Apply search query if provided
        if search_query and search_query.strip():
            stripped_search_query = search_query.strip()
            logger.info(f"Applying search filter for card_name: >='{stripped_search_query}' and <='{stripped_search_query}\uf8ff'")
            query = query.where("card_name", ">=", stripped_search_query)
            query = query.where("card_name", "<=", stripped_search_query + "\uf8ff")

        # Count total items matching the (potentially filtered) query
        count_agg_query = query.count() # Apply count on the same base query (with search filters)
        count_snapshot = await count_agg_query.get()
        total_items = count_snapshot[0][0].value if count_snapshot and count_snapshot[0] else 0

        if total_items == 0:
            logger.info(f"No cards found matching criteria in Firestore collection '{effective_collection_name}'.")
            return CardListResponse(
                cards=[],
                pagination=PaginationInfo(
                    total_items=0,
                    total_pages=0,
                    current_page=page,
                    per_page=per_page
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
            sort_order = "desc" # Ensure applied filter reflects actual sort

        # Apply sorting
        # Important: Firestore requires that the first orderBy() field must be the same as the inequality field if one is used.
        # If search_query is active, card_name is already part of an inequality. 
        # If sorting by card_name, it's fine. If sorting by something else, 
        # we might need to add card_name as a secondary sort, or rethink if complex sorting can be combined with this search.
        # For now, let's assume Firestore handles this, or we prioritize search results then sort.
        # If search_query is present, and sort_by is not 'card_name', Firestore might require 'card_name' to be the first .order_by()
        # This is a limitation of Firestore: If you have a filter with an inequality (<, <=, >, >=), 
        # your first ordering must be on the same field.

        query_with_filters = query # query already has search filters if any

        if search_query and search_query.strip() and sort_by != "card_name":
            # If searching and sorting by a different field, ensure card_name is the first sort key
            # then apply the requested sort_by. This may not always yield intuitive results for the secondary sort.
            # A more robust solution for multi-field sort with range/inequality search might need client-side sorting after fetching
            # a broader set, or using a more advanced search service like Algolia/Elasticsearch.
            logger.warning(f"Search query on 'card_name' is active while sorting by '{sort_by}'. Firestore requires ordering by 'card_name' first.")
            query_with_sort = query_with_filters.order_by("card_name").order_by(sort_by, direction=direction)
        else:
            query_with_sort = query_with_filters.order_by(sort_by, direction=direction)

        # Apply pagination
        # Ensure page and per_page are positive
        current_page_query = max(1, page)
        per_page_query = max(1, per_page)
        offset = (current_page_query - 1) * per_page_query

        paginated_query = query_with_sort.limit(per_page_query).offset(offset)

        logger.info(f"Executing Firestore query for collection '{effective_collection_name}' with pagination and sorting.")
        stream = paginated_query.stream() # stream() is an async iterator

        async for doc in stream:
            try:
                card_data = doc.to_dict()
                if not card_data:  # Skip empty documents
                    logger.warning(f"Skipping empty document with ID: {doc.id} in collection '{effective_collection_name}'.")
                    continue

                card_data['id'] = doc.id # Ensure ID is part of the data

                # Generate signed URL for the card image
                if 'image_url' in card_data and card_data['image_url']:
                    try:
                        card_data['image_url'] = await generate_signed_url(card_data['image_url'])
                        logger.debug(f"Generated signed URL for image: {card_data['image_url']}")
                    except Exception as sign_error:
                        logger.error(f"Failed to generate signed URL for {card_data['image_url']}: {sign_error}")
                        # Keep the original URL if signing fails

                cards_list.append(StoredCardInfo(**card_data))
            except Exception as e:
                logger.error(f"Error processing document {doc.id} from Firestore collection '{effective_collection_name}': {e}", exc_info=True)
                # Optionally, skip this card and continue
                continue

        total_pages = math.ceil(total_items / per_page_query) if per_page_query > 0 else 0

        pagination_info = PaginationInfo(
            total_items=total_items,
            total_pages=total_pages,
            current_page=current_page_query,
            per_page=per_page_query
        )

        applied_filters_info = AppliedFilters(
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )

        logger.info(f"Successfully fetched {len(cards_list)} cards for page {current_page_query} from Firestore collection '{effective_collection_name}'. Total items: {total_items}.")
        return CardListResponse(cards=cards_list, pagination=pagination_info, filters=applied_filters_info)

    except Exception as e:
        logger.error(f"Failed to fetch cards from Firestore collection '{effective_collection_name}': {e}", exc_info=True)
        # Consider specific error types if needed, e.g., for invalid sort_by field
        if "no matching index" in str(e).lower() or "3 INVALID_ARGUMENT" in str(e).upper(): # Firebase specific error messages
             raise HTTPException(status_code=400, detail=f"Could not retrieve cards. Likely an issue with sorting configuration (e.g., field '{sort_by}' does not exist or requires an index). Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Could not retrieve cards from database: {str(e)}")

async def update_card_quantity(document_id: str, quantity_change: int, collection_name: str | None = None) -> StoredCardInfo:
    """
    Updates the quantity of a specific card in Firestore.
    If collection_name is provided, it updates in that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.
    """
    firestore_client = get_firestore_client()

    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    doc_ref = firestore_client.collection(effective_collection_name).document(document_id)

    try:
        # Get the document reference
        doc = await doc_ref.get()

        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {document_id} not found")

        # Get current card data
        card_data = doc.to_dict()
        current_quantity = card_data.get('quantity', 0)

        # Calculate new quantity, ensure it doesn't go below 0
        new_quantity = max(0, current_quantity + quantity_change)

        # Update only the quantity field
        await doc_ref.update({'quantity': new_quantity})

        # Update the quantity in the card data and add the document ID
        card_data['quantity'] = new_quantity
        card_data['id'] = document_id

        # Generate signed URL for the image if it's a GCS URI
        if 'image_url' in card_data and card_data['image_url'].startswith('gs://'):
            card_data['image_url'] = await generate_signed_url(card_data['image_url'])

        return StoredCardInfo(**card_data)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating quantity for card {document_id} in '{effective_collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not update card quantity in collection '{effective_collection_name}'.")

async def update_card_information(document_id: str, update_data: dict, collection_name: str | None = None) -> StoredCardInfo:
    """
    Updates specific fields of a card in Firestore.
    If collection_name is provided, it updates in that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.
    """
    firestore_client = get_firestore_client()

    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    doc_ref = firestore_client.collection(effective_collection_name).document(document_id)
    logger.info(f"Attempting to update card {document_id} in collection '{effective_collection_name}' with data: {update_data}")

    try:
        # Get the document reference
        doc = await doc_ref.get()

        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Card with ID {document_id} not found")

        # Get current card data
        card_data = doc.to_dict()

        # Update only the fields that are provided
        for field, value in update_data.items():
            if value is not None:  # Only update if value is provided
                card_data[field] = value

        # Update the document in Firestore
        await doc_ref.update(update_data)

        # Add the document ID back to the data
        card_data['id'] = document_id

        # Generate signed URL for the image if it's a GCS URI
        if 'image_url' in card_data and card_data['image_url'].startswith('gs://'):
            card_data['image_url'] = await generate_signed_url(card_data['image_url'])

        return StoredCardInfo(**card_data)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating card {document_id} in '{effective_collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not update card information in '{effective_collection_name}'.")

async def clean_fusion_references(document_id: str, collection_name: str | None = None, fusion_id_to_remove: str | None = None) -> None:
    """
    Cleans up fusion references when a card is deleted or when a specific fusion recipe is deleted.
    If the card has a used_in_fusion field, it deletes all fusion recipes that use this card.
    If fusion_id_to_remove is provided, it only removes that specific fusion from the card's used_in_fusion array.

    Args:
        document_id: The ID of the card being deleted or cleaned
        collection_name: The collection name where the card is stored. If provided, it will look up
                        the metadata for that collection to get the actual Firestore collection name.
        fusion_id_to_remove: If provided, only this specific fusion ID will be removed from the card's used_in_fusion array.
    """
    firestore_client = get_firestore_client()

    # Determine the effective collection name
    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    try:
        # Get the card document to check if it has used_in_fusion field
        doc_ref = firestore_client.collection(effective_collection_name).document(document_id)
        doc = await doc_ref.get()

        if not doc.exists:
            logger.warning(f"Card {document_id} not found in collection '{effective_collection_name}' when cleaning fusion references.")
            return

        card_data = doc.to_dict()


        # Check if the card has used_in_fusion field
        if 'used_in_fusion' in card_data and card_data['used_in_fusion']:
            logger.info(f"Card {document_id} has fusion references. Cleaning up...")

            # If fusion_id_to_remove is provided, only remove that specific fusion
            if fusion_id_to_remove:
                # Filter out the fusion with the specified fusion_id
                updated_fusions = [
                    fusion for fusion in card_data['used_in_fusion'] 
                    if fusion.get('fusion_id') != fusion_id_to_remove and fusion.get('result_card_id') != fusion_id_to_remove
                ]

                # Update the card with the filtered array
                await doc_ref.update({
                    'used_in_fusion': updated_fusions
                })

                logger.info(f"Removed fusion information from card '{document_id}' (with collection_id '{effective_collection_name}') for fusion ID '{fusion_id_to_remove}'")
            else:
                # Iterate through all fusion recipes referenced in used_in_fusion
                for fusion_info in card_data['used_in_fusion']:
                    result_card_id = fusion_info.get('result_card_id')

                    if result_card_id:
                        try:
                            # Delete the fusion recipe
                            fusion_doc_ref = firestore_client.collection('fusion_recipes').document(result_card_id)
                            fusion_doc = await fusion_doc_ref.get()

                            if fusion_doc.exists:
                                # Get the recipe data to find all ingredients
                                recipe_data = fusion_doc.to_dict()

                                # Remove fusion information from all ingredient cards
                                if 'ingredients' in recipe_data and recipe_data['ingredients']:
                                    for ingredient_data in recipe_data['ingredients']:
                                        try:
                                            # Get card_id and card_collection_id from ingredient data
                                            card_id = ingredient_data.get('card_id')
                                            card_collection_id = ingredient_data.get('card_collection_id')

                                            if card_id and card_collection_id:
                                                # Skip the card being deleted
                                                if card_id == document_id and card_collection_id == effective_collection_name:
                                                    continue

                                                # Get the card document
                                                ingredient_doc_ref = firestore_client.collection(card_collection_id).document(card_id)
                                                ingredient_doc = await ingredient_doc_ref.get()

                                                if ingredient_doc.exists:
                                                    ingredient_data = ingredient_doc.to_dict()

                                                    # Remove this fusion from the used_in_fusion array
                                                    if 'used_in_fusion' in ingredient_data:
                                                        if ingredient_data['used_in_fusion']:
                                                            # Filter out the fusion with this result_card_id
                                                            updated_fusions = [
                                                                fusion for fusion in ingredient_data['used_in_fusion'] 
                                                                if fusion.get('result_card_id') != result_card_id
                                                            ]

                                                            # Update the card with the filtered array
                                                            await ingredient_doc_ref.update({
                                                                'used_in_fusion': updated_fusions
                                                            })

                                                            logger.info(f"Removed fusion information from card '{card_id}' in collection '{card_collection_id}'")
                                        except Exception as e:
                                            # Log the error but continue with other ingredients
                                            logger.error(f"Error removing fusion information from ingredient card: {e}", exc_info=True)

                                # Delete the fusion recipe
                                await fusion_doc_ref.delete()
                                logger.info(f"Deleted fusion recipe '{result_card_id}'")
                        except Exception as e:
                            # Log the error but continue with other fusion recipes
                            logger.error(f"Error deleting fusion recipe '{result_card_id}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error cleaning fusion references for card {document_id}: {e}", exc_info=True)

async def delete_card_from_firestore(document_id: str, collection_name: str | None = None) -> None:
    """
    Deletes a card document from Firestore.
    If collection_name is provided, it deletes from that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.
    """
    firestore_client = get_firestore_client()
    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    # Clean up fusion references before deleting the card
    await clean_fusion_references(document_id, effective_collection_name)

    doc_ref = firestore_client.collection(effective_collection_name).document(document_id)
    try:
        await doc_ref.delete()
        logger.info(f"Successfully deleted card {document_id} from collection '{effective_collection_name}'.")
    except Exception as e:
        logger.error(f"Error deleting card {document_id} from '{effective_collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not delete card from collection '{effective_collection_name}'.")

async def add_collection_metadata(metadata: CollectionMetadata) -> CollectionMetadata:
    """
    Adds metadata for a collection to the metadata collection.
    Uses the name field as the document ID.
    Returns the saved metadata.
    """
    firestore_client = get_firestore_client()
    meta_collection_name = settings.meta_data_collection

    # Use the name field as the document ID
    doc_id = metadata.name
    if not doc_id or not doc_id.strip():
        raise HTTPException(status_code=400, detail="Collection name (for use as ID) cannot be empty.")

    doc_ref = firestore_client.collection(meta_collection_name).document(doc_id)

    try:
        # Check if document already exists
        existing_doc = await doc_ref.get()
        if existing_doc.exists:
            logger.warning(f"Attempted to create metadata with existing name (ID): '{doc_id}' in collection '{meta_collection_name}'.")
            raise HTTPException(status_code=409, detail=f"Metadata for collection '{doc_id}' already exists.")

        # Prepare data for Firestore
        metadata_data = metadata.model_dump()

        await doc_ref.set(metadata_data)

        logger.info(f"Metadata for collection '{doc_id}' saved to Firestore collection '{meta_collection_name}'.")
        return metadata
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to save metadata for collection '{doc_id}' to Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not save collection metadata: {str(e)}")

async def get_collection_metadata(collection_name: str) -> CollectionMetadata:
    """
    Retrieves metadata for a specific collection from the metadata collection.
    Returns the metadata if found, otherwise raises a 404 error.
    """
    firestore_client = get_firestore_client()
    meta_collection_name = settings.meta_data_collection

    doc_ref = firestore_client.collection(meta_collection_name).document(collection_name)

    try:
        doc_snapshot = await doc_ref.get()

        if not doc_snapshot.exists:
            logger.warning(f"Metadata for collection '{collection_name}' not found in Firestore.")
            raise HTTPException(status_code=404, detail=f"Metadata for collection '{collection_name}' not found")

        metadata_data = doc_snapshot.to_dict()
        return CollectionMetadata(**metadata_data)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to retrieve metadata for collection '{collection_name}' from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve collection metadata: {str(e)}")

async def get_all_collection_metadata() -> List[CollectionMetadata]:
    """
    Retrieves metadata for all collections from the metadata collection.
    Returns a list of metadata objects.
    """
    firestore_client = get_firestore_client()
    meta_collection_name = settings.meta_data_collection

    try:
        collection_ref = firestore_client.collection(meta_collection_name)
        docs_stream = collection_ref.stream()

        metadata_list = []
        async for doc in docs_stream:
            metadata_data = doc.to_dict()
            metadata_list.append(CollectionMetadata(**metadata_data))

        return metadata_list
    except Exception as e:
        logger.error(f"Failed to retrieve all collection metadata from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve collection metadata: {str(e)}")

# Function to get all collections - for API naming consistency
async def get_all_collections() -> List[CollectionMetadata]:
    """
    Alias for get_all_collection_metadata.
    Returns metadata for all collections from the metadata collection.
    """
    return await get_all_collection_metadata()

async def get_card_by_id(document_id: str, collection_name: str | None = None) -> StoredCardInfo:
    """
    Retrieves all data for a specific card from Firestore by its ID.
    If collection_name is provided, it fetches from that collection, otherwise defaults
    to settings.firestore_collection_cards.

    If collection_name is provided, it will look up the metadata for that collection
    in the metadata collection and use the firestoreCollection for the Firestore collection.

    Returns:
        StoredCardInfo: Complete data for the requested card

    Raises:
        HTTPException: 404 if card not found, 500 for other errors
    """
    firestore_client = get_firestore_client()

    if not collection_name:
        effective_collection_name = settings.firestore_collection_cards
    else:
        # Try to get the metadata for the collection
        try:
            metadata = await get_collection_metadata(collection_name)
            effective_collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata for collection '{collection_name}': firestoreCollection='{effective_collection_name}'")
        except HTTPException as e:
            if e.status_code == 404:
                # If metadata not found, use the provided collection_name as is
                effective_collection_name = collection_name
                logger.warning(f"Metadata for collection '{collection_name}' not found. Using provided collection_name as is.")
            else:
                # For other HTTP exceptions, re-raise
                raise e

    doc_ref = firestore_client.collection(effective_collection_name).document(document_id)

    try:
        doc = await doc_ref.get()

        if not doc.exists:
            logger.warning(f"Card with ID {document_id} not found in collection '{effective_collection_name}'.")
            raise HTTPException(status_code=404, detail=f"Card with ID {document_id} not found")

        # Get the card data and add the document ID
        card_data = doc.to_dict()
        card_data['id'] = document_id

        # Generate signed URL for the image if it's a GCS URI
        if 'image_url' in card_data and card_data['image_url'].startswith('gs://'):
            try:
                card_data['image_url'] = await generate_signed_url(card_data['image_url'])
                logger.debug(f"Generated signed URL for image: {card_data['image_url']}")
            except Exception as sign_error:
                logger.error(f"Failed to generate signed URL for {card_data['image_url']}: {sign_error}")
                # Keep the original URL if signing fails

        return StoredCardInfo(**card_data)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error retrieving card {document_id} from '{effective_collection_name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve card data from collection '{effective_collection_name}': {str(e)}")
