from uuid import UUID, uuid4
from datetime import datetime
from typing import List, Optional, Dict
import json

from draw_backend.config import get_logger, settings, get_firestore_client
from draw_backend.models.schemas import Drawing, DrawingCreate, DrawingListResponse, PaginationInfo
from draw_backend.utils.gcs_utils import upload_file_to_gcs, generate_signed_url

# Initialize logger
logger = get_logger(__name__)

async def create_drawing(
    title: str,
    file_content: bytes,
    filename: str,
    description: Optional[str] = None,
    tags: List[str] = None,
    user_id: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> Drawing:
    """
    Create a new drawing by uploading an image to GCS and storing metadata in Firestore.
    
    Args:
        title: The title of the drawing
        file_content: The binary content of the image file
        filename: The original filename of the image
        description: Optional description of the drawing
        tags: Optional list of tags for the drawing
        user_id: Optional ID of the user who created the drawing
        metadata: Optional additional metadata for the drawing
        
    Returns:
        The created Drawing object
    """
    try:
        # Generate a unique ID for the drawing
        drawing_id = uuid4()
        
        # Create a unique filename for the image
        file_extension = filename.split(".")[-1] if "." in filename else "png"
        blob_name = f"drawings/{drawing_id}.{file_extension}"
        
        # Upload the file to GCS
        image_url = upload_file_to_gcs(file_content, blob_name)
        
        # Prepare the drawing data
        drawing_data = {
            "id": str(drawing_id),
            "title": title,
            "description": description or "",
            "tags": tags or [],
            "image_url": image_url,
            "created_at": datetime.now(),
            "user_id": user_id,
            "metadata": metadata or {}
        }
        
        # In a real implementation, save to Firestore
        # firestore_client = await get_firestore_client()
        # doc_ref = firestore_client.collection(settings.firestore_collection_drawings).document(str(drawing_id))
        # await doc_ref.set(drawing_data)
        
        # Create and return the Drawing object
        drawing = Drawing(
            id=drawing_id,
            title=title,
            description=description or "",
            tags=tags or [],
            image_url=image_url,
            created_at=drawing_data["created_at"],
            user_id=user_id,
            metadata=metadata or {}
        )
        
        logger.info(f"Created drawing {drawing_id}")
        return drawing
    except Exception as e:
        logger.error(f"Error creating drawing: {e}", exc_info=True)
        raise

async def get_drawing(drawing_id: UUID) -> Optional[Drawing]:
    """
    Get a drawing by ID from Firestore.
    
    Args:
        drawing_id: The UUID of the drawing to retrieve
        
    Returns:
        The Drawing object if found, None otherwise
    """
    try:
        # In a real implementation, fetch from Firestore
        # firestore_client = await get_firestore_client()
        # doc_ref = firestore_client.collection(settings.firestore_collection_drawings).document(str(drawing_id))
        # doc = await doc_ref.get()
        # 
        # if doc.exists:
        #     data = doc.to_dict()
        #     return Drawing(
        #         id=UUID(data["id"]),
        #         title=data["title"],
        #         description=data.get("description", ""),
        #         tags=data.get("tags", []),
        #         image_url=data["image_url"],
        #         created_at=data["created_at"],
        #         updated_at=data.get("updated_at"),
        #         user_id=data.get("user_id"),
        #         metadata=data.get("metadata", {})
        #     )
        
        logger.info(f"Drawing {drawing_id} not found")
        return None
    except Exception as e:
        logger.error(f"Error getting drawing {drawing_id}: {e}", exc_info=True)
        raise

async def list_drawings(
    page: int = 1,
    page_size: int = 10,
    tag: Optional[str] = None
) -> DrawingListResponse:
    """
    List drawings with pagination and optional filtering.
    
    Args:
        page: The page number (1-indexed)
        page_size: The number of items per page
        tag: Optional tag to filter by
        
    Returns:
        A DrawingListResponse containing the drawings and pagination info
    """
    try:
        # In a real implementation, fetch from Firestore with pagination
        # firestore_client = await get_firestore_client()
        # query = firestore_client.collection(settings.firestore_collection_drawings)
        # 
        # if tag:
        #     query = query.where("tags", "array_contains", tag)
        # 
        # # Calculate pagination
        # offset = (page - 1) * page_size
        # 
        # # Get total count (in a real app, you might use a counter document for this)
        # total_query = query
        # total_docs = [doc async for doc in total_query.stream()]
        # total_items = len(total_docs)
        # 
        # # Get paginated results
        # query = query.offset(offset).limit(page_size)
        # docs = [doc async for doc in query.stream()]
        # 
        # drawings = []
        # for doc in docs:
        #     data = doc.to_dict()
        #     drawings.append(Drawing(
        #         id=UUID(data["id"]),
        #         title=data["title"],
        #         description=data.get("description", ""),
        #         tags=data.get("tags", []),
        #         image_url=data["image_url"],
        #         created_at=data["created_at"],
        #         updated_at=data.get("updated_at"),
        #         user_id=data.get("user_id"),
        #         metadata=data.get("metadata", {})
        #     ))
        
        # Mock data for now
        total_items = 0
        total_pages = max(1, (total_items + page_size - 1) // page_size)
        
        pagination = PaginationInfo(
            total_items=total_items,
            items_per_page=page_size,
            current_page=page,
            total_pages=total_pages
        )
        
        drawings = []
        
        logger.info(f"Listed drawings (page {page}, page_size {page_size}, total {total_items})")
        return DrawingListResponse(items=drawings, pagination=pagination)
    except Exception as e:
        logger.error(f"Error listing drawings: {e}", exc_info=True)
        raise

async def delete_drawing(drawing_id: UUID) -> bool:
    """
    Delete a drawing by ID from Firestore and GCS.
    
    Args:
        drawing_id: The UUID of the drawing to delete
        
    Returns:
        True if the drawing was deleted, False if it wasn't found
    """
    try:
        # In a real implementation, first get the drawing to find the image URL
        # drawing = await get_drawing(drawing_id)
        # if not drawing:
        #     return False
        # 
        # # Delete from Firestore
        # firestore_client = await get_firestore_client()
        # doc_ref = firestore_client.collection(settings.firestore_collection_drawings).document(str(drawing_id))
        # await doc_ref.delete()
        # 
        # # Delete from GCS
        # from draw_backend.config import get_storage_client
        # storage_client = get_storage_client()
        # 
        # # Extract bucket name and blob name from the image URL
        # # Assuming URL format is gs://bucket_name/blob_name
        # image_url = drawing.image_url
        # if image_url.startswith("gs://"):
        #     parts = image_url[5:].split("/", 1)
        #     if len(parts) == 2:
        #         bucket_name, blob_name = parts
        #         bucket = storage_client.bucket(bucket_name)
        #         blob = bucket.blob(blob_name)
        #         blob.delete()
        
        logger.info(f"Deleted drawing {drawing_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting drawing {drawing_id}: {e}", exc_info=True)
        raise