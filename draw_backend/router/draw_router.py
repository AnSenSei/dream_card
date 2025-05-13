from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from typing import List, Optional
from uuid import UUID, uuid4
import json

from draw_backend.config import get_logger
from draw_backend.models.schemas import Drawing, DrawingListResponse
from draw_backend.service import draw_service

# Initialize router
router = APIRouter(
    prefix="/drawings",
    tags=["drawings"],
    responses={404: {"description": "Not found"}},
)

# Initialize logger
logger = get_logger(__name__)

@router.post("/", response_model=Drawing)
async def create_drawing(
    title: str = Form(...),
    description: Optional[str] = Form(None),
    tags: str = Form("[]"),  # JSON string of tags
    file: UploadFile = File(...),
):
    """
    Create a new drawing by uploading an image.
    """
    try:
        # Parse tags from JSON string
        tags_list = json.loads(tags)

        # Read the file content
        file_content = await file.read()

        # Use the service to create the drawing
        drawing = await draw_service.create_drawing(
            title=title,
            file_content=file_content,
            filename=file.filename,
            description=description,
            tags=tags_list,
            user_id=None,  # Would be set from authentication in a real app
            metadata={}
        )

        return drawing
    except Exception as e:
        logger.error(f"Error creating drawing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error creating drawing: {str(e)}")

@router.get("/", response_model=DrawingListResponse)
async def list_drawings(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    tag: Optional[str] = None,
):
    """
    List drawings with pagination and optional filtering.
    """
    try:
        # Use the service to list drawings
        result = await draw_service.list_drawings(
            page=page,
            page_size=page_size,
            tag=tag
        )

        return result
    except Exception as e:
        logger.error(f"Error listing drawings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error listing drawings: {str(e)}")

@router.get("/{drawing_id}", response_model=Drawing)
async def get_drawing(drawing_id: UUID):
    """
    Get a specific drawing by ID.
    """
    try:
        # Use the service to get the drawing
        drawing = await draw_service.get_drawing(drawing_id)

        if drawing is None:
            logger.error(f"Drawing {drawing_id} not found")
            raise HTTPException(status_code=404, detail=f"Drawing {drawing_id} not found")

        return drawing
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting drawing {drawing_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting drawing: {str(e)}")

@router.delete("/{drawing_id}")
async def delete_drawing(drawing_id: UUID):
    """
    Delete a drawing by ID.
    """
    try:
        # Use the service to delete the drawing
        success = await draw_service.delete_drawing(drawing_id)

        if not success:
            logger.error(f"Drawing {drawing_id} not found")
            raise HTTPException(status_code=404, detail=f"Drawing {drawing_id} not found")

        return {"message": f"Drawing {drawing_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting drawing {drawing_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error deleting drawing: {str(e)}")
