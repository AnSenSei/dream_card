from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
from uuid import UUID

class DrawingBase(BaseModel):
    """Base model for drawing data"""
    title: str
    description: Optional[str] = None
    tags: List[str] = []

class DrawingCreate(DrawingBase):
    """Model for creating a new drawing"""
    pass

class Drawing(DrawingBase):
    """Model for a drawing with all fields"""
    id: UUID
    image_url: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    user_id: Optional[str] = None
    metadata: Optional[Dict] = None

    class Config:
        from_attributes = True

class PaginationInfo(BaseModel):
    """Pagination information for list responses"""
    total_items: int
    items_per_page: int
    current_page: int
    total_pages: int

class DrawingListResponse(BaseModel):
    """Response model for listing drawings"""
    items: List[Drawing]
    pagination: PaginationInfo