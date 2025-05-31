from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

from models.schemas import CardListing, PaginationInfo, AppliedFilters




class PaginatedListingsResponse(BaseModel):
    """Response model for paginated listings"""
    id: str
    listings: List[CardListing]
    pagination: PaginationInfo
    filters: AppliedFilters
    next_cursor: Optional[str] = None  # Cursor for the next page
