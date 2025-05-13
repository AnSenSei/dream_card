from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
from uuid import UUID

class User(BaseModel):
    """Model for a user with all fields"""
    createdAt: datetime
    currentMonthCash: int = 0
    currentMonthKey: str
    displayName: str
    email: str
    lastMonthCash: int = 0
    lastMonthKey: str
    level: int = 1
    pointsBalance: int = 0
    totalCashRecharged: int = 0
    totalPointsSpent: int = 0

    class Config:
        from_attributes = True

class PaginationInfo(BaseModel):
    """Pagination information for list responses"""
    total_items: int
    items_per_page: int
    current_page: int
    total_pages: int

class UserCard(BaseModel):
    """Model for a card in a user's collection"""
    card_reference: str  # Reference to the original card
    card_name: str
    date_got: datetime
    id: str
    image_url: str
    point_worth: int
    quantity: int
    rarity: int
    expireAt: Optional[datetime] = None
    buybackexpiresAt: Optional[datetime] = None

    class Config:
        from_attributes = True

class AppliedFilters(BaseModel):
    """Filters applied to a card list query"""
    sort_by: str
    sort_order: str
    search_query: Optional[str] = None

class UserCardListResponse(BaseModel):
    """Response model for listing user cards by subcollection"""
    subcollection_name: str
    cards: List[UserCard]
    pagination: PaginationInfo
    filters: AppliedFilters

class UserCardsResponse(BaseModel):
    """Response model for listing all user cards grouped by subcollection"""
    subcollections: List[UserCardListResponse]

class UserListResponse(BaseModel):
    """Response model for listing users"""
    items: List[User]
    pagination: PaginationInfo
