from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

class Address(BaseModel):
    """Model for a user address"""
    id: Optional[str] = None  # Optional identifier like "home" or "work"
    street: str
    city: str
    state: str
    zip: str
    country: str

class User(BaseModel):
    """Model for a user with all fields"""
    createdAt: datetime
    currentMonthCash: int = 0
    currentMonthKey: str
    displayName: str
    email: str
    addresses: List[Address] = []  # Changed from allow_to_address to addresses
    avatar: Optional[str] = None  # URL or path to user's avatar image
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
    request_date: Optional[datetime] = None  # Timestamp for when card was requested for shipping

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

class UserEmailAddressUpdate(BaseModel):
    """Request model for updating user email and avatar"""
    email: str
    avatar: Optional[str] = None  # Can be base64 encoded string or binary data

class DrawnCard(BaseModel):
    """Model for a card drawn from a pack"""
    id: str
    collection_id: str
    card_reference: str
    image_url: Optional[str] = None
    card_name: Optional[str] = None
    point_worth: Optional[int] = None
    quantity: Optional[int] = None
    rarity: Optional[int] = None
    # Allow additional fields with any type
    model_config = {
        "extra": "allow"
    }

class CardReferencesRequest(BaseModel):
    """Request model for adding multiple cards to a user"""
    card_references: List[str]

class AddPointsRequest(BaseModel):
    """Request model for adding points to a user"""
    points: int = Field(..., gt=0, description="The number of points to add (must be greater than 0)")

class UserListResponse(BaseModel):
    """Response model for listing users"""
    items: List[User]
    pagination: PaginationInfo
