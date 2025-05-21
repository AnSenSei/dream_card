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

class CreateAccountRequest(BaseModel):
    """Request model for creating a new user account"""
    email: str
    displayName: str = "AnSenSei"
    avatar: Optional[str] = None  # URL or path to user's avatar image
    addresses: List[Address] = []
    totalFusion: int = 0

class User(BaseModel):
    """Model for a user with all fields"""
    createdAt: datetime
    displayName: str
    email: str
    addresses: List[Address] = []  # Changed from allow_to_address to addresses
    avatar: Optional[str] = None  # URL or path to user's avatar image
    level: int = 1
    pointsBalance: int = 0
    totalCashRecharged: int = 0
    totalPointsSpent: int = 0
    totalFusion: int = 0  # Added new field
    clientSeed: Optional[str] = None  # Client seed for randomization

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
    locked_quantity: int = 0  # Quantity locked for listings
    expireAt: Optional[datetime] = None
    buybackexpiresAt: Optional[datetime] = None
    request_date: Optional[datetime] = None  # Timestamp for when card was requested for shipping

    class Config:
        from_attributes = True

class CardReferencesRequest(BaseModel):
    """Request model for adding cards to a user's collection"""
    card_references: List[str]

class CardWithPointsRequest(BaseModel):
    """
    Request model for adding cards to a user's collection while deducting points in a single transaction.
    """
    card_references: List[str]
    points_to_deduct: int

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
    num_draw: Optional[int] = None  # Position of the card in the drawing sequence
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

class PerformFusionRequest(BaseModel):
    """Request model for performing fusion"""
    result_card_id: str = Field(..., description="The ID of the fusion recipe to use")

class PerformFusionResponse(BaseModel):
    """Response model for fusion result"""
    success: bool
    message: str
    result_card: Optional[UserCard] = None

class RandomFusionRequest(BaseModel):
    """Request model for performing random fusion"""
    card_id1: str = Field(..., description="The ID of the first card to fuse")
    card_id2: str = Field(..., description="The ID of the second card to fuse")
    collection_id: str = Field(..., description="The collection ID of both cards")

class CardListing(BaseModel):
    """Model for a card listing"""
    owner_reference: str  # Reference to the seller user document
    card_reference: str  # Card global ID
    collection_id: str  # Collection ID of the card
    quantity: int  # Quantity being listed
    createdAt: datetime
    expiresAt: Optional[datetime] = None
    pricePoints: Optional[int] = None  # Fixed price in points
    priceCash: Optional[float] = None  # Fixed price in cash (yuan)
    highestOfferPoints: Optional[Dict[str, Any]] = None  # Highest offer in points
    highestOfferCash: Optional[Dict[str, Any]] = None  # Highest offer in cash
    image_url: Optional[str] = None  # URL of the card image

    class Config:
        from_attributes = True

class CreateCardListingRequest(BaseModel):
    """Request model for creating a card listing"""
    collection_id: str = Field(..., description="The collection ID of the card")
    card_id: str = Field(..., description="The ID of the card")
    quantity: int = Field(..., gt=0, description="The quantity to list (must be greater than 0)")
    pricePoints: Optional[int] = None
    priceCash: Optional[float] = None
    expiresAt: Optional[datetime] = None

class OfferPointsRequest(BaseModel):
    """Request model for offering points for a listing"""
    points: int = Field(..., gt=0, description="The number of points to offer (must be greater than 0)")

class OfferCashRequest(BaseModel):
    """Request model for offering cash for a listing"""
    cash: float = Field(..., gt=0, description="The amount of cash to offer (must be greater than 0)")

class UpdatePointOfferRequest(BaseModel):
    """Request model for updating a point offer for a listing"""
    points: int = Field(..., gt=0, description="The new number of points to offer (must be greater than the current offer)")

class UpdateCashOfferRequest(BaseModel):
    """Request model for updating a cash offer for a listing"""
    cash: float = Field(..., gt=0, description="The new amount of cash to offer (must be greater than the current offer)")

class AcceptOfferRequest(BaseModel):
    """Request model for accepting an offer for a listing"""
    offer_type: str = Field(..., description="The type of offer to accept (cash or point)")

class CheckCardMissingRequest(BaseModel):
    """Request model for checking missing cards for fusion recipes"""
    fusion_recipe_ids: List[str] = Field(..., description="List of fusion recipe IDs to check")

class MissingCard(BaseModel):
    """Model for a missing card required for fusion"""
    card_collection_id: str
    card_id: str
    required_quantity: int
    user_quantity: int = 0
    card_name: Optional[str] = None
    image_url: Optional[str] = None

class FusionRecipeMissingCards(BaseModel):
    """Model for missing cards for a specific fusion recipe"""
    recipe_id: str
    recipe_name: Optional[str] = None
    result_card_name: Optional[str] = None
    result_card_image: Optional[str] = None
    missing_cards: List[MissingCard]
    has_all_cards: bool = False

class CheckCardMissingResponse(BaseModel):
    """Response model for checking missing cards for fusion recipes"""
    recipes: List[FusionRecipeMissingCards]

class CardToWithdraw(BaseModel):
    """Model for a card to withdraw"""
    card_id: str = Field(..., description="The ID of the card to withdraw")
    quantity: int = Field(1, gt=0, description="The quantity to withdraw (default: 1)")

class WithdrawCardsRequest(BaseModel):
    """Request model for withdrawing multiple cards"""
    cards: List[CardToWithdraw] = Field(..., description="List of cards to withdraw")
    subcollection_name: str = Field(..., description="The name of the subcollection where the cards are stored")

class WithdrawCardsResponse(BaseModel):
    """Response model for withdrawing multiple cards"""
    cards: List[UserCard] = Field(..., description="List of withdrawn cards")

class UserListResponse(BaseModel):
    """Response model for listing users"""
    items: List[User]
    pagination: PaginationInfo

class RankEntry(BaseModel):
    """
    Represents a user's rank entry based on their weekly spending.
    The format is "user_id:spent" where spent is the amount spent by the user.
    """
    user_id: str
    spent: int

    def __str__(self) -> str:
        return f"{self.user_id}:{self.spent}"

# Note: For file uploads, we don't use a Pydantic model
# The avatar upload endpoint will use FastAPI's File and UploadFile directly
