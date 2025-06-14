from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from uuid import UUID

class Address(BaseModel):
    """Model for a user address"""
    id: Optional[str] = None  # Optional identifier like "home" or "work"
    name: str  # Name for the address (e.g., "John Smith")
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
    referred_by: Optional[str] = None  # ID of the user who referred this user
    total_point_refered: int = 0  # Total points earned through referrals
    stripe_account_id: Optional[str] = None  # Stripe Connect account ID for sellers

    class Config:
        from_attributes = True

class PaginationInfo(BaseModel):
    """Pagination information for list responses"""
    total_items: Optional[int] = None
    items_per_page: int
    current_page: Optional[int] = None  # Not used with cursor pagination
    total_pages: Optional[int] = None

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
    subcollection_name: Optional[str] = None  # The name of the subcollection where the card is stored

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
    sort_by: Optional[str] = None
    sort_order: str = "desc"
    search_query: Optional[str] = None
    collection_id: Optional[str] = None
    filter_out_accepted: bool = True

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
    email: Optional[str] = None
    avatar: Optional[Any] = None  # Can be base64 encoded string or binary data

    class Config:
        arbitrary_types_allowed = True  # Allow binary data for avatar

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

class CheckReferResponse(BaseModel):
    """Response model for checking if a user has been referred"""
    user_id: str
    is_referred: bool
    referer_id: Optional[str] = None

class ReferredUser(BaseModel):
    """Model for a user who has been referred"""
    user_id: str
    points_recharged: int
    first_recharge_at: datetime
    last_recharge_at: datetime

class GetReferralsResponse(BaseModel):
    """Response model for getting all users referred by a specific user"""
    user_id: str
    total_referred: int
    referred_users: List[ReferredUser] = []

class GetReferCodeResponse(BaseModel):
    """Response model for getting a user's referral code"""
    user_id: str
    refer_code: str

class RandomFusionRequest(BaseModel):
    """Request model for performing random fusion"""
    card_id1: str = Field(..., description="The ID of the first card to fuse")
    card_id2: str = Field(..., description="The ID of the second card to fuse")
    collection_id: str = Field(..., description="The collection ID of both cards")

class CardListing(BaseModel):
    """Model for a card listing"""
    id: Optional[str] = None  # Listing ID
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
    card_name: Optional[str] = None  # Name of the card

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
    card_name: Optional[str] = None

class OfferPointsRequest(BaseModel):
    """Request model for offering points for a listing"""
    points: int = Field(..., gt=0, description="The number of points to offer (must be greater than 0)")

class OfferCashRequest(BaseModel):
    """Request model for offering cash for a listing"""
    cash: float = Field(..., gt=0, description="The amount of cash to offer (must be greater than 0)")


class AllOffersResponse(BaseModel):
    """Response model for getting all offers for a user"""
    offers: List[Dict[str, Any]] = Field(..., description="List of all offers")

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
    subcollection_name: str = Field(..., description="The name of the subcollection where the card is stored")

class CardToDestroy(BaseModel):
    """Model for a card to destroy"""
    card_id: str = Field(..., description="The ID of the card to destroy")
    quantity: int = Field(1, gt=0, description="The quantity to destroy (default: 1)")

class DestroyCardsRequest(BaseModel):
    """Request model for destroying multiple cards"""
    cards: List[CardToDestroy] = Field(..., description="List of cards to destroy")

class WithdrawCardsRequest(BaseModel):
    """Request model for withdrawing multiple cards"""
    cards: List[CardToWithdraw] = Field(..., description="List of cards to withdraw")
    address_id: str = Field(..., description="The ID of the address to ship the cards to")
    phone_number: str = Field(..., description="The phone number of the recipient for shipping purposes")

class WithdrawCardsResponse(BaseModel):
    """Response model for withdrawing multiple cards"""
    cards: List[UserCard] = Field(..., description="List of withdrawn cards")

class WithdrawRequest(BaseModel):
    """Model for a withdraw request in the list of all withdraw requests"""
    id: str = Field(..., description="The ID of the withdraw request")
    created_at: datetime = Field(..., description="The timestamp when the withdraw request was created")
    request_date: datetime = Field(..., description="The timestamp when the withdraw request was made")
    status: str = Field(..., description="The status of the withdraw request (e.g., 'pending', 'label_created', 'shipped', 'delivered')")
    user_id: str = Field(..., description="The ID of the user who made the withdraw request")
    card_count: Optional[int] = Field(None, description="The number of cards in this withdraw request")
    shipping_address: Optional[Dict[str, Any]] = Field(None, description="The shipping address for this withdraw request")
    shippo_address_id: Optional[str] = Field(None, description="The Shippo address ID")
    shippo_parcel_id: Optional[str] = Field(None, description="The Shippo parcel ID")
    shippo_shipment_id: Optional[str] = Field(None, description="The Shippo shipment ID")
    shippo_transaction_id: Optional[str] = Field(None, description="The Shippo transaction ID for label purchase")
    shippo_label_url: Optional[str] = Field(None, description="The URL for the shipping label PDF")
    tracking_number: Optional[str] = Field(None, description="The tracking number for the shipment")
    tracking_url: Optional[str] = Field(None, description="The URL for tracking the shipment")
    shipping_status: Optional[str] = Field(None, description="The status of the shipment (e.g., 'label_created', 'shipped', 'delivered')")

class WithdrawRequestDetail(BaseModel):
    """Model for the details of a specific withdraw request"""
    id: str = Field(..., description="The ID of the withdraw request")
    created_at: datetime = Field(..., description="The timestamp when the withdraw request was created")
    request_date: datetime = Field(..., description="The timestamp when the withdraw request was made")
    status: str = Field(..., description="The status of the withdraw request (e.g., 'pending', 'label_created', 'shipped', 'delivered')")
    user_id: str = Field(..., description="The ID of the user who made the withdraw request")
    card_count: Optional[int] = Field(None, description="The number of cards in this withdraw request")
    cards: List[UserCard] = Field(..., description="The cards in this withdraw request")
    shipping_address: Optional[Dict[str, Any]] = Field(None, description="The shipping address for this withdraw request")
    shippo_address_id: Optional[str] = Field(None, description="The Shippo address ID")
    shippo_parcel_id: Optional[str] = Field(None, description="The Shippo parcel ID")
    shippo_shipment_id: Optional[str] = Field(None, description="The Shippo shipment ID")
    shippo_transaction_id: Optional[str] = Field(None, description="The Shippo transaction ID for label purchase")
    shippo_label_url: Optional[str] = Field(None, description="The URL for the shipping label PDF")
    tracking_number: Optional[str] = Field(None, description="The tracking number for the shipment")
    tracking_url: Optional[str] = Field(None, description="The URL for tracking the shipment")
    shipping_status: Optional[str] = Field(None, description="The status of the shipment (e.g., 'label_created', 'shipped', 'delivered')")

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

class AcceptedOffer(BaseModel):
    """Model for an accepted offer (cash or point)"""
    amount: float
    at: datetime
    card_reference: str
    collection_id: str
    expiresAt: datetime
    image_url: str
    listingId: str
    offererRef: str
    offerreference: str
    payment_due: datetime
    status: str
    type: str

class AcceptedOffersResponse(BaseModel):
    """Response model for listing accepted offers (cash or point)"""
    offers: List[AcceptedOffer]

class PackOpeningHistory(BaseModel):
    id: int
    user_id: str
    pack_type: str
    pack_count: int
    price_points: int
    client_seed: str
    nonce: int
    server_seed_hash: str
    server_seed: str
    random_hash: str
    opened_at: datetime

class PackOpeningHistoryResponse(BaseModel):
    pack_openings: List[PackOpeningHistory]
    total_count: int

class UpdateWithdrawRequestStatusRequest(BaseModel):
    """Request model for updating withdraw request status"""
    status: str = Field(..., description="The new status for the withdraw request (e.g., 'pending', 'label_created', 'shipped', 'delivered')")
    shipping_status: str = Field(..., description="The new shipping status for the withdraw request (e.g., 'label_created', 'shipped', 'delivered')")

class UpdateWithdrawCardsRequest(BaseModel):
    """Request model for updating cards in a withdraw request"""
    cards: List[CardToWithdraw] = Field(..., description="Updated list of cards to withdraw")
    address_id: Optional[str] = Field(None, description="The ID of the address to ship the cards to")
    phone_number: Optional[str] = Field(None, description="The phone number of the recipient for shipping purposes")

class WithdrawRequestsResponse(BaseModel):
    """Response model for listing withdraw requests with pagination"""
    withdraw_requests: List[WithdrawRequest]
    pagination: PaginationInfo

class CursorPaginationInfo(BaseModel):
    """Cursor-based pagination information for list responses"""
    next_cursor: Optional[str] = None  # Cursor for the next page, null if no more pages
    limit: int  # Number of items per page
    has_more: bool = False  # Whether there are more items to fetch

class AllWithdrawRequestsResponse(BaseModel):
    """Response model for listing all withdraw requests with cursor pagination"""
    withdraw_requests: List[WithdrawRequest]
    pagination: CursorPaginationInfo

class MarketplaceTransaction(BaseModel):
    """Model for a marketplace transaction"""
    id: str = Field(..., description="The ID of the transaction")
    listing_id: str = Field(..., description="The ID of the listing")
    seller_id: str = Field(..., description="The ID of the seller")
    buyer_id: str = Field(..., description="The ID of the buyer")
    card_id: str = Field(..., description="The ID of the card")
    quantity: int = Field(..., description="The quantity of cards traded")
    price_points: Optional[int] = Field(None, description="The price in points")
    price_card_id: Optional[str] = Field(None, description="The ID of the card used as payment (if applicable)")
    price_card_qty: Optional[int] = Field(None, description="The quantity of cards used as payment (if applicable)")
    price_cash: Optional[float] = Field(None, description="The price in cash (if applicable)")
    traded_at: datetime = Field(..., description="The timestamp when the transaction occurred")

class PayPointOfferRequest(BaseModel):
    """Request model for paying a point offer"""
    offer_id: str = Field(..., description="The ID of the offer to pay")

class PayPricePointRequest(BaseModel):
    """Request model for paying a price point directly"""
    quantity: int = Field(1, description="The quantity of cards to buy (default: 1)")

class LikeUserRequest(BaseModel):
    """Request model for liking another user"""
    target_user_id: str = Field(..., description="The ID of the user to like")

class LikeUserResponse(BaseModel):
    """Response model for liking another user"""
    success: bool
    message: str
    user_id: str
    target_user_id: str
    liked_at: datetime

class CalculateLevelResponse(BaseModel):
    """Response model for calculating a user's level"""
    user_id: str
    previous_level: int
    current_level: int
    total_drawn: int

class LevelRankEntry(BaseModel):
    """
    Represents a user's rank entry based on their level (determined by total_drawn).
    The format is "user_id:total_drawn" where total_drawn is the total number of cards drawn by the user.
    Now includes avatar and display name for more detailed information.
    """
    user_id: str
    total_drawn: int
    level: int
    display_name: Optional[str] = None
    avatar: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.user_id}:{self.total_drawn}"

class AchievementCondition(BaseModel):
    """
    Represents the condition for an achievement.
    """
    target: int
    type: str

class AchievementRewardPoint(BaseModel):
    """
    Represents a point reward for an achievement.
    """
    amount: int
    type: str = "point"

class AchievementRewardEmblem(BaseModel):
    """
    Represents an emblem reward for an achievement.
    """
    emblemId: str
    type: str = "emblem"
    url: str

class Achievement(BaseModel):
    """
    Represents an achievement in the system.
    """
    id: str
    name: str
    description: str
    image_url: Optional[str] = None
    criteria: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    emblemId: Optional[str] = None
    emblemUrl: Optional[str] = None
    condition: Optional[AchievementCondition] = None
    reward: Optional[List[Union[AchievementRewardPoint, AchievementRewardEmblem]]] = None

class UserAchievement(BaseModel):
    """
    Represents a user's progress or completion of an achievement.
    """
    achievement_id: str
    user_id: str
    acquired: bool = False
    progress: Optional[float] = None  # Progress as a percentage (0-100)
    acquired_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class AchievementWithProgress(Achievement):
    """
    Represents an achievement with user progress information.
    Only includes the fields required by the API response format.
    """
    awardedAt: Optional[datetime] = None
    progress: Optional[float] = None
    achieved: bool = False

class UserAchievementsResponse(BaseModel):
    """
    Response model for listing user achievements with pagination.
    """
    achievements: List[AchievementWithProgress]
    pagination: PaginationInfo

class AchievementResponse(BaseModel):
    """
    Simplified achievement response model that excludes certain fields.
    """
    id: str
    name: str
    description: str
    emblemId: Optional[str] = None
    emblemUrl: Optional[str] = None
    condition: Optional[AchievementCondition] = None
    reward: Optional[List[Union[AchievementRewardPoint, AchievementRewardEmblem]]] = None
    created_at: datetime
    progress: Optional[float] = None
    achieved: bool = False

class AllAchievementsResponse(BaseModel):
    """
    Response model for listing all achievements with user progress and pagination.
    """
    achievements: List[AchievementResponse]
    pagination: PaginationInfo

# Note: For file uploads, we don't use a Pydantic model
# The avatar upload endpoint will use FastAPI's File and UploadFile directly
