from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from pydantic import validator
from datetime import datetime


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

class CollectionMetadata(BaseModel):
    """
    Represents metadata for a collection.
    This data will be stored in the collection_meta_data collection.
    """
    name: str
    firestoreCollection: str
    storagePrefix: str

class Card(BaseModel):
    id: str
    name: str
    image_url: str # URL to Cloud Storage
    rarity: str # e.g., "Common", "Rare", "Epic", "Legendary"
    quantity: int # Number of this card available
    # Add any other card attributes here

class CardPack(BaseModel):
    id: str
    name: str
    description: str
    rarity_probabilities: Dict[str, float]
    cards_by_rarity: Dict[str, List[str]]

class DrawRequest(BaseModel):
    num_cards: int = 5 # Default to 5 if not provided, can be 1 or 10 as per new req. 

class StoredCardInfo(BaseModel):
    id: str
    card_name: str
    rarity: int
    point_worth: int
    date_got_in_stock: str  # Can use date or datetime if specific format is needed
    image_url: str
    quantity: int = 0  # D
    condition: str = "mint"  # Default condition is "mint"

class UpdateQuantityRequest(BaseModel):
    quantity_change: int 

class UpdateCardRequest(BaseModel):
    card_name: Optional[str] = None
    rarity: Optional[int] = None
    point_worth: Optional[int] = None
    date_got_in_stock: Optional[str] = None
    quantity: Optional[int] = None
    condition: Optional[str] = None

# --- Models for paginated card list response ---
class PaginationInfo(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    per_page: int

class AppliedFilters(BaseModel):
    sort_by: str
    sort_order: str
    search_query: Optional[str] = None
    # In the future, could add:
    # available_sort_options: List[str]

class CardListResponse(BaseModel):
    cards: List[StoredCardInfo]
    pagination: PaginationInfo
    filters: AppliedFilters 

# --- Models for adding new packs ---
class RarityDetail(BaseModel):
    """
    Represents the configuration/data for a specific rarity level within a pack.
    This data will be stored in a document under /packs/{packId}/rarities/{rarityLevel}/.
    """
    # Example: attributes: Dict[str, Any] = Field(default_factory=dict)
    # For now, allowing any structure. Define specific fields as needed.
    # e.g., drop_rate: float, card_pool: List[str], etc.
    data: Dict[str, Any] # The actual content for the rarity document

class AddPackRequest(BaseModel):
    """
    Request model for creating a new card pack.
    """
    pack_name: str
    # The keys are rarity_level strings (e.g., "Common", "Legendary_Rare_EX_Tier_SSJ4_GODMODE")
    # The values are RarityDetail objects containing the data for that rarity.
    rarities_config: Dict[str, RarityDetail]

# --- Models for withdraw requests ---
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
    cards: List[UserCard] = Field(..., description="The cards in this withdraw request")

class CursorPaginationInfo(BaseModel):
    """Cursor-based pagination information for list responses"""
    next_cursor: Optional[str] = None  # Cursor for the next page, null if no more pages
    limit: int  # Number of items per page
    has_more: bool = False  # Whether there are more items to fetch

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

class WithdrawRequestDetail(BaseModel):
    """Model for the details of a specific withdraw request"""
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

class UpdateWithdrawRequestStatusRequest(BaseModel):
    """Request model for updating withdraw request status"""
    status: str = Field(..., description="The new status for the withdraw request (e.g., 'pending', 'label_created', 'shipped', 'delivered')")
    shipping_status: str = Field(..., description="The new shipping status for the withdraw request (e.g., 'label_created', 'shipped', 'delivered')")

class AllWithdrawRequestsResponse(BaseModel):
    """Response model for listing all withdraw requests with cursor pagination"""
    withdraw_requests: List[WithdrawRequest]
    pagination: CursorPaginationInfo
