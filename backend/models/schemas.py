from pydantic import BaseModel
from typing import List, Dict, Optional, Any
from pydantic import validator

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
    date_got_in_stock: str # Can use date or datetime if specific format is needed
    image_url: str
    quantity: int = 0  # Default to 0 if not specified 

class UpdateQuantityRequest(BaseModel):
    quantity_change: int 

class UpdateCardRequest(BaseModel):
    card_name: Optional[str] = None
    rarity: Optional[str] = None
    point_worth: Optional[int] = None
    date_got_in_stock: Optional[str] = None
    quantity: Optional[int] = None 

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
