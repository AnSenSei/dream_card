from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class CardPack(BaseModel):
    """
    Represents a card pack, typically fetched from Firestore.
    The main pack document in Firestore might only store id, name, image_url.
    Other fields like description, rarity_probabilities, cards_by_rarity
    might be compiled from subcollections or other related data if needed for a full view.
    """
    id: str
    name: str
    image_url: Optional[str] = None
    win_rate: Optional[int] = None
    max_win: Optional[int] = None
    min_win: Optional[int] = None
    popularity: Optional[int] = 0

class AddPackRequest(BaseModel):
    """
    Request model for creating a new card pack.
    """
    pack_name: str
    collection_id: str
    price: int
    win_rate: Optional[int] = None
    max_win: Optional[int] = None
    min_win: Optional[int] = None
    is_active: bool = False
    popularity: Optional[int] = 0

class CardInPack(BaseModel):
    """
    Represents a card added to a pack.
    This is stored as a document under /packs/{packId}/cards/{cardId}
    """
    name: str
    quantity: Optional[int] = 0
    point: Optional[int] = 0
    image_url: Optional[str] = None
    probability: Optional[float] = 0.0
    condition: Optional[str] = "new"

    class Config:
        arbitrary_types_allowed = True

class AddCardToPackRequest(BaseModel):
    """
    Request model for adding a card directly to a pack with its own probability.
    """
    name: str
    quantity: Optional[int] = 0
    point: Optional[int] = 0
    image_url: Optional[str] = None
    probability: Optional[float] = 0.0
    condition: Optional[str] = "new"

class UpdatePackRequest(BaseModel):
    """
    Request model for updating an existing card pack.

    Fields:
    - pack_name: Optional new name for the pack.
    - description: Optional new description for the pack.
    - rarities: Optional. Updates to rarity configurations. For each rarity level (e.g., "common"),
      provide a dictionary. To overwrite the entire card list for a rarity, 
      include a "cards": ["card_id1", "card_id2", ...] entry in the dictionary.
      Example: `{"common": {"probability": 0.75, "cards": ["new_card_x", "new_card_y"]}}`
    - cards_to_add: Atomically adds cards to specific rarities without overwriting existing cards.
    - cards_to_delete: Atomically removes cards from specific rarities.

    At least one field (pack_name, description, rarities, cards_to_add, or cards_to_delete)
    must be provided to make an update.
    """
    pack_name: Optional[str] = None
    description: Optional[str] = None
    rarities: Optional[Dict[str, Dict[str, Any]]] = None
    win_rate: Optional[int] = None
    max_win: Optional[int] = None
    min_win: Optional[int] = None
    popularity: Optional[int] = None


class AddCardToPackDirectRequest(BaseModel):
    """
    Request model for adding a card directly to a pack with its own probability.
    This card will be stored as a document under /packs/{packId}/cards/{cardId}

    Fields:
    - collection_metadata_id: The ID of the collection metadata for fetching card details
    - document_id: The ID of the card to add (card name)
    - probability: The probability value for the card (0.0 to 1.0)
    - condition: The condition of the card (e.g., "mint", "near mint", etc.)
    """
    collection_metadata_id: str
    document_id: str
    probability: float
    condition: Optional[str] = "new"

class DeleteCardFromPackRequest(BaseModel):
    """
    Request model for deleting a card directly from a pack.
    This identifies the card to be deleted from /packs/{packId}/cards/{cardId}

    Fields:
    - collection_metadata_id: The ID of the collection metadata for identifying the card
    - document_id: The ID of the card to delete (card name)
    """
    collection_metadata_id: str
    document_id: str
