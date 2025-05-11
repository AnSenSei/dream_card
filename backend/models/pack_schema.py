from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class RarityDetail(BaseModel):
    """
    Represents the configuration/data for a specific rarity level within a pack.
    This data will be stored in a document under /packs/{packId}/rarities/{rarityLevel}/.
    Example: {"probability": 0.75, "card_ids": ["card1", "card2"]}
    """
    data: Dict[str, Any]

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

class AddPackRequest(BaseModel):
    """
    Request model for creating a new card pack.
    """
    pack_name: str
    rarities_config: Dict[str, RarityDetail]
    collection_id: str
    win_rate: Optional[int] = None

class CardInPack(BaseModel):
    """
    Represents a card added to a specific rarity in a pack.
    This is stored as a document under /packs/{packId}/rarities/{rarityId}/cards/{cardId}
    """
    name: str
    quantity: Optional[int] = 0
    point: Optional[int] = 0
    image_url: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

class AddCardToPackRequest(BaseModel):
    """
    Request model for adding a card to a specific rarity in a pack.
    """
    name: str
    quantity: Optional[int] = 0
    point: Optional[int] = 0
    image_url: Optional[str] = None

class UpdatePackRequest(BaseModel):
    """
    Request model for updating an existing card pack.

    Fields:
    - pack_name: Optional new name for the pack.
    - description: Optional new description for the pack.
    - rarities: Optional. Updates to rarity configurations. For each rarity level (e.g., "common"),
      provide a RarityDetail object. The `data` field within RarityDetail (a Dict[str, Any])
      will be used to set/overwrite properties for that rarity. To overwrite the entire card list
      for a rarity, include a "cards": ["card_id1", "card_id2", ...] entry in the `data` dict.
      Example: `{"common": {"data": {"probability": 0.75, "cards": ["new_card_x", "new_card_y"]}}}`
    - cards_to_add: Atomically adds cards to specific rarities without overwriting existing cards.
    - cards_to_delete: Atomically removes cards from specific rarities.

    At least one field (pack_name, description, rarities, cards_to_add, or cards_to_delete)
    must be provided to make an update.
    """
    pack_name: Optional[str] = None
    description: Optional[str] = None
    rarities: Optional[Dict[str, RarityDetail]] = None # Changed from Dict[str, Dict[str, Any]]
    win_rate: Optional[int] = None

class UpdateRarityProbabilityRequest(BaseModel):
    """
    Request model for updating the probability of a specific rarity in a card pack.

    Fields:
    - probability: New probability value for the rarity (0.0 to 1.0)
    """
    probability: float

class AddRarityRequest(BaseModel):
    """
    Request model for adding a new rarity with probability to a card pack.

    Fields:
    - rarity_name: Name of the new rarity
    - probability: Probability value for the rarity (0.0 to 1.0)
    """
    rarity_id: str
    probability: float

class CollectionPackRarityParams(BaseModel):
    """
    Model for specifying the path parameters for accessing a rarity within a pack collection.

    Fields:
    - collection_id: ID of the pack collection
    - pack_id: ID of the pack
    - rarity_id: ID of the rarity
    """
    collection_id: str
    pack_id: str
    rarity_id: str

class DeleteRarityRequest(BaseModel):
    """
    Request model for deleting a rarity from a card pack.
    This model is currently empty as the rarity ID is passed in the URL path.
    """
    pass

class AddCardToRarityRequest(BaseModel):
    """
    Request model for adding a card to a rarity in a pack.
    This card will be stored as a document under /packs/{packId}/rarities/{rarityId}/cards/{cardId}

    Fields:
    - collection_metadata_id: The ID of the collection metadata for fetching card details
    - document_id: The ID of the card to add (card name)
    """
    collection_metadata_id: str
    document_id: str

class DeleteCardFromRarityRequest(BaseModel):
    """
    Request model for deleting a card from a rarity in a pack.
    This identifies the card to be deleted from /packs/{packId}/rarities/{rarityId}/cards/{cardId}

    Fields:
    - collection_metadata_id: The ID of the collection metadata for identifying the card
    - document_id: The ID of the card to delete (card name)
    """
    collection_metadata_id: str
    document_id: str
