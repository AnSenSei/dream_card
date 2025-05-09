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
    description: Optional[str] = None 
    rarity_configurations: Optional[Dict[str, Dict[str, Any]]] = None 
    # Example of what might be compiled if needed for a detailed pack view:
    # rarity_probabilities: Optional[Dict[str, float]] = None
    # cards_by_rarity: Optional[Dict[str, List[str]]] = None 

class AddPackRequest(BaseModel):
    """
    Request model for creating a new card pack.
    """
    pack_name: str
    rarities_config: Dict[str, RarityDetail] 

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