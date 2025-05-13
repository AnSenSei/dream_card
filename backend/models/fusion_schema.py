from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class FusionIngredient(BaseModel):
    """
    Represents an ingredient card required for a fusion recipe.
    """
    card_collection_id: str
    card_id: str
    quantity: int
    available_in_packages: Optional[List[str]] = []

class FusionRecipe(BaseModel):
    """
    Represents a fusion recipe that allows users to combine cards to create a new card.
    """
    result_card_id: str
    pack_id: str
    pack_collection_id: str
    ingredients: List[FusionIngredient]

class CreateFusionRecipeRequest(BaseModel):
    """
    Request model for creating a new fusion recipe.
    """
    result_card_id: str
    pack_id: str
    pack_collection_id: str
    ingredients: List[FusionIngredient]

class UpdateFusionRecipeRequest(BaseModel):
    """
    Request model for updating an existing fusion recipe.
    """
    pack_id: Optional[str] = None
    pack_collection_id: Optional[str] = None
    added_ingredients: Optional[List[FusionIngredient]] = None
    deleted_ingredients: Optional[List[FusionIngredient]] = None
