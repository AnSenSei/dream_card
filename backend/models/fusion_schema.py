from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class FusionIngredientRequest(BaseModel):
    """
    Represents an ingredient card required for a fusion recipe in request models.
    This model is used for client requests and does not include server-side fields.
    The card_collection_id is not included here as it's passed once at the recipe level.
    """
    card_id: str
    quantity: int

class FusionIngredient(BaseModel):
    """
    Represents an ingredient card required for a fusion recipe.
    This model is used for server responses and includes all fields.
    """
    card_collection_id: str
    card_id: str
    card_reference: Optional[str] = None  # This is constructed server-side
    quantity: int

class FusionRecipe(BaseModel):
    """
    Represents a fusion recipe that allows users to combine cards to create a new card.
    This model is used for server responses.
    """
    result_card_id: str
    card_collection_id: str
    card_reference: Optional[str] = None
    pack_id: str
    pack_collection_id: str
    ingredients: List[FusionIngredient]

class CreateFusionRecipeRequest(BaseModel):
    """
    Request model for creating a new fusion recipe.
    """
    result_card_id: str
    card_collection_id: str
    pack_id: str
    pack_collection_id: str
    ingredients: List[FusionIngredientRequest]

class UpdateFusionRecipeRequest(BaseModel):
    """
    Request model for updating an existing fusion recipe.
    """
    card_collection_id: Optional[str] = None
    pack_id: Optional[str] = None
    pack_collection_id: Optional[str] = None
    added_ingredients: Optional[List[FusionIngredientRequest]] = None
    deleted_ingredients: Optional[List[FusionIngredientRequest]] = None
