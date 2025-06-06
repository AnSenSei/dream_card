from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class CardFusionInfo(BaseModel):
    """
    Represents information about a fusion recipe that uses a specific card as an ingredient.
    This model is used for the response when querying what fusions a card is used in.
    """
    fusion_id: str
    result_card_id: str
    pack_reference: str

class CardFusionsResponse(BaseModel):
    """
    Response model for the endpoint that returns information about what fusions a card is used in.
    """
    card_id: str
    collection_id: str
    fusions: List[CardFusionInfo]

class PaginationInfo(BaseModel):
    """Pagination information for list responses"""
    total_items: int
    total_pages: int
    current_page: int
    per_page: int

class AppliedFilters(BaseModel):
    """Filters applied to a fusion recipe list query"""
    sort_by: Optional[str] = None
    sort_order: str = "desc"
    search_query: Optional[str] = None

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
    cards_needed: Optional[int] = None
    total_cards_needed: Optional[int] = None

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

class FusionRecipePack(BaseModel):
    """
    Represents a pack containing fusion recipes.
    """
    pack_id: str
    pack_collection_id: str
    cards: List[FusionRecipe]
    cards_count: int

class FusionRecipeCollection(BaseModel):
    """
    Represents a collection containing fusion recipe packs.
    """
    collection_id: str
    packs: List[FusionRecipePack]
    packs_count: int

class PaginatedFusionRecipesResponse(BaseModel):
    """Response model for paginated fusion recipes"""
    collections: List[FusionRecipeCollection]
    pagination: PaginationInfo
    filters: AppliedFilters
