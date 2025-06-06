from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Dict, Optional, Any
import json

from models.fusion_schema import (
    FusionRecipe,
    CreateFusionRecipeRequest,
    UpdateFusionRecipeRequest,
    PaginationInfo,
    AppliedFilters,
    FusionRecipePack,
    FusionRecipeCollection,
    PaginatedFusionRecipesResponse,
    CardFusionsResponse
)
from pydantic import BaseModel
from service.fusion_service import (
    create_fusion_recipe,
    get_fusion_recipe_by_id,
    get_all_fusion_recipes,
    update_fusion_recipe,
    delete_fusion_recipe,
    get_card_fusions
)
from config import get_firestore_client, get_logger
from google.cloud import firestore

logger = get_logger(__name__)

router = APIRouter(
    prefix="/fusion_recipes",
    tags=["fusion_recipes"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=Dict[str, str], status_code=201)
async def create_fusion_recipe_route(
    recipe: CreateFusionRecipeRequest = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Creates a new fusion recipe.

    Args:
        recipe: The CreateFusionRecipeRequest containing recipe details
        db: Firestore client dependency

    Returns:
        Dict with result_card_id and success message
    """
    try:
        result_card_id = await create_fusion_recipe(recipe, db)
        return {
            "result_card_id": result_card_id,
            "message": f"Fusion recipe for '{result_card_id}' created successfully"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in create_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while creating the fusion recipe.")

@router.get("/{pack_collection_id}/{pack_id}/cards/{result_card_id}", response_model=FusionRecipe)
async def get_fusion_recipe_route(
    pack_collection_id: str,
    pack_id: str,
    result_card_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves a fusion recipe by its pack collection ID, pack ID, and result card ID.

    Args:
        pack_collection_id: The ID of the pack collection
        pack_id: The ID of the pack
        result_card_id: The ID of the result card
        db: Firestore client dependency

    Returns:
        FusionRecipe: The requested fusion recipe
    """
    try:
        return await get_fusion_recipe_by_id(pack_id, pack_collection_id, result_card_id, db)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving the fusion recipe.")

@router.get("/", response_model=PaginatedFusionRecipesResponse)
async def get_all_fusion_recipes_route(
    collection_id: Optional[str] = None,
    user_id: Optional[str] = None,
    page: int = 1,
    per_page: int = 10,
    sort_by: str = "result_card_id",
    sort_order: str = "desc",
    search_query: Optional[str] = None,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves fusion recipes grouped by collections and packs with pagination.
    If collection_id is provided, retrieves only recipes under that collection.
    Otherwise, retrieves all recipes from all collections.
    If user_id is provided, calculates how many kinds of cards are needed vs. total kinds needed for each recipe.

    Args:
        collection_id: Optional ID of the collection to filter recipes by
        user_id: Optional ID of the user to calculate cards needed for
        page: Page number (default: 1)
        per_page: Number of items per page (default: 10)
        sort_by: Field to sort by (default: "result_card_id")
        sort_order: Sort order ("asc" or "desc", default: "desc")
        search_query: Optional search query to filter recipes by result_card_id
        db: Firestore client dependency

    Returns:
        PaginatedFusionRecipesResponse: Paginated list of collections with their packs and fusion recipes
    """
    try:
        return await get_all_fusion_recipes(
            db_client=db,
            collection_id=collection_id,
            user_id=user_id,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_all_fusion_recipes_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving fusion recipes.")

@router.get("/{collection_id}/recipes", response_model=PaginatedFusionRecipesResponse)
async def get_collection_recipes_route(
    collection_id: str,
    user_id: Optional[str] = None,
    page: int = 1,
    per_page: int = 10,
    sort_by: str = "result_card_id",
    sort_order: str = "desc",
    search_query: Optional[str] = None,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves all fusion recipes for a specific collection, grouped by packs, with pagination.
    If user_id is provided, calculates how many kinds of cards are needed vs. total kinds needed for each recipe.

    Args:
        collection_id: ID of the collection to get recipes for
        user_id: Optional ID of the user to calculate cards needed for
        page: Page number (default: 1)
        per_page: Number of items per page (default: 10)
        sort_by: Field to sort by (default: "result_card_id")
        sort_order: Sort order ("asc" or "desc", default: "desc")
        search_query: Optional search query to filter recipes by result_card_id
        db: Firestore client dependency

    Returns:
        PaginatedFusionRecipesResponse: Paginated collection with its packs and fusion recipes
    """
    try:
        return await get_all_fusion_recipes(
            db_client=db,
            collection_id=collection_id,
            user_id=user_id,
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            search_query=search_query
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_collection_recipes_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred while retrieving fusion recipes for collection '{collection_id}'.")

@router.put("/{pack_collection_id}/{pack_id}/cards/{result_card_id}", response_model=Dict[str, str])
async def update_fusion_recipe_route(
    pack_collection_id: str,
    pack_id: str,
    result_card_id: str,
    updates: UpdateFusionRecipeRequest = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates an existing fusion recipe.

    Args:
        pack_collection_id: The ID of the pack collection
        pack_id: The ID of the pack
        result_card_id: The ID of the result card
        updates: The UpdateFusionRecipeRequest containing fields to update
        db: Firestore client dependency

    Returns:
        Dict with success message
    """
    try:
        await update_fusion_recipe(pack_id, pack_collection_id, result_card_id, updates, db)
        return {
            "message": f"Fusion recipe for result card '{result_card_id}' in pack '{pack_id}' and collection '{pack_collection_id}' updated successfully"
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in update_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the fusion recipe.")

@router.delete("/{pack_collection_id}/{pack_id}/cards/{result_card_id}", response_model=Dict[str, str])
async def delete_fusion_recipe_route(
    pack_collection_id: str,
    pack_id: str,
    result_card_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a fusion recipe.

    Args:
        pack_collection_id: The ID of the pack collection
        pack_id: The ID of the pack
        result_card_id: The ID of the result card
        db: Firestore client dependency

    Returns:
        Dict with success message
    """
    try:
        await delete_fusion_recipe(pack_id, pack_collection_id, result_card_id, db)
        return {
            "message": f"Fusion recipe for result card '{result_card_id}' in pack '{pack_id}' and collection '{pack_collection_id}' deleted successfully"
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in delete_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the fusion recipe.")

@router.get("/card/{collection_id}/{card_id}", response_model=CardFusionsResponse)
async def get_card_fusions_route(
    collection_id: str,
    card_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves information about what fusions a card is used in.

    Args:
        collection_id: The ID of the collection the card belongs to
        card_id: The ID of the card
        db: Firestore client dependency

    Returns:
        CardFusionsResponse: Information about the fusions the card is used in
    """
    try:
        return await get_card_fusions(collection_id, card_id, db)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_card_fusions_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal error occurred while retrieving fusion information for card '{card_id}' in collection '{collection_id}'.")
