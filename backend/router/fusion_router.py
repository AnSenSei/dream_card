from fastapi import APIRouter, HTTPException, Depends, Body
from typing import List, Dict, Optional, Any
import json

from models.fusion_schema import (
    FusionRecipe,
    CreateFusionRecipeRequest,
    UpdateFusionRecipeRequest
)
from service.fusion_service import (
    create_fusion_recipe,
    get_fusion_recipe_by_id,
    get_all_fusion_recipes,
    update_fusion_recipe,
    delete_fusion_recipe
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

@router.get("/{result_card_id}", response_model=FusionRecipe)
async def get_fusion_recipe_route(
    result_card_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves a fusion recipe by its result card ID.
    
    Args:
        result_card_id: The ID of the result card
        db: Firestore client dependency
        
    Returns:
        FusionRecipe: The requested fusion recipe
    """
    try:
        return await get_fusion_recipe_by_id(result_card_id, db)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving the fusion recipe.")

@router.get("/", response_model=List[FusionRecipe])
async def get_all_fusion_recipes_route(
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Retrieves all fusion recipes.
    
    Args:
        db: Firestore client dependency
        
    Returns:
        List[FusionRecipe]: List of all fusion recipes
    """
    try:
        return await get_all_fusion_recipes(db)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in get_all_fusion_recipes_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving fusion recipes.")

@router.put("/{result_card_id}", response_model=Dict[str, str])
async def update_fusion_recipe_route(
    result_card_id: str,
    updates: UpdateFusionRecipeRequest = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Updates an existing fusion recipe.
    
    Args:
        result_card_id: The ID of the result card
        updates: The UpdateFusionRecipeRequest containing fields to update
        db: Firestore client dependency
        
    Returns:
        Dict with success message
    """
    try:
        await update_fusion_recipe(result_card_id, updates, db)
        return {
            "message": f"Fusion recipe for '{result_card_id}' updated successfully"
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in update_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while updating the fusion recipe.")

@router.delete("/{result_card_id}", response_model=Dict[str, str])
async def delete_fusion_recipe_route(
    result_card_id: str,
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Deletes a fusion recipe.
    
    Args:
        result_card_id: The ID of the result card
        db: Firestore client dependency
        
    Returns:
        Dict with success message
    """
    try:
        await delete_fusion_recipe(result_card_id, db)
        return {
            "message": f"Fusion recipe for '{result_card_id}' deleted successfully"
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error in delete_fusion_recipe_route: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while deleting the fusion recipe.")