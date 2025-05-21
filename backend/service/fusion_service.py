from typing import List, Optional, Dict, Any
from fastapi import HTTPException
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, ArrayUnion

from config import get_logger
from models.fusion_schema import FusionRecipe, FusionIngredient, FusionIngredientRequest, CreateFusionRecipeRequest, UpdateFusionRecipeRequest
from service.storage_service import update_card_information

logger = get_logger(__name__)

async def create_fusion_recipe(
    recipe_data: CreateFusionRecipeRequest,
    db_client: AsyncClient
) -> str:
    """
    Creates a new fusion recipe in Firestore.

    Args:
        recipe_data: The CreateFusionRecipeRequest model containing recipe details
        db_client: Firestore client

    Returns:
        str: The ID of the created recipe (same as result_card_id)

    Raises:
        HTTPException: If there's an error creating the recipe
    """
    if not db_client:
        logger.error("Firestore client not provided to create_fusion_recipe.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")

    result_card_id = recipe_data.result_card_id

    if not result_card_id:
        raise HTTPException(status_code=400, detail="Result card ID cannot be empty.")

    try:
        # Use result_card_id as the document ID
        doc_ref = db_client.collection('fusion_recipes').document(result_card_id)

        # Check if recipe with this ID already exists
        doc = await doc_ref.get()
        if doc.exists:
            logger.warning(f"Fusion recipe with ID '{result_card_id}' already exists")
            raise HTTPException(status_code=409, detail=f"Fusion recipe with ID '{result_card_id}' already exists")

        # Get collection metadata to create card_reference
        card_reference = None
        try:
            from service.storage_service import get_collection_metadata
            metadata = await get_collection_metadata(recipe_data.card_collection_id)
            card_reference = f"{metadata.firestoreCollection}/{result_card_id}"
            logger.info(f"Created card_reference: {card_reference}")
        except Exception as e:
            logger.warning(f"Could not create card_reference: {e}")
            # If we can't get the metadata, use the card_collection_id as is
            card_reference = f"{recipe_data.card_collection_id}/{result_card_id}"

        # Get collection metadata once for all ingredients
        from service.storage_service import get_collection_metadata
        global_card_collection = recipe_data.card_collection_id
        collection_name = recipe_data.card_collection_id

        try:
            metadata = await get_collection_metadata(recipe_data.card_collection_id)
            global_card_collection = metadata.firestoreCollection
            collection_name = metadata.firestoreCollection
            logger.info(f"Using metadata firestoreCollection path: '{global_card_collection}' and name: '{collection_name}'")
        except Exception as e:
            logger.warning(f"Could not get collection metadata: {e}")
            # If we can't get the metadata, use the card_collection_id as is

        # Add card_reference to each ingredient
        ingredients_with_reference = []
        for ingredient in recipe_data.ingredients:
            ingredient_data = ingredient.model_dump()
            # Add card_collection_id to each ingredient
            ingredient_data["card_collection_id"] = recipe_data.card_collection_id
            # Create card_reference using the collection name
            ingredient_data["card_reference"] = f"{collection_name}/{ingredient.card_id}"
            logger.info(f"Created card_reference for ingredient: {ingredient_data['card_reference']}")

            ingredients_with_reference.append(ingredient_data)

        # Create recipe document data
        recipe_doc_data = {
            "result_card_id": result_card_id,
            "card_collection_id": recipe_data.card_collection_id,
            "card_reference": card_reference,
            "pack_id": recipe_data.pack_id,
            "pack_collection_id": recipe_data.pack_collection_id,
            "ingredients": ingredients_with_reference,
            "created_at": firestore.SERVER_TIMESTAMP
        }

        # Set the recipe document
        await doc_ref.set(recipe_doc_data)
        logger.info(f"Created fusion recipe document for result card '{result_card_id}'")

        # Update each ingredient card with fusion information
        for ingredient in recipe_data.ingredients:
            try:
                # Create fusion info object with pack_reference instead of package_id
                pack_reference = f"/packs/{recipe_data.pack_collection_id}/{recipe_data.pack_collection_id}/{recipe_data.pack_id}"
                fusion_info = {
                    "fusion_id": result_card_id,  # Using result_card_id as fusion_id
                    "result_card_id": result_card_id,
                    "pack_reference": pack_reference
                }

                # Update the card with the fusion information
                # We need to use ArrayUnion to add to the array without overwriting existing entries
                update_data = {
                    "used_in_fusion": ArrayUnion([fusion_info])
                }

                await update_card_information(
                    document_id=ingredient.card_id,
                    update_data=update_data,
                    collection_name=recipe_data.card_collection_id
                )

                logger.info(f"Updated card '{ingredient.card_id}' (with collection_id '{recipe_data.card_collection_id}') with fusion information")
            except Exception as e:
                # Log the error but continue with other ingredients
                logger.error(f"Error updating ingredient card '{ingredient.card_id}' with fusion information: {e}", exc_info=True)
                # We don't want to fail the whole recipe creation if updating an ingredient fails
                # So we just log the error and continue

        return result_card_id
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error creating fusion recipe in Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error creating fusion recipe in Firestore: {str(e)}")

async def get_fusion_recipe_by_id(
    result_card_id: str,
    db_client: AsyncClient
) -> FusionRecipe:
    """
    Retrieves a fusion recipe by its result card ID from Firestore.

    Args:
        result_card_id: The ID of the result card (used as document ID)
        db_client: Firestore client

    Returns:
        FusionRecipe: The requested fusion recipe

    Raises:
        HTTPException: If recipe not found or on database error
    """
    if not db_client:
        logger.error("Firestore client not provided to get_fusion_recipe_by_id.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")

    try:
        doc_ref = db_client.collection('fusion_recipes').document(result_card_id)
        doc = await doc_ref.get()

        if not doc.exists:
            logger.warning(f"Fusion recipe with ID '{result_card_id}' not found")
            raise HTTPException(status_code=404, detail=f"Fusion recipe with ID '{result_card_id}' not found")

        recipe_data = doc.to_dict()

        # Convert ingredients data to FusionIngredient objects
        ingredients = []
        for ingredient_data in recipe_data.get('ingredients', []):
            ingredients.append(FusionIngredient(**ingredient_data))

        return FusionRecipe(
            result_card_id=recipe_data.get('result_card_id'),
            card_collection_id=recipe_data.get('card_collection_id'),
            card_reference=recipe_data.get('card_reference'),
            pack_id=recipe_data.get('pack_id'),
            pack_collection_id=recipe_data.get('pack_collection_id'),
            ingredients=ingredients
        )
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error retrieving fusion recipe '{result_card_id}' from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not retrieve fusion recipe '{result_card_id}' from database.")

async def get_all_fusion_recipes(
    db_client: AsyncClient
) -> List[FusionRecipe]:
    """
    Retrieves all fusion recipes from Firestore.

    Args:
        db_client: Firestore client

    Returns:
        List[FusionRecipe]: List of all fusion recipes

    Raises:
        HTTPException: On database error
    """
    if not db_client:
        logger.error("Firestore client not provided to get_all_fusion_recipes.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")

    try:
        recipes_ref = db_client.collection('fusion_recipes')
        recipes_stream = recipes_ref.stream()

        recipes_list = []
        async for doc in recipes_stream:
            recipe_data = doc.to_dict()

            # Convert ingredients data to FusionIngredient objects
            ingredients = []
            for ingredient_data in recipe_data.get('ingredients', []):
                ingredients.append(FusionIngredient(**ingredient_data))

            recipes_list.append(FusionRecipe(
                result_card_id=recipe_data.get('result_card_id'),
                card_collection_id=recipe_data.get('card_collection_id'),
                card_reference=recipe_data.get('card_reference'),
                pack_id=recipe_data.get('pack_id'),
                pack_collection_id=recipe_data.get('pack_collection_id'),
                ingredients=ingredients
            ))

        return recipes_list
    except Exception as e:
        logger.error(f"Error retrieving fusion recipes from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not retrieve fusion recipes from database.")

async def update_fusion_recipe(
    result_card_id: str,
    updates: UpdateFusionRecipeRequest,
    db_client: AsyncClient
) -> bool:
    """
    Updates an existing fusion recipe in Firestore.
    Also updates the fusion information in ingredient cards.

    Args:
        result_card_id: The ID of the result card (used as document ID)
        updates: The UpdateFusionRecipeRequest model containing fields to update
        db_client: Firestore client

    Returns:
        bool: True if update was successful

    Raises:
        HTTPException: If recipe not found or on database error
    """
    if not db_client:
        logger.error("Firestore client not provided to update_fusion_recipe.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")

    try:
        doc_ref = db_client.collection('fusion_recipes').document(result_card_id)
        doc = await doc_ref.get()

        if not doc.exists:
            logger.warning(f"Fusion recipe with ID '{result_card_id}' not found")
            raise HTTPException(status_code=404, detail=f"Fusion recipe with ID '{result_card_id}' not found")

        # Get current recipe data
        current_recipe_data = doc.to_dict()

        # Prepare update data
        update_data = {}

        # Track if pack_id is updated
        new_pack_id = None
        if updates.pack_id is not None:
            update_data['pack_id'] = updates.pack_id
            new_pack_id = updates.pack_id

        if updates.pack_collection_id is not None:
            update_data['pack_collection_id'] = updates.pack_collection_id

        if updates.card_collection_id is not None:
            update_data['card_collection_id'] = updates.card_collection_id

            # Update card_reference if card_collection_id is updated
            try:
                from service.storage_service import get_collection_metadata
                metadata = await get_collection_metadata(updates.card_collection_id)
                update_data['card_reference'] = f"{metadata.name}/{result_card_id}"
                logger.info(f"Updated card_reference: {update_data['card_reference']}")
            except Exception as e:
                logger.warning(f"Could not update card_reference: {e}")
                # If we can't get the metadata, use the card_collection_id as is
                update_data['card_reference'] = f"{updates.card_collection_id}/{result_card_id}"


        # Handle ingredient updates
        if updates.added_ingredients is not None or updates.deleted_ingredients is not None:
            # Get current ingredients
            current_ingredients = current_recipe_data.get('ingredients', [])

            # Use the current recipe's card_collection_id or the updated one if provided
            card_collection_id = updates.card_collection_id or current_recipe_data.get('card_collection_id')

            ingredients_to_add = {
                (ingredient.card_id, card_collection_id)
                for ingredient in (updates.added_ingredients or [])
            }

            # Create a dictionary to store the full ingredient objects for removal
            ingredients_to_remove_dict = {}
            for ingredient in (updates.deleted_ingredients or []):
                key = (ingredient.card_id, card_collection_id)
                ingredients_to_remove_dict[key] = ingredient

            # Create a set of tuples for efficient set operations
            ingredients_to_remove = set(ingredients_to_remove_dict.keys())

            # Log the ingredients to be removed for debugging
            for (card_id, card_collection_id), ingredient in ingredients_to_remove_dict.items():
                logger.info(f"Will remove fusion information from card '{card_id}' (with collection_id '{card_collection_id}') with quantity={ingredient.quantity}")

            # Calculate the new ingredients list by removing deleted and adding new
            current_ingredient_set = {
                (ingredient.get('card_id'), ingredient.get('card_collection_id'))
                for ingredient in current_ingredients
            }

            # Remove ingredients_to_remove and add ingredients_to_add
            final_ingredient_set = (current_ingredient_set - ingredients_to_remove) | ingredients_to_add

            # Convert back to a list of FusionIngredient objects
            final_ingredients = []
            for card_id, card_collection_id in final_ingredient_set:
                # Find the ingredient in current ingredients or added ingredients
                ingredient_found = False

                # First check in current ingredients
                for ingredient in current_ingredients:
                    if (ingredient.get('card_id') == card_id and 
                        ingredient.get('card_collection_id') == card_collection_id):
                        final_ingredients.append(FusionIngredient(**ingredient))
                        ingredient_found = True
                        break

                # If not found in current, check in added ingredients
                if not ingredient_found and updates.added_ingredients:
                    for ingredient in updates.added_ingredients:
                        if (ingredient.card_id == card_id and 
                            ingredient.card_collection_id == card_collection_id):
                            final_ingredients.append(ingredient)
                            break

            # Remove fusion information from removed ingredients
            for key in ingredients_to_remove:
                card_id, card_collection_id = key
                ingredient = ingredients_to_remove_dict[key]

                try:
                    # Use get_card_by_id to handle collection metadata lookup
                    from service.storage_service import get_card_by_id
                    try:
                        card = await get_card_by_id(card_id, card_collection_id)
                        card_data = card.model_dump()

                        # Remove this fusion from the used_in_fusion array
                        if 'used_in_fusion' in card_data and card_data['used_in_fusion']:
                            # Filter out the fusion with this result_card_id
                            updated_fusions = [
                                fusion for fusion in card_data['used_in_fusion'] 
                                if fusion.get('result_card_id') != result_card_id
                            ]

                            # Update the card with the filtered array using update_card_information
                            from service.storage_service import update_card_information
                            await update_card_information(
                                document_id=card_id,
                                update_data={'used_in_fusion': updated_fusions},
                                collection_name=card_collection_id
                            )

                            logger.info(f"Removed fusion information from card '{card_id}' (with collection_id '{card_collection_id}') with quantity={ingredient.quantity}")
                        else:
                            logger.info(f"Card '{card_id}' (with collection_id '{card_collection_id}') has no fusion information to remove")
                    except HTTPException as e:
                        if e.status_code == 404:
                            # If the card doesn't exist, log a warning but continue
                            logger.warning(f"Card '{card_id}' (with collection_id '{card_collection_id}') not found when trying to remove fusion information")
                        else:
                            # Re-raise other HTTP exceptions
                            raise e
                except Exception as e:
                    # Log the error but continue with other ingredients
                    logger.error(f"Error removing fusion information from ingredient card: {e}", exc_info=True)

            # Add fusion information to new ingredients
            for card_id, card_collection_id in ingredients_to_add:
                try:
                    # Create fusion info object with pack_reference instead of package_id
                    pack_id = new_pack_id or current_recipe_data.get('pack_id')
                    pack_collection_id = updates.pack_collection_id or current_recipe_data.get('pack_collection_id')
                    pack_reference = f"/packs/{pack_collection_id}/{pack_collection_id}/{pack_id}"

                    fusion_info = {
                        "fusion_id": result_card_id,
                        "result_card_id": result_card_id,
                        "pack_reference": pack_reference
                    }

                    # Update the card with the fusion information
                    update_data = {
                        "used_in_fusion": ArrayUnion([fusion_info])
                    }

                    await update_card_information(
                        document_id=card_id,
                        update_data=update_data,
                        collection_name=card_collection_id
                    )

                    logger.info(f"Added fusion information to card '{card_id}' (with collection_id '{card_collection_id}')")
                except Exception as e:
                    # Log the error but continue with other ingredients
                    logger.error(f"Error adding fusion information to ingredient card: {e}", exc_info=True)

            # Get collection metadata once for all ingredients
            from service.storage_service import get_collection_metadata
            global_card_collection = card_collection_id
            collection_name = card_collection_id

            try:
                metadata = await get_collection_metadata(card_collection_id)
                global_card_collection = metadata.firestoreCollection
                collection_name = metadata.name
                logger.info(f"Using metadata firestoreCollection path: '{global_card_collection}' and name: '{collection_name}'")
            except Exception as e:
                logger.warning(f"Could not get collection metadata: {e}")
                # If we can't get the metadata, use the card_collection_id as is

            # Add card_reference to each ingredient and update the ingredients in the recipe
            ingredients_with_reference = []
            for ingredient in final_ingredients:
                ingredient_data = ingredient.model_dump()

                # If ingredient already has card_reference, use it
                if hasattr(ingredient, 'card_reference') and ingredient.card_reference:
                    ingredient_data["card_reference"] = ingredient.card_reference
                else:
                    # Create card_reference using the collection name
                    ingredient_data["card_reference"] = f"{collection_name}/{ingredient.card_id}"
                    logger.info(f"Created card_reference for ingredient: {ingredient_data['card_reference']}")

                # Ensure card_collection_id is set correctly
                ingredient_data["card_collection_id"] = card_collection_id

                ingredients_with_reference.append(ingredient_data)

            update_data['ingredients'] = ingredients_with_reference

        # If pack_id is updated but ingredients are not, update fusion information in all current ingredients
        elif new_pack_id is not None:
            current_ingredients = current_recipe_data.get('ingredients', [])
            for ingredient in current_ingredients:
                try:
                    card_id = ingredient.get('card_id')
                    card_collection_id = ingredient.get('card_collection_id')

                    if card_id and card_collection_id:
                        # Use get_card_by_id to handle collection metadata lookup
                        from service.storage_service import get_card_by_id
                        try:
                            card = await get_card_by_id(card_id, card_collection_id)
                            card_data = card.model_dump()

                            # Update the pack_reference in the used_in_fusion array
                            if 'used_in_fusion' in card_data and card_data['used_in_fusion']:
                                updated_fusions = []
                                for fusion in card_data['used_in_fusion']:
                                    if fusion.get('result_card_id') == result_card_id:
                                        # Update the pack_reference instead of package_id
                                        pack_collection_id = updates.pack_collection_id or current_recipe_data.get('pack_collection_id')
                                        pack_reference = f"/packs/{pack_collection_id}/{pack_collection_id}/{new_pack_id}"

                                        # Remove package_id if it exists and add pack_reference
                                        if 'package_id' in fusion:
                                            del fusion['package_id']

                                        fusion['pack_reference'] = pack_reference
                                    updated_fusions.append(fusion)

                                # Update the card with the updated array using update_card_information
                                from service.storage_service import update_card_information
                                await update_card_information(
                                    document_id=card_id,
                                    update_data={'used_in_fusion': updated_fusions},
                                    collection_name=card_collection_id
                                )

                                logger.info(f"Updated fusion information in card '{card_id}' (with collection_id '{card_collection_id}')")
                        except HTTPException as e:
                            if e.status_code == 404:
                                # If the card doesn't exist, log a warning but continue
                                logger.warning(f"Card '{card_id}' (with collection_id '{card_collection_id}') not found when trying to update fusion information")
                            else:
                                # Re-raise other HTTP exceptions
                                raise e
                except Exception as e:
                    # Log the error but continue with other ingredients
                    logger.error(f"Error updating fusion information in ingredient card: {e}", exc_info=True)

        if not update_data:
            logger.warning(f"No updates provided for fusion recipe '{result_card_id}'")
            return True  # Nothing to update

        # Update the document
        await doc_ref.update(update_data)
        logger.info(f"Updated fusion recipe '{result_card_id}'")

        return True
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error updating fusion recipe '{result_card_id}' in Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not update fusion recipe '{result_card_id}' in database.")

async def delete_fusion_recipe(
    result_card_id: str,
    db_client: AsyncClient
) -> bool:
    """
    Deletes a fusion recipe from Firestore.
    Also removes the fusion information from all ingredient cards.

    Args:
        result_card_id: The ID of the result card (used as document ID)
        db_client: Firestore client

    Returns:
        bool: True if deletion was successful

    Raises:
        HTTPException: If recipe not found or on database error
    """
    if not db_client:
        logger.error("Firestore client not provided to delete_fusion_recipe.")
        raise HTTPException(status_code=500, detail="Firestore service not configured (client missing).")

    try:
        doc_ref = db_client.collection('fusion_recipes').document(result_card_id)
        doc = await doc_ref.get()

        if not doc.exists:
            logger.warning(f"Fusion recipe with ID '{result_card_id}' not found")
            raise HTTPException(status_code=404, detail=f"Fusion recipe with ID '{result_card_id}' not found")

        # Get the recipe data to find all ingredients
        recipe_data = doc.to_dict()

        # Remove fusion information from all ingredient cards
        if 'ingredients' in recipe_data and recipe_data['ingredients']:
            for ingredient_data in recipe_data['ingredients']:
                try:
                    # Get card_id and card_collection_id from ingredient data
                    card_id = ingredient_data.get('card_id')
                    card_collection_id = ingredient_data.get('card_collection_id')
                    quantity = ingredient_data.get('quantity', 0)
                    available_in_packages = ingredient_data.get('available_in_packages', [])

                    if card_id and card_collection_id:
                        try:
                            # Use update_card_information to handle collection metadata lookup
                            # First, get the current card data to check if it has used_in_fusion
                            from service.storage_service import clean_fusion_references
                            try:
                                    await clean_fusion_references(
                                        document_id=card_id,
                                        collection_name=card_collection_id,
                                        fusion_id_to_remove=result_card_id
                                    )

                                    logger.info(f"Removed fusion information from card '{card_id}' (with collection_id '{card_collection_id}') with metadata: quantity={quantity}, available_in_packages={available_in_packages}")
                            except HTTPException as e:
                                if e.status_code == 404:
                                    # If the card doesn't exist, log a warning but continue
                                    logger.warning(f"Card '{card_id}' (with collection_id '{card_collection_id}') not found when trying to remove fusion information")
                                else:
                                    # Re-raise other HTTP exceptions
                                    raise e
                        except Exception as e:
                            # Log the error but continue with other ingredients
                            logger.error(f"Error removing fusion information from ingredient card '{card_id}' (with collection_id '{card_collection_id}'): {e}", exc_info=True)
                except Exception as e:
                    # Log the error but continue with other ingredients
                    logger.error(f"Error removing fusion information from ingredient card: {e}", exc_info=True)
                    # We don't want to fail the whole recipe deletion if updating an ingredient fails
                    # So we just log the error and continue

        # Delete the document
        await doc_ref.delete()
        logger.info(f"Deleted fusion recipe '{result_card_id}'")

        return True
    except HTTPException:
        # Re-raise HTTPExceptions
        raise
    except Exception as e:
        logger.error(f"Error deleting fusion recipe '{result_card_id}' from Firestore: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not delete fusion recipe '{result_card_id}' from database.")
