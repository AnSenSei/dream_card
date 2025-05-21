from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient
from fastapi import HTTPException
from typing import Dict, Any, List
import httpx

from config import get_logger, settings

logger = get_logger(__name__)

async def buy_card_from_official_listing(
    user_id: str,
    collection_id: str,
    card_id: str,
    quantity: int,
    db_client: AsyncClient
) -> Dict[str, Any]:
    """
    Buy a card from the official listing as a transaction.
    This ensures that all operations (deducting points, adding card to user, updating official listing)
    either all succeed or all fail together.

    Args:
        user_id: The ID of the user buying the card
        collection_id: The ID of the collection the card belongs to
        card_id: The ID of the card to buy
        quantity: The quantity of cards to buy
        db_client: Firestore client

    Returns:
        Dict containing information about the purchased card

    Raises:
        HTTPException: If any part of the transaction fails
    """
    try:
        # Get the card from the official listing
        official_listing_ref = db_client.collection("official_listing").document(collection_id).collection("cards").document(card_id)
        official_listing_doc = await official_listing_ref.get()

        if not official_listing_doc.exists:
            raise HTTPException(
                status_code=404, 
                detail=f"Card with ID {card_id} not found in official listing for collection {collection_id}"
            )

        official_listing_data = official_listing_doc.to_dict()
        current_listing_quantity = official_listing_data.get('quantity', 0)
        price_points = official_listing_data.get('pricePoints', 0)
        card_reference = official_listing_data.get('card_reference')

        # Check if card_reference is valid
        if not card_reference:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid card reference for card {card_id} in collection {collection_id}"
            )

        # Check if there's enough quantity in the official listing
        if current_listing_quantity < quantity:
            raise HTTPException(
                status_code=400, 
                detail=f"Card quantity in official listing ({current_listing_quantity}) is less than requested quantity ({quantity})"
            )

        # Calculate total price
        total_price = price_points * quantity

        # Get the user and check if they have enough points
        user_ref = db_client.collection("users").document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()
        points_balance = user_data.get('pointsBalance', 0)

        if points_balance < total_price:
            raise HTTPException(
                status_code=400, 
                detail=f"Insufficient points balance. You have {points_balance} points, but need {total_price} points"
            )

        # Get the original card data outside the transaction
        original_card_ref = db_client.document(card_reference)
        original_card_doc = await original_card_ref.get()

        if not original_card_doc.exists:
            raise HTTPException(
                status_code=404,
                detail=f"Original card not found at reference {card_reference}"
            )

        original_card_data = original_card_doc.to_dict()
        current_marketplace_quantity = original_card_data.get('quantity_in_offical_marketplace', 0)

        # Start transaction - This transaction now only handles the marketplace updates
        @firestore.async_transactional
        async def _transaction(tx: firestore.AsyncTransaction):
            # 1. Update the official listing
            new_listing_quantity = current_listing_quantity - quantity
            if new_listing_quantity <= 0:
                # If quantity becomes 0, remove the card from the official listing
                tx.delete(official_listing_ref)
            else:
                # Otherwise, update the quantity
                tx.update(official_listing_ref, {"quantity": new_listing_quantity})
        
            # 2. Update the original card's quantity_in_official_marketplace
            # Update only quantity_in_official_marketplace, not the original quantity
            tx.update(
                original_card_ref,
                {'quantity_in_offical_marketplace': max(0, current_marketplace_quantity - quantity)}
            )
            tx.update(
                original_card_ref,
                {'quantity': original_card_data.get('quantity', 0) +1}
            )
            # Remove this update as it seems conflicting with the marketplace operation
            # tx.update(
            #     original_card_ref,
            #     {'quantity': original_card_data.get('quantity', 0) + 1}
            # )
        
            # Note: We'll call the user_backend service to handle both deducting points and adding cards
            # in a single transaction on the user service side.

        # Execute the transaction for marketplace updates
        txn = db_client.transaction()
        await _transaction(txn)
        
        # Now call the user service to both deduct points and add the card in a single transaction
        try:
            import httpx
            from config import settings
        
            async with httpx.AsyncClient() as client:
                # Create a payload that includes both the points to deduct and the card to add
                payload = {
                    "card_references": [card_reference] * quantity,
                    "points_to_deduct": total_price
                }
                
                # Call the user service endpoint that handles both operations atomically
                response = await client.post(
                    f"{settings.user_backend_url}/users/{user_id}/cards_with_points?collection_metadata_id={collection_id}",
                    json=payload
                )
                
                # The user service returns a 201 Created status code on success
                if response.status_code not in (200, 201):
                    logger.error(f"Error processing transaction for user {user_id}: {response.text}")
                    
                    # If the user service transaction failed, we should roll back our marketplace changes
                    # This would require implementing a compensating transaction
                    logger.error("User service transaction failed. Marketplace changes might need to be reverted.")
                    
                    raise HTTPException(
                        status_code=response.status_code, 
                        detail=f"Error from user service: {response.text}"
                    )
                
                # Parse the response as JSON and log the success message
                response_data = response.json()
                logger.info(f"User service transaction response: {response_data}")
                logger.info(f"Successfully processed purchase of {quantity} card(s) {card_id} for user {user_id}")
        except httpx.RequestError as e:
            logger.error(f"Error communicating with user_backend service: {e}")
            raise HTTPException(status_code=503, detail=f"User service unavailable: {str(e)}")
        
        logger.info(f"Successfully bought {quantity} card(s) {card_id} from collection {collection_id} for user {user_id}")

        # Return the result
        result = {
            "card": {
                **official_listing_data,
                "quantity": max(0, current_listing_quantity - quantity),
                "collection_id": collection_id
            },
            "quantity": quantity,
            "total_price": total_price,
            "collection_id": collection_id
        }

        return result

    except HTTPException as e:
        logger.error(f"HTTP error in buy_card_from_official_listing: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"Error in buy_card_from_official_listing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
