from fastapi import APIRouter, HTTPException, Query

from service.storage_service import add_to_official_listing
from config import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/marketplace",
    tags=["marketplace"],
    responses={404: {"description": "Not found"}},
)

@router.post("/official_listing")
async def add_to_official_listing_endpoint(
    collection_id: str = Query(..., description="Collection ID the card belongs to"),
    card_id: str = Query(..., description="Card ID to add to the official listing"),
    quantity: int = Query(1, description="Quantity of cards to add to the official listing"),
    pricePoints: int = Query(..., description="Price in points for the card in the official listing"),
    priceCash: int = Query(0, description="Price in cash for the card in the official listing")
):
    """
    Adds a card to the official_listing collection.
    Creates a new collection called "official_listing" if it doesn't exist.
    Adds a subcollection with the provided collection_id.
    Adds the card under that subcollection with the specified fields.

    Parameters:
    - collection_id: The ID of the collection the card belongs to
    - card_id: The ID of the card to add to the official listing
    - quantity: The quantity of cards to add to the official listing (default: 1)
    - pricePoints: The price in points for the card in the official listing (required)
    - priceCash: The price in cash for the card in the official listing (default: 0)
    """
    try:
        result = await add_to_official_listing(collection_id, card_id, quantity, pricePoints, priceCash)
        return {
            "status": "success",
            "message": f"Card {card_id} from collection {collection_id} added to official listing with quantity {quantity}, pricePoints {pricePoints}, and priceCash {priceCash}",
            "data": result
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in add_to_official_listing_endpoint: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
