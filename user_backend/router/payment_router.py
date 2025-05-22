from fastapi import APIRouter, HTTPException, Depends, Path, Body, Request, Header
from google.cloud import firestore
from typing import Optional

from models.payment_schemas import CreatePaymentIntentRequest, PaymentIntentResponse, WebhookResponse
from service.payment_service import create_payment_intent, handle_stripe_webhook
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["payments"],
)

@router.post("/{user_id}/payment/create-intent", response_model=PaymentIntentResponse)
async def create_payment_intent_route(
    user_id: str = Path(..., description="The ID of the user making the payment"),
    request: CreatePaymentIntentRequest = Body(..., description="Payment intent request details"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a payment intent for a user.

    This endpoint:
    1. Takes a user ID and payment details
    2. Creates a payment intent using Stripe
    3. Returns the payment intent details including client_secret

    The client_secret can be used on the frontend to complete the payment using Stripe Elements or other Stripe libraries.
    """
    try:
        payment_intent = await create_payment_intent(
            user_id=user_id,
            amount=request.amount,
            currency=request.currency,
            metadata=request.metadata,
            db_client=db
        )

        # Convert to response model
        return PaymentIntentResponse(
            id=payment_intent["id"],
            client_secret=payment_intent["client_secret"],
            amount=payment_intent["amount"],
            currency=payment_intent["currency"],
            status=payment_intent["status"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating payment intent for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the payment intent")

@router.post("/webhook", response_model=WebhookResponse)
async def stripe_webhook_route(
    request: Request,
    stripe_signature: str = Header(..., alias="Stripe-Signature"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Handle Stripe webhook events.

    This endpoint:
    1. Receives webhook events from Stripe
    2. Verifies the Stripe signature
    3. Processes payment_intent.succeeded events
    4. Adds points to the user's account
    5. Records the transaction in the database

    The webhook URL should be configured in the Stripe dashboard to point to this endpoint.
    
    Stripe will retry webhooks that receive a 5xx response up to 3 times with exponential backoff.
    We deliberately return 500 status when we want Stripe to retry (e.g., if user points addition fails).
    """
    try:
        result = await handle_stripe_webhook(request, stripe_signature, db)
        return WebhookResponse(status=result.get("status", "success"), details=result)
    except HTTPException as e:
        # For status 500, log as warning since we expect Stripe to retry
        if e.status_code == 500:
            logger.warning(f"Returning 500 to Stripe to trigger a retry: {e.detail}")
        else:
            logger.error(f"HTTP error processing Stripe webhook: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Error processing Stripe webhook: {e}", exc_info=True)
        # Return 500 to cause Stripe to retry the webhook
        raise HTTPException(
            status_code=500, 
            detail="An error occurred while processing the webhook, Stripe should retry"
        )
