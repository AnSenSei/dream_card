from fastapi import APIRouter, HTTPException, Depends, Path, Body, Request, Header
from google.cloud import firestore
from typing import Optional

from models.payment_schemas import CreatePaymentIntentRequest, CreateMarketplaceCashIntentRequest, PaymentIntentResponse, WebhookResponse, RechargeHistoryResponse, OnboardingLinkResponse, ConnectStatusResponse, StripeTaxStatusResponse, StripeDashboardLinkResponse, TaxConsentResponse
from service.payment_service import create_payment_intent, create_marketplace_intent, handle_stripe_webhook, get_user_recharge_history, create_stripe_connect_account, check_stripe_connect_status, check_stripe_tax_enabled, create_stripe_dashboard_link, update_tax_user_consent
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
            db_client=db,
            refer_code=request.refer_code
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

@router.post("/{user_id}/payment/create-marketplace-intent", response_model=PaymentIntentResponse)
async def create_marketplace_intent_route(
    user_id: str = Path(..., description="The ID of the user making the payment (buyer)"),
    offer_id: str = Body(..., description="The ID of the offer"),
    listing_id: str = Body(..., description="The ID of the listing"),
    buyer_address_id: str = Body(..., description="The ID of the buyer's address"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a marketplace payment intent for a user.

    This endpoint:
    1. Takes a user ID, offer ID, listing ID, and buyer address ID
    2. Creates a marketplace payment intent using Stripe Connect
    3. Returns the payment intent details including client_secret

    The client_secret can be used on the frontend to complete the payment using Stripe Elements or other Stripe libraries.
    """
    try:
        payment_intent = await create_marketplace_intent(
            user_id=user_id,
            offer_id=offer_id,
            listing_id=listing_id,
            buyer_address_id=buyer_address_id,
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
        logger.error(f"Error creating marketplace payment intent for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the marketplace payment intent")



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



@router.get("/{user_id}/recharge-history", response_model=RechargeHistoryResponse)
async def get_user_recharge_history_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a user's recharge history and total amount recharged.

    This endpoint:
    1. Takes a user ID
    2. Retrieves the user's total cash recharged from Firestore
    3. Retrieves the user's recharge history from the cash_recharges table
    4. Returns the combined information

    The recharge history includes details about each transaction such as
    amount, points granted, and timestamp.
    """
    try:
        recharge_info = await get_user_recharge_history(user_id, db)
        return RechargeHistoryResponse(**recharge_info)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving recharge history for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the recharge history")

@router.post("/{user_id}/stripe/connect/init", response_model=OnboardingLinkResponse)
async def create_stripe_connect_account_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a Stripe Connect Express account for a seller and generate an onboarding link.

    This endpoint:
    1. Takes a user ID
    2. Creates a Stripe Connect Express account if the user doesn't already have one
    3. Saves the Stripe account ID to the user's document in Firestore
    4. Generates an onboarding link for the user to complete the Connect onboarding process
    5. Returns the onboarding link URL

    If the user already has a Stripe Connect account:
    - If the account is fully onboarded, returns an empty URL
    - If the account is incomplete, generates a new onboarding link
    """
    try:
        result = await create_stripe_connect_account(user_id, db)
        return OnboardingLinkResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Stripe Connect account for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the Stripe Connect account")

@router.get("/{user_id}/stripe/connect/status", response_model=ConnectStatusResponse)
async def check_stripe_connect_status_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Check the status of a user's Stripe Connect account.

    This endpoint:
    1. Takes a user ID
    2. Checks if the user has a Stripe Connect account
    3. If they do, retrieves the account from Stripe to check its status
    4. Returns the status of the account:
       - "not_connected": User doesn't have a Stripe Connect account
       - "incomplete": User has started but not completed the onboarding process
       - "ready": User has completed onboarding and can receive payments
    """
    try:
        result = await check_stripe_connect_status(user_id, db)
        return ConnectStatusResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking Stripe Connect status for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while checking the Stripe Connect status")

@router.get("/{user_id}/stripe/tax/status", response_model=StripeTaxStatusResponse)
async def check_stripe_tax_status_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Check if automatic tax is enabled for a user's Stripe Connect account.

    This endpoint:
    1. Takes a user ID
    2. Checks if the user has a Stripe Connect account
    3. If they do, retrieves the account from Stripe to check if automatic tax is enabled
    4. Optionally updates the user's document in Firestore with the tax status
    5. Returns whether automatic tax is enabled for the account
    """
    try:
        result = await check_stripe_tax_enabled(user_id, db)
        return StripeTaxStatusResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking Stripe tax status for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while checking the Stripe tax status")

@router.get("/{user_id}/stripe/dashboard", response_model=StripeDashboardLinkResponse)
async def create_stripe_dashboard_link_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a login link for a user's Stripe Express dashboard.

    This endpoint:
    1. Takes a user ID
    2. Checks if the user has a Stripe Connect account
    3. If they do, creates a login link for the Stripe Express dashboard
    4. Returns the login URL
    """
    try:
        result = await create_stripe_dashboard_link(user_id, db)
        return StripeDashboardLinkResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Stripe dashboard link for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the Stripe dashboard link")

@router.post("/{user_id}/tax/consent", response_model=TaxConsentResponse)
async def tax_user_consented_route(
    user_id: str = Path(..., description="The ID of the user"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update a user's tax consent status to true.

    This endpoint:
    1. Takes a user ID
    2. Sets the tax_user_consented field to true for that user
    3. Returns a success response
    """
    try:
        result = await update_tax_user_consent(user_id, db)
        return TaxConsentResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating tax consent for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the tax consent")
