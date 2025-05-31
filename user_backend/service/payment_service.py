from typing import Dict, Any, Optional, Tuple
from fastapi import HTTPException, Request
import stripe
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, Increment as firestore_Increment, async_transactional
import json
from datetime import datetime

from config import get_logger, settings, execute_query
from service.card_service import add_card_to_user
from service.account_service import add_points_to_user,add_points_and_update_cash_recharged
from service.user_service import get_user_by_id
from service.marketplace_service import send_item_sold_email
from config.db_connection import test_connection, db_connection


# Initialize Stripe with the API key from settings
stripe.api_key = settings.stripe_api_key

# Define the points conversion rate (e.g., $1 = 100 points)
POINTS_PER_DOLLAR = 100

logger = get_logger(__name__)

# Verify database connection on module load
try:
    if test_connection():
        logger.info("Payment service successfully connected to the database")
    else:
        logger.error("Payment service could not connect to the database")
except Exception as e:
    logger.error(f"Error testing database connection: {str(e)}", exc_info=True)

def ensure_payment_tables_exist():
    """
    Ensure that the necessary database tables for payment processing exist.
    This function should be called during application startup.
    """
    try:
        # Create cash_recharges table if it doesn't exist
        execute_query(
            """
            CREATE TABLE IF NOT EXISTS cash_recharges (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                amount_cash DECIMAL(10, 2) NOT NULL,
                points_granted INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            fetch=False
        )
        logger.info("Ensured cash_recharges table exists")

        # Check if transactions table exists, and if not, create it
        # Note: This table might already exist for other transaction types
        execute_query(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                type VARCHAR(50) NOT NULL,
                amount_cash DECIMAL(10, 2),
                points_delta INTEGER NOT NULL,
                reference_id VARCHAR(255),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            fetch=False
        )
        logger.info("Ensured transactions table exists")

        return True
    except Exception as e:
        logger.error(f"Error ensuring payment tables exist: {str(e)}", exc_info=True)
        return False

async def create_payment_intent(
    user_id: str,
    amount: int,
    currency: str = "usd",
    metadata: Optional[Dict[str, Any]] = None,
    db_client: AsyncClient = None,
    refer_code: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a payment intent using Stripe.

    Args:
        user_id: The ID of the user making the payment
        amount: The amount to charge in cents (e.g., 1000 for $10.00)
        currency: The currency to use (default: usd)
        metadata: Additional metadata to attach to the payment intent
        db_client: Firestore client (optional, for future use)
        refer_code: Optional referral code to apply to this payment

    Returns:
        Dict containing the payment intent details including client_secret

    Raises:
        HTTPException: If there's an error creating the payment intent
    """
    try:
        # Get the user to ensure they exist
        if db_client:
            user_ref = db_client.collection("users").document(user_id)
            user_doc = await user_ref.get()

            if not user_doc.exists:
                raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Prepare metadata
        payment_metadata = {"user_id": user_id}
        if metadata:
            payment_metadata.update(metadata)

        # Process referral code if provided
        if refer_code and db_client:
            try:
                # Look up the referral code in the refer_codes collection
                refer_code_ref = db_client.collection('refer_codes').document(refer_code)
                refer_code_doc = await refer_code_ref.get()

                if refer_code_doc.exists:
                    refer_code_data = refer_code_doc.to_dict()
                    referer_id = refer_code_data.get('referer_id')

                    # Add referral information to payment metadata
                    payment_metadata["refer_code"] = refer_code
                    payment_metadata["referer_id"] = referer_id

                    logger.info(f"Applied referral code {refer_code} from user {referer_id} to payment for user {user_id}")
                else:
                    logger.warning(f"Invalid referral code {refer_code} provided for payment by user {user_id}")
            except Exception as e:
                logger.error(f"Error processing referral code {refer_code}: {str(e)}", exc_info=True)
                # Continue with payment even if referral code processing fails

        # Create the payment intent
        payment_intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=currency,
            metadata=payment_metadata,
            automatic_payment_methods={
                "enabled": True,
                "allow_redirects": "never"
            },
            # You can add additional parameters like receipt_email, description, etc.
        )

        # Log the payment intent creation
        logger.info(f"Created payment intent {payment_intent.id} for user {user_id} with amount {amount} {currency}")

        # Return the payment intent details
        return {
            "id": payment_intent.id,
            "client_secret": payment_intent.client_secret,
            "amount": payment_intent.amount,
            "currency": payment_intent.currency,
            "status": payment_intent.status
        }

    except stripe.error.StripeError as e:
        # Handle Stripe-specific errors
        logger.error(f"Stripe error creating payment intent for user {user_id}: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Stripe error: {str(e)}")
    except HTTPException as e:
        # Re-raise HTTP exceptions
        logger.error(f"HTTP error in create_payment_intent: {e.detail}")
        raise e
    except Exception as e:
        # Handle other exceptions
        logger.error(f"Error in create_payment_intent: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")


async def create_marketplace_intent(
    user_id: str,
    offer_id: str,
    listing_id: str,
    buyer_address_id: str,
    db_client: AsyncClient = None
) -> Dict[str, Any]:
    """
    Create a marketplace payment intent using Stripe Connect.

    Args:
        user_id: The ID of the user making the payment (buyer)
        offer_id: The ID of the offer
        listing_id: The ID of the listing
        buyer_address_id: The ID of the buyer's address
        db_client: Firestore client

    Returns:
        Dict containing the payment intent details including client_secret

    Raises:
        HTTPException: If there's an error creating the payment intent
    """
    try:
        # Get the buyer to ensure they exist
        if not db_client:
            raise HTTPException(status_code=500, detail="Database client is required")

        buyer_ref = db_client.collection("users").document(user_id)
        buyer_doc = await buyer_ref.get()

        if not buyer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Buyer with ID {user_id} not found")

        buyer_data = buyer_doc.to_dict()

        # Get the listing to ensure it exists and to get the seller information
        listing_ref = db_client.collection("listings").document(listing_id)
        listing_doc = await listing_ref.get()

        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()
        seller_ref_path = listing_data.get("owner_reference")

        # Get the seller to ensure they exist and to get their Stripe account ID
        # The owner_reference is a full document path, so we need to get a document reference directly
        seller_ref = db_client.document(seller_ref_path)
        seller_doc = await seller_ref.get()

        if not seller_doc.exists:
            raise HTTPException(status_code=404, detail=f"Seller with path {seller_ref_path} not found")

        seller_data = seller_doc.to_dict()
        stripe_account_id = seller_data.get("stripe_account_id")

        if not stripe_account_id:
            raise HTTPException(status_code=400, detail="Seller does not have a Stripe Connect account")

        # Get the offer to ensure it exists and to get the amount
        offer_ref = db_client.collection("listings").document(listing_id).collection("cash_offers").document(offer_id)
        offer_doc = await offer_ref.get()

        if not offer_doc.exists:
            raise HTTPException(status_code=404, detail=f"Offer with ID {offer_id} not found")

        offer_data = offer_doc.to_dict()
        amount = int(float(offer_data.get("amount", 0)) * 100)  # Convert dollars to cents

        if amount <= 0:
            raise HTTPException(status_code=400, detail="Offer amount must be greater than 0")

        # Calculate application fee (platform fee)
        application_fee_percentage = 0.10  # 10% platform fee
        application_fee_amount = int(amount * application_fee_percentage)

        # Prepare metadata
        metadata = {
            "buyer_id": user_id,
            "listing_id": listing_id,
            "offer_id": offer_id
        }

        # Create the payment intent
        payment_intent = stripe.PaymentIntent.create(
            amount=amount,
            currency="usd",
            application_fee_amount=application_fee_amount,
            metadata=metadata,
            customer=None,  # You can set this if you have a Stripe customer ID for the buyer
            stripe_account=stripe_account_id,  # Connect seller account
            automatic_payment_methods = {
                "enabled": True,
                "allow_redirects": "never"
            }

        )

        # Log the payment intent creation
        logger.info(f"Created marketplace payment intent {payment_intent.id} for user {user_id} with amount {amount} USD")

        # Return the payment intent details
        return {
            "id": payment_intent.id,
            "client_secret": payment_intent.client_secret,
            "amount": payment_intent.amount,
            "currency": payment_intent.currency,
            "status": payment_intent.status,
            "listing_id": listing_id,
            "offer_id": offer_id
        }

    except stripe.error.StripeError as e:
        # Handle Stripe-specific errors
        logger.error(f"Stripe error creating marketplace payment intent: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Stripe error: {str(e)}")
    except HTTPException as e:
        # Re-raise HTTP exceptions
        logger.error(f"HTTP error in create_marketplace_intent: {e.detail}")
        raise e
    except Exception as e:
        # Handle other exceptions
        logger.error(f"Error in create_marketplace_intent: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")




async def handle_stripe_webhook(request: Request, signature: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Handle Stripe webhook events, particularly payment_intent.succeeded.

    Args:
        request: The FastAPI request object containing the webhook payload
        signature: The Stripe signature from the request headers
        db_client: Firestore client

    Returns:
        Dict containing information about the processed webhook

    Raises:
        HTTPException: If there's an error processing the webhook. Status codes:
            - 400: For permanent failures (invalid payload, signature, etc.) - Stripe won't retry
            - 500: For temporary failures (database issues, etc.) - Stripe will retry up to 3 times
    """
    try:
        # Get the request body as bytes
        payload = await request.body()

        # Check if this is a retry from Stripe
        # Stripe adds a Stripe-Signature header with a timestamp and a signature
        # We can check if the signature has a 'retry-count' parameter
        is_retry = False
        if 'retry-count' in signature:
            retry_count = int(signature.split('retry-count=')[1].split(',')[0])
            logger.info(f"Processing a Stripe webhook retry attempt #{retry_count}")
            is_retry = True

        # Verify the webhook signature
        try:
            event = stripe.Webhook.construct_event(
                payload, signature, settings.stripe_webhook_secret
            )
        except ValueError as e:
            # Invalid payload - this is a permanent failure
            logger.error(f"Invalid Stripe webhook payload: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError as e:
            # Invalid signature - this is a permanent failure
            logger.error(f"Invalid Stripe webhook signature: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid signature")

        # Log webhook event details
        logger.info(f"Received Stripe webhook event: {event['type']}, ID: {event['id']}")

        # Handle the event
        if event['type'] == 'payment_intent.succeeded':
            # If this is a retry, log it
            if is_retry:
                logger.info(f"Processing a retry for payment_intent.succeeded event. ID: {event['id']}")

            return await handle_payment_succeeded(event['data']['object'], db_client)
        else:
            # Unhandled event type - this is not an error, just an event we don't process
            logger.info(f"Unhandled event type: {event['type']}")
            return {"status": "ignored", "type": event['type']}

    except HTTPException:
        # Re-raise HTTP exceptions to be handled by the route handler
        raise
    except Exception as e:
        logger.error(f"Error handling Stripe webhook: {str(e)}", exc_info=True)
        # Return 500 to trigger a Stripe retry
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred, Stripe should retry this webhook: {str(e)}"
        )

async def get_user_recharge_history(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Retrieve a user's recharge history and total amount recharged.

    Args:
        user_id: The ID of the user
        db_client: Firestore client

    Returns:
        Dict containing the user's recharge history and total

    Raises:
        HTTPException: If there's an error retrieving the data or user not found
    """
    try:
        # Validate user_id by checking if user exists in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            logger.error(f"User with ID {user_id} not found when fetching recharge history")
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()

        # Get total amount recharged from Firestore
        total_cash_recharged = user_data.get("totalCashRecharged", 0)

        # Query the cash_recharges table in PostgreSQL

        recharge_history = []
        with db_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT id, amount_cash, points_granted, created_at 
                    FROM cash_recharges 
                    WHERE user_id = %s 
                    ORDER BY created_at DESC
                    """,
                    (user_id,)
                )

                # Convert query results to a list of dictionaries
                recharge_records = cursor.fetchall()

                # Get column names
                column_names = [desc[0] for desc in cursor.description]

                # Create list of dictionaries from results
                for record in recharge_records:
                    recharge_dict = dict(zip(column_names, record))
                    # Format datetime for JSON serialization
                    if 'created_at' in recharge_dict and recharge_dict['created_at']:
                        recharge_dict['created_at'] = recharge_dict['created_at'].isoformat()
                    recharge_history.append(recharge_dict)

            except Exception as db_error:
                logger.error(f"Database error when fetching recharge history for user {user_id}: {str(db_error)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to retrieve recharge history: {str(db_error)}"
                )
            finally:
                cursor.close()

        # Return the user's recharge information
        return {
            "user_id": user_id,
            "total_cash_recharged": total_cash_recharged,
            "recharge_history": recharge_history
        }

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error retrieving recharge history for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def create_stripe_connect_account(user_id: str, db_client: AsyncClient) -> Dict[str, str]:
    """
    Create a Stripe Connect Express account for a seller and generate an onboarding link.

    Args:
        user_id: The ID of the user (seller)
        db_client: Firestore client

    Returns:
        Dict containing the onboarding URL

    Raises:
        HTTPException: If there's an error creating the account or generating the link
    """
    try:
        # Validate user_id by checking if user exists in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            logger.error(f"User with ID {user_id} not found when creating Stripe Connect account")
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()

        # Check if user already has a Stripe Connect account
        if user_data.get("stripe_account_id"):
            # If the user already has an account, retrieve it to check its status
            try:
                account = stripe.Account.retrieve(user_data["stripe_account_id"])

                # If the account exists and is not rejected, create a new account link
                if account.get("charges_enabled") and account.get("payouts_enabled"):
                    logger.info(f"User {user_id} already has a fully onboarded Stripe Connect account")
                    return {"onboarding_url": ""}  # Return empty URL for fully onboarded accounts

                # Create a new account link for incomplete accounts
                account_link = stripe.AccountLink.create(
                    account=user_data["stripe_account_id"],
                    type="account_onboarding",
                    refresh_url="https://zapull.com/stripe/complete",
                    return_url="https://zapull.com/stripe/complete"
                )

                logger.info(f"Created new onboarding link for existing Stripe Connect account for user {user_id}")
                return {"onboarding_url": account_link.url}

            except stripe.error.StripeError as e:
                # If the account doesn't exist anymore or there's another issue, create a new one
                logger.warning(f"Error retrieving Stripe account for user {user_id}: {str(e)}")
                # Continue to create a new account

        # Create a new Stripe Connect Express account
        account = stripe.Account.create(
            type="express",
            country="US",
            capabilities={"card_payments": {"requested": True},"transfers": {"requested": True}},
            metadata={"user_id": user_id}
        )

        logger.info(f"Created Stripe Connect account {account.id} for user {user_id}")

        # Save the account ID to the user's document in Firestore
        await user_ref.update({"stripe_account_id": account.id})

        # Create an account link for onboarding
        account_link = stripe.AccountLink.create(
            account=account.id,
            type="account_onboarding",
            refresh_url="https://zapull.com/stripe/complete",
            return_url="https://zapull.com/stripe/complete"
        )

        logger.info(f"Created onboarding link for Stripe Connect account for user {user_id}")

        return {"onboarding_url": account_link.url}

    except stripe.error.StripeError as e:
        logger.error(f"Stripe error creating Connect account for user {user_id}: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Stripe error: {str(e)}")
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error creating Stripe Connect account for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def check_stripe_tax_enabled(user_id: str, db_client: AsyncClient) -> Dict[str, bool]:
    """
    Check if automatic tax is enabled for a user's Stripe Connect account.

    Args:
        user_id: The ID of the user (seller)
        db_client: Firestore client

    Returns:
        Dict containing whether automatic tax is enabled for the Stripe account

    Raises:
        HTTPException: If there's an error checking the tax status
    """
    try:
        user_ref = db_client.collection("users").document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail="User not found")

        user_data = user_doc.to_dict()
        stripe_account_id = user_data.get("stripe_account_id")

        if not stripe_account_id:
            raise HTTPException(status_code=400, detail="User does not have a Stripe Connect account")

        try:
            account = stripe.Account.retrieve(stripe_account_id)
            tax_settings = account.get("settings", {}).get("tax", {})
            automatic_tax_enabled = tax_settings.get("automatic_tax", {}).get("enabled", False)

            # Optionally update the user document with the tax status
            if automatic_tax_enabled:
                await user_ref.update({"stripe_tax_enabled": True})

            return {
                "stripe_tax_enabled": automatic_tax_enabled
            }
        except stripe.error.StripeError as e:
            raise HTTPException(status_code=400, detail=f"Stripe error: {str(e)}")
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error checking Stripe tax status for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def create_stripe_dashboard_link(user_id: str, db_client: AsyncClient) -> Dict[str, str]:
    """
    Create a login link for a user's Stripe Express dashboard.

    Args:
        user_id: The ID of the user (seller)
        db_client: Firestore client

    Returns:
        Dict containing the login URL for the Stripe Express dashboard

    Raises:
        HTTPException: If there's an error creating the login link
    """
    try:
        user_ref = db_client.collection("users").document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail="User not found")

        user_data = user_doc.to_dict()
        stripe_account_id = user_data.get("stripe_account_id")

        if not stripe_account_id:
            raise HTTPException(status_code=400, detail="User does not have a Stripe Connect account")

        try:
            # Create a login link for the Stripe Express dashboard
            login_link = stripe.Account.create_login_link(stripe_account_id)

            return {
                "login_url": login_link.url
            }
        except stripe.error.StripeError as e:
            raise HTTPException(status_code=400, detail=f"Stripe error: {str(e)}")
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error creating Stripe dashboard link for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def handle_marketplace_payment(
    payment_id: str,
    amount: int,
    amount_dollars: float,
    currency: str,
    listing_id: str,
    buyer_id: str,
    offer_id: str,
    db_client: AsyncClient
) -> Dict[str, Any]:
    """
    Handle a successful marketplace payment intent event.
    This is similar to pay_point_offer but for cash payments.

    Args:
        payment_id: The payment intent ID from Stripe
        amount: The amount in cents
        amount_dollars: The amount in dollars
        currency: The currency code
        listing_id: The ID of the listing
        buyer_id: The ID of the buyer
        offer_id: The ID of the offer
        db_client: Firestore client

    Returns:
        Dict containing information about the processed payment

    Raises:
        HTTPException: If there's an error processing the payment
    """
    try:
        logger.info(f"Processing marketplace payment for listing {listing_id}, buyer {buyer_id}, offer {offer_id}")

        # 1. Verify listing exists
        listing_ref = db_client.collection('listings').document(listing_id)
        listing_doc = await listing_ref.get()
        if not listing_doc.exists:
            raise HTTPException(status_code=404, detail=f"Listing with ID {listing_id} not found")

        listing_data = listing_doc.to_dict()

        # 2. Get the seller information
        seller_ref_path = listing_data.get("owner_reference", "")
        if not seller_ref_path:
            raise HTTPException(status_code=500, detail="Invalid listing data: missing owner reference")

        seller_id = seller_ref_path.split('/')[-1]

        # 3. Get card information
        card_reference = listing_data.get("card_reference", "")
        collection_id = listing_data.get("collection_id", "")

        if not card_reference or not collection_id:
            raise HTTPException(status_code=500, detail="Invalid listing data: missing card reference or collection ID")

        # 4. Get the quantity to deduct from the listing
        quantity_to_deduct = 1  # Default to 1

        # 5. Create a transaction ID
        transaction_id = f"tx_{listing_id}_{offer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Get all point offers for this listing
        point_offers_ref = listing_ref.collection('point_offers')
        point_offers = await point_offers_ref.get()

        # Get all cash offers for this listing
        cash_offers_ref = listing_ref.collection('cash_offers')
        cash_offers = await cash_offers_ref.get()

        # 6. Execute the transaction
        @firestore.async_transactional
        async def _txn(tx: firestore.AsyncTransaction):
            # Delete the offer from the listing's cash_offers collection
            if offer_id:
                offer_ref = listing_ref.collection('cash_offers').document(offer_id)
                tx.delete(offer_ref)

                # e. Delete the user's offer from their my_cash_offers collection if it exists
                try:
                    buyer_ref = db_client.collection('users').document(buyer_id)
                    my_cash_offers_ref = buyer_ref.collection('my_cash_offers')
                    my_cash_offers_query = my_cash_offers_ref.where("listingId", "==", listing_id)
                    my_cash_offers_docs = await my_cash_offers_query.get()

                    for doc in my_cash_offers_docs:
                        my_offer_data = doc.to_dict()
                        # Check if this is the same offer by comparing offerreference
                        if my_offer_data.get("offerreference") == offer_id:
                            tx.delete(doc.reference)
                            break
                except Exception as e:
                    logger.error(f"Error deleting user's offer: {e}", exc_info=True)
                    # Continue with the transaction even if deleting the user's offer fails

            # a. Update the listing quantity
            current_quantity = listing_data.get("quantity", 0)
            new_quantity = current_quantity - quantity_to_deduct

            if new_quantity <= 0:
                # Delete all point offers for this listing
                for offer in point_offers:
                    tx.delete(point_offers_ref.document(offer.id))

                # Delete all cash offers for this listing
                for offer in cash_offers:
                    # We've already deleted the current offer above, so we can skip it here
                    if offer_id and offer.id == offer_id:
                        continue
                    tx.delete(cash_offers_ref.document(offer.id))

                # Delete the listing if quantity becomes zero
                tx.delete(listing_ref)
            else:
                # Update the listing quantity
                tx.update(listing_ref, {
                    "quantity": new_quantity
                })

            # b. Deduct locked_quantity from the seller's card
            try:
                # Parse card_reference to get card_id
                card_id = card_reference.split('/')[-1]

                # Get reference to the seller's card
                seller_ref = db_client.document(seller_ref_path)
                seller_card_ref = seller_ref.collection('cards').document('cards').collection(collection_id).document(card_id)

                # Get the seller's card to check current values
                seller_card_doc = await seller_card_ref.get()

                if seller_card_doc.exists:
                    seller_card_data = seller_card_doc.to_dict()
                    current_locked_quantity = seller_card_data.get('locked_quantity', 0)
                    current_card_quantity = seller_card_data.get('quantity', 0)

                    # Ensure we don't go below zero for locked_quantity
                    new_locked_quantity = max(0, current_locked_quantity - quantity_to_deduct)

                    # Check if both quantity and locked_quantity will be zero
                    if current_card_quantity == 0 and new_locked_quantity == 0:
                        # Delete the card from the seller's collection
                        tx.delete(seller_card_ref)
                        logger.info(f"Deleted card {card_id} from seller {seller_id}'s collection as both quantity and locked_quantity are zero")
                    else:
                        # Update the card with decremented locked_quantity
                        tx.update(seller_card_ref, {
                            'locked_quantity': new_locked_quantity
                        })
                        logger.info(f"Updated locked_quantity for card {card_id} in seller {seller_id}'s collection to {new_locked_quantity}")
            except Exception as e:
                logger.error(f"Error updating seller's card: {e}", exc_info=True)
                # Continue with the transaction even if updating the seller's card fails
                # This ensures the main transaction still completes

            # c. Create a marketplace transaction record
            transaction_ref = db_client.collection('marketplace_transactions').document(transaction_id)
            transaction_data = {
                "id": transaction_id,
                "listing_id": listing_id,
                "seller_id": seller_id,
                "buyer_id": buyer_id,
                "card_id": card_reference.split('/')[-1],
                "quantity": quantity_to_deduct,
                "price_points": None,
                "price_cash": amount_dollars,
                "price_card_id": None,
                "price_card_qty": None,
                "traded_at": datetime.now()
            }
            tx.set(transaction_ref, transaction_data)

        # Execute the transaction
        transaction = db_client.transaction()
        await _txn(transaction)

        # 7. Insert data into the marketplace_transactions SQL table
        # Use a single database connection for the SQL operation to ensure transaction integrity
        with db_connection() as conn:
            cursor = conn.cursor()
            try:
                # Begin transaction
                conn.autocommit = False

                # Record the transaction in marketplace_transactions table
                cursor.execute(
                    """
                    INSERT INTO marketplace_transactions (listing_id, seller_id, buyer_id, card_id, quantity, price_points, price_cash, price_card_id, price_card_qty, traded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (listing_id, seller_id, buyer_id, card_reference.split('/')[-1], quantity_to_deduct, None, int(amount_dollars), None, None, datetime.now())
                )
                sql_transaction_id = cursor.fetchone()[0]
                logger.info(f"Created marketplace transaction record with ID {sql_transaction_id}")

                # Commit the transaction
                conn.commit()
                logger.info(f"Successfully committed SQL database transaction for marketplace transaction {transaction_id}")
                logger.info(f"Recorded marketplace transaction: listing {listing_id}, seller {seller_id}, buyer {buyer_id}, cash {amount_dollars}")

            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"SQL database transaction failed, rolling back: {str(e)}", exc_info=True)
                # Continue with the response - we've already completed the Firestore transaction,
                # so we don't want to fail the whole operation just because of a database issue
                logger.warning("SQL database transaction failed but Firestore transaction was successful")

            finally:
                # Close cursor (connection will be closed by context manager)
                cursor.close()

        # 8. Add the card to the user's collection
        try:
            await add_card_to_user(
                user_id=buyer_id,
                card_reference=card_reference,
                db_client=db_client,
                collection_metadata_id=collection_id,
                from_market_place = True,
            )
        except Exception as e:
            logger.error(f"Error adding card to user {buyer_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Transaction completed but failed to add card to user: {str(e)}")

        # 9. Send email notification to the seller
        try:
            # Get the seller's user details
            seller = await get_user_by_id(seller_id, db_client)

            # Get the buyer's user details
            buyer = await get_user_by_id(buyer_id, db_client)

            if seller and seller.email:
                # Send the email notification
                await send_item_sold_email(
                    to_email=seller.email,
                    to_name=seller.displayName,
                    listing_data=listing_data,
                    offer_type="cash",
                    offer_amount=amount_dollars,
                    buyer_name=buyer.displayName if buyer else "a user"
                )
                logger.info(f"Sent item sold email to {seller.email}")
            else:
                logger.warning(f"Could not send email notification: Seller {seller_id} not found or has no email")
        except Exception as e:
            # Log the error but don't fail the whole operation if email sending fails
            logger.error(f"Error sending item sold email: {e}", exc_info=True)

        logger.info(f"Successfully processed marketplace payment for listing {listing_id}, buyer {buyer_id}, offer {offer_id}")
        return {
            "status": "success",
            "payment_id": payment_id,
            "transaction_id": transaction_id,
            "listing_id": listing_id,
            "offer_id": offer_id,
            "buyer_id": buyer_id,
            "seller_id": seller_id,
            "amount": amount,
            "currency": currency
        }

    except HTTPException as e:
        # Re-raise HTTP exceptions to be handled by the FastAPI exception handler
        raise
    except Exception as e:
        logger.error(f"Error handling marketplace payment: {str(e)}", exc_info=True)
        # Return a 500 status code which will cause Stripe to retry the webhook
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred, Stripe should retry this webhook: {str(e)}"
        )

async def update_tax_user_consent(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Update a user's tax consent status to true.

    Args:
        user_id: The ID of the user
        db_client: Firestore client

    Returns:
        Dict containing success status and user ID

    Raises:
        HTTPException: If there's an error updating the user's tax consent
    """
    try:
        user_ref = db_client.collection("users").document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail="User not found")

        # Update the user document with tax_user_consented=True
        await user_ref.update({"tax_user_consented": True})

        logger.info(f"Updated tax consent for user {user_id}")

        return {
            "success": True,
            "user_id": user_id
        }
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error updating tax consent for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def check_stripe_connect_status(user_id: str, db_client: AsyncClient) -> Dict[str, str]:
    """
    Check the status of a user's Stripe Connect account.

    Args:
        user_id: The ID of the user (seller)
        db_client: Firestore client

    Returns:
        Dict containing the status of the Stripe Connect account

    Raises:
        HTTPException: If there's an error checking the account status
    """
    try:
        # Validate user_id by checking if user exists in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            logger.error(f"User with ID {user_id} not found when checking Stripe Connect status")
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()

        # Check if user has a Stripe Connect account
        stripe_account_id = user_data.get("stripe_account_id")

        if not stripe_account_id:
            logger.info(f"User {user_id} does not have a Stripe Connect account")
            return {"status": "not_connected"}

        # Retrieve the Stripe account to check its status
        try:
            account = stripe.Account.retrieve(stripe_account_id)

            # Check if the account is fully onboarded (can accept charges and receive payouts)
            if account.charges_enabled and account.payouts_enabled:
                logger.info(f"User {user_id} has a fully onboarded Stripe Connect account")
                return {"status": "ready"}
            else:
                logger.info(f"User {user_id} has an incomplete Stripe Connect account")
                return {"status": "incomplete"}

        except stripe.error.StripeError as e:
            logger.error(f"Stripe error retrieving Connect account for user {user_id}: {str(e)}")
            # If the account doesn't exist or there's another issue, consider it not connected
            return {"status": "not_connected"}

    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error checking Stripe Connect status for user {user_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def handle_payment_succeeded(payment_intent: Dict[str, Any], db_client: AsyncClient) -> Dict[str, Any]:
    """
    Handle a successful payment intent event.

    Args:
        payment_intent: The payment intent object from Stripe
        db_client: Firestore client

    Returns:
        Dict containing information about the processed payment

    Raises:
        HTTPException: If there's an error processing the payment
    """
    try:
        # Extract payment details
        payment_id = payment_intent['id']
        amount = payment_intent['amount']  # Amount in cents
        amount_dollars = amount / 100.0    # Convert to dollars for readability
        currency = payment_intent['currency']
        metadata = payment_intent.get('metadata', {})
        user_id = metadata.get('user_id')

        # Check if this is a marketplace payment
        listing_id = metadata.get('listing_id')
        buyer_id = metadata.get('buyer_id')
        offer_id = metadata.get('offer_id')

        # If this is a marketplace payment, handle it differently
        if listing_id and buyer_id:
            return await handle_marketplace_payment(
                payment_id=payment_id,
                amount=amount,
                amount_dollars=amount_dollars,
                currency=currency,
                listing_id=listing_id,
                buyer_id=buyer_id,
                offer_id=offer_id,
                db_client=db_client
            )

        # Validate user_id
        if not user_id:
            logger.error(f"Payment {payment_id} has no user_id in metadata")
            # Return 400 which will cause Stripe to mark this as a permanent failure
            raise HTTPException(status_code=400, detail="Payment has no user_id in metadata")

        # Calculate points to add (e.g., $1 = 100 points)
        points_to_add = int(amount * POINTS_PER_DOLLAR / 100)  # Convert cents to points

        # Check if this payment has referral information
        referer_id = metadata.get('referer_id')
        refer_code = metadata.get('refer_code')

        # Apply referral bonus if applicable
        if referer_id and refer_code:
            # Give the referred user (current user) 5% more points
            referral_bonus = int(points_to_add * 0.05)
            original_points = points_to_add
            points_to_add += referral_bonus

            logger.info(f"User {user_id} was referred by {referer_id}. Adding 5% bonus: {referral_bonus} extra points")

            # Calculate points for the referrer (5% of the original points granted to the new user)
            referrer_points = int(original_points * 0.05)

            # Try to add points to user and update totalCashRecharged - wrap this in its own try block to handle separately
            try:
                # Add points to user and update totalCashRecharged
                updated_user = await add_points_and_update_cash_recharged(user_id, points_to_add, amount_dollars, db_client)
                logger.info(f"Successfully added {points_to_add} points to user {user_id} and updated totalCashRecharged")

                # Update the user's referred_by field if not already set
                user_ref = db_client.collection("users").document(user_id)
                user_doc = await user_ref.get()
                user_data = user_doc.to_dict()

                if not user_data.get('referred_by'):
                    await user_ref.update({
                        "referred_by": referer_id
                    })
                    logger.info(f"Updated user {user_id} with referred_by: {referer_id}")

                # Add points to the referrer
                try:
                    await add_points_to_user(referer_id, referrer_points, db_client)
                    logger.info(f"Added {referrer_points} points to referrer {referer_id}")

                    # Add the new user's ID and recharged points to the referrer's refers subcollection
                    referrer_ref = db_client.collection("users").document(referer_id)
                    refers_ref = referrer_ref.collection("refers").document(user_id)

                    # Check if the document already exists
                    refers_doc = await refers_ref.get()

                    if refers_doc.exists:
                        # Update existing document
                        await refers_ref.update({
                            "points_recharged": firestore_Increment(original_points),
                            "last_recharge_at": datetime.now()
                        })
                    else:
                        # Create new document
                        await refers_ref.set({
                            "user_id": user_id,
                            "points_recharged": original_points,
                            "first_recharge_at": datetime.now(),
                            "last_recharge_at": datetime.now()
                        })

                    # Update the total_point_refered field for the referrer
                    await referrer_ref.update({
                        "total_point_refered": firestore_Increment(original_points)
                    })

                    logger.info(f"Updated referrer {referer_id}'s refers collection with user {user_id} and incremented total_point_refered by {original_points}")

                except Exception as referrer_error:
                    logger.error(f"Failed to update referrer {referer_id}: {str(referrer_error)}", exc_info=True)
                    # Continue processing even if updating the referrer fails

            except Exception as points_error:
                logger.error(f"Failed to update user {user_id}: {str(points_error)}", exc_info=True)
                # Return a 500 status code which will cause Stripe to retry the webhook
                raise HTTPException(
                    status_code=500, 
                    detail=f"Failed to update user, Stripe should retry this webhook: {str(points_error)}"
                )
        else:
            # No referral - normal flow
            try:
                # Add points to user and update totalCashRecharged
                updated_user = await add_points_and_update_cash_recharged(user_id, points_to_add, amount_dollars, db_client)
                logger.info(f"Successfully added {points_to_add} points to user {user_id} and updated totalCashRecharged")
            except Exception as points_error:
                logger.error(f"Failed to update user {user_id}: {str(points_error)}", exc_info=True)
                # Return a 500 status code which will cause Stripe to retry the webhook
                raise HTTPException(
                    status_code=500, 
                    detail=f"Failed to update user, Stripe should retry this webhook: {str(points_error)}"
                )

        # Use db_connection for database operations

        # Use a single database connection for both operations to ensure transaction integrity
        with db_connection() as conn:
            cursor = conn.cursor()
            try:
                # Begin transaction
                conn.autocommit = False

                # Record the transaction in cash_recharges table
                cursor.execute(
                    """
                    INSERT INTO cash_recharges (user_id, amount_cash, points_granted, created_at)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (user_id, amount_dollars, points_to_add, datetime.now())
                )
                recharge_id = cursor.fetchone()[0]
                logger.info(f"Created cash recharge record with ID {recharge_id} for user {user_id}")

                # Record the transaction in transactions table
                cursor.execute(
                    """
                    INSERT INTO transactions (user_id, type, amount_cash, points_delta, reference_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, "payment", amount_dollars, points_to_add, str(recharge_id), datetime.now())
                )

                # Commit the transaction
                conn.commit()
                logger.info(f"Successfully committed database transaction for payment {payment_id}")
                logger.info(f"Recorded cash recharge for user {user_id}: ${amount_dollars} = {points_to_add} points")
                logger.info(f"Recorded transaction for user {user_id}: payment of ${amount_dollars}")

            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"Database transaction failed, rolling back: {str(e)}", exc_info=True)
                # Continue with the response - we've already added points to the user,
                # so we don't want to fail the whole operation just because of a database issue
                logger.warning("Database transaction failed but points were already added to user")

            finally:
                # Close cursor (connection will be closed by context manager)
                cursor.close()

        return {
            "status": "success",
            "payment_id": payment_id,
            "user_id": user_id,
            "amount": amount,
            "currency": currency,
            "points_added": points_to_add,
            "new_points_balance": updated_user.pointsBalance
        }

    except HTTPException:
        # Re-raise HTTP exceptions to be handled by the FastAPI exception handler
        raise
    except Exception as e:
        logger.error(f"Error handling payment succeeded event: {str(e)}", exc_info=True)
        # Return a 500 status code which will cause Stripe to retry the webhook
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred, Stripe should retry this webhook: {str(e)}"
        )
