from typing import Dict, Any, Optional, Tuple
from fastapi import HTTPException, Request
import stripe
from google.cloud.firestore_v1 import AsyncClient
import json
from datetime import datetime

from config import get_logger, settings, execute_query
from service.user_service import add_points_to_user, add_points_and_update_cash_recharged
from config.db_connection import test_connection

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
    db_client: AsyncClient = None
) -> Dict[str, Any]:
    """
    Create a payment intent using Stripe.

    Args:
        user_id: The ID of the user making the payment
        amount: The amount to charge in cents (e.g., 1000 for $10.00)
        currency: The currency to use (default: usd)
        metadata: Additional metadata to attach to the payment intent
        db_client: Firestore client (optional, for future use)

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

        # Validate user_id
        if not user_id:
            logger.error(f"Payment {payment_id} has no user_id in metadata")
            # Return 400 which will cause Stripe to mark this as a permanent failure
            raise HTTPException(status_code=400, detail="Payment has no user_id in metadata")

        # Calculate points to add (e.g., $1 = 100 points)
        points_to_add = int(amount * POINTS_PER_DOLLAR / 100)  # Convert cents to points

        # Try to add points to user and update totalCashRecharged - wrap this in its own try block to handle separately
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

        # Import needed modules for transaction management
        from config.db_connection import db_connection

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
