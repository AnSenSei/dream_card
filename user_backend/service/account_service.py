from typing import Optional, Dict, List, Tuple, Any
from uuid import UUID
import random
import re
import secrets
from datetime import datetime, timedelta

from fastapi import HTTPException
from shippo import Shippo
from shippo.models import components
from google.cloud import firestore
from google.cloud.firestore_v1 import AsyncClient, SERVER_TIMESTAMP, Increment

from config import get_logger, settings
from models.schemas import User, Address, CreateAccountRequest
from utils.gcs_utils import generate_signed_url, upload_avatar_to_gcs, parse_base64_image

logger = get_logger(__name__)

async def validate_address_with_shippo(address: Address) -> bool:
    """
    Validate an address using the latest Shippo Python SDK.

    Args:
        address: The address object to validate (including name)

    Returns:
        True if the address is valid, False otherwise

    Raises:
        HTTPException: If there's an error validating the address
    """
    try:
        # Configure Shippo SDK
        if not hasattr(settings, 'shippo_api_key') or not settings.shippo_api_key:
            logger.error("Shippo API key not configured")
            raise HTTPException(status_code=500, detail="Address validation service not configured")

        # Initialize the Shippo SDK with API key
        shippo_sdk = Shippo(
            api_key_header=settings.shippo_api_key
        )

        # Create address object for validation using the new SDK structure
        logger.info(f"Validating address for {address.name}: {address.street}, {address.city}, {address.state}, {address.zip}, {address.country}")

        # Create and validate the address using the new SDK
        validation_result = shippo_sdk.addresses.create(
            components.AddressCreateRequest(
                name=address.name,
                street1=address.street,
                city=address.city,
                state=address.state,
                zip=address.zip,
                country=address.country,
                validate=True
            )
        )

        # Check if validation was successful
        if not validation_result:
            raise HTTPException(status_code=400, detail="Address validation failed: No response from service")

        # Access validation results from the response
        validation_results = validation_result.validation_results

        if not validation_results:
            logger.warning(f"No validation results returned for address: {validation_result}")
            return True  # If no validation results, consider it valid (some addresses may not support validation)

        # Check if the address is valid
        is_valid = validation_results.is_valid

        if not is_valid:
            # Get validation messages for detailed error reporting
            error_messages = []

            messages = validation_results.messages or []
            for msg in messages:
                error_text = msg.text if hasattr(msg, 'text') else str(msg)
                error_code = msg.code if hasattr(msg, 'code') else ""
                if error_code:
                    error_messages.append(f"{error_code}: {error_text}")
                else:
                    error_messages.append(error_text)

            error_detail = "; ".join(error_messages) if error_messages else "Address validation failed"
            logger.warning(f"Address validation failed for {address.name}: {error_detail}")
            raise HTTPException(status_code=400, detail=f"Address validation failed: {error_detail}")

        logger.info(f"Address validated successfully for {address.name}")
        return True

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Handle other errors
        logger.error(f"Unexpected error validating address: {e}", exc_info=True)
        error_msg = str(e)

        # Check if it's a Shippo-specific error
        if "shippo" in error_msg.lower() or "api" in error_msg.lower():
            raise HTTPException(status_code=400, detail=f"Address validation failed: {error_msg}")

        # Check if it's a network/connection error
        if any(keyword in error_msg.lower() for keyword in ['connection', 'timeout', 'network', 'dns']):
            raise HTTPException(status_code=503, detail="Address validation service temporarily unavailable")

        # Check if it's an authentication error
        if any(keyword in error_msg.lower() for keyword in ['unauthorized', 'forbidden', 'authentication', 'api key']):
            raise HTTPException(status_code=500, detail="Address validation service configuration error")

        raise HTTPException(status_code=500, detail=f"Failed to validate address: {error_msg}")

async def check_user_referred(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Check if a user has been referred (has the referred_by field).

    Args:
        user_id: The ID of the user to check
        db_client: Firestore client

    Returns:
        Dict containing user_id, is_referred status, and referer_id if referred

    Raises:
        HTTPException: If there's an error checking the user
    """
    try:
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()
        is_referred = 'referred_by' in user_data and user_data['referred_by'] is not None
        referer_id = user_data.get('referred_by') if is_referred else None

        return {
            "user_id": user_id,
            "is_referred": is_referred,
            "referer_id": referer_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking if user {user_id} has been referred: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to check user referral status: {str(e)}")

async def get_user_referrals(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Get all users referred by a specific user.

    Args:
        user_id: The ID of the user to get referrals for
        db_client: Firestore client

    Returns:
        Dict containing user_id, total_referred, and a list of referred users

    Raises:
        HTTPException: If there's an error getting the referrals
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get all documents from the user's "refers" subcollection
        refers_ref = user_ref.collection("refers")
        refers_docs = await refers_ref.get()

        referred_users = []
        for doc in refers_docs:
            referred_user_data = doc.to_dict()
            referred_users.append({
                "user_id": doc.id,
                "points_recharged": referred_user_data.get("points_recharged", 0),
                "first_recharge_at": referred_user_data.get("first_recharge_at"),
                "last_recharge_at": referred_user_data.get("last_recharge_at")
            })

        return {
            "user_id": user_id,
            "total_referred": len(referred_users),
            "referred_users": referred_users
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting referrals for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get user referrals: {str(e)}")

async def get_user_refer_code(user_id: str, db_client: AsyncClient) -> Dict[str, Any]:
    """
    Get a user's referral code.

    Args:
        user_id: The ID of the user to get the referral code for
        db_client: Firestore client

    Returns:
        Dict containing user_id and refer_code

    Raises:
        HTTPException: If there's an error getting the referral code
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Query the refer_codes collection where referer_id equals the user_id
        refer_codes_ref = db_client.collection('refer_codes')
        query = refer_codes_ref.where('referer_id', '==', user_id)
        refer_codes_docs = await query.get()

        # If no referral code is found, return an error
        if not refer_codes_docs:
            raise HTTPException(status_code=404, detail=f"No referral code found for user with ID {user_id}")

        # Get the first document (there should only be one)
        refer_code_doc = refer_codes_docs[0]
        refer_code = refer_code_doc.id

        return {
            "user_id": user_id,
            "refer_code": refer_code
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting referral code for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get user referral code: {str(e)}")

async def get_user_by_id(user_id: str, db_client: AsyncClient) -> Optional[User]:
    """
    Get a user by ID from Firestore.

    Args:
        user_id: The ID of the user to get
        db_client: Firestore client

    Returns:
        The user if found, None otherwise

    Raises:
        HTTPException: If there's an error getting the user
    """
    try:
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            return None

        user_data = user_doc.to_dict()

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

        return User(**user_data)
    except Exception as e:
        logger.error(f"Error getting user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get user: {str(e)}")

async def update_user_email_and_address(user_id: str, email: Optional[str] = None, db_client: AsyncClient = None, avatar: Optional[Any] = None, addresses: Optional[List[Address]] = None) -> User:
    """
    Update a user's email and avatar fields.

    Args:
        user_id: The ID of the user to update
        email: Optional email address to update
        db_client: Firestore client
        avatar: Optional image data for user's avatar (can be base64 encoded string or binary data as bytes)
        addresses: Optional list of address objects with id, street, city, state, zip, and country

    Returns:
        The updated User object with a signed URL for the avatar if it exists

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get user data for display name
        user_data = user_doc.to_dict()
        user_name = user_data.get("displayName", "User")

        # Prepare update data
        update_data = {}

        # Only update email if provided
        if email is not None:
            # Validate email format
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, email):
                raise HTTPException(status_code=400, detail="Invalid email format")
            update_data["email"] = email

        # Convert Address objects to dictionaries for Firestore if provided
        if addresses is not None:
            # Validate each address with Shippo before updating
            for address in addresses:
                await validate_address_with_shippo(address)

            address_dicts = [address.model_dump() for address in addresses]
            update_data["addresses"] = address_dicts

        # Handle avatar upload if provided
        if avatar is not None:
            try:
                import base64

                if isinstance(avatar, str) and avatar.strip():  # String avatar
                    if avatar.startswith('data:'):
                        # Handle base64 encoded data URI
                        content_type, base64_data = parse_base64_image(avatar)
                        avatar_bytes = base64.b64decode(base64_data)
                        avatar_gcs_uri = await upload_avatar_to_gcs(avatar_bytes, user_id, content_type)
                    else:
                        # Assume it's a base64 string without data URI prefix
                        try:
                            avatar_bytes = base64.b64decode(avatar)
                            avatar_gcs_uri = await upload_avatar_to_gcs(avatar_bytes, user_id)
                        except Exception:
                            # If it's not base64, treat it as a URL/string
                            update_data["avatar"] = avatar
                            avatar_gcs_uri = None
                elif isinstance(avatar, bytes):  # Binary data
                    # Handle bytes directly
                    avatar_gcs_uri = await upload_avatar_to_gcs(avatar, user_id)
                else:
                    # Unsupported avatar type
                    logger.warning(f"Unsupported avatar type: {type(avatar)}")
                    raise HTTPException(status_code=400, detail="Unsupported avatar format")

                if avatar_gcs_uri:
                    update_data["avatar"] = avatar_gcs_uri
            except HTTPException as e:
                # Re-raise the exception from upload_avatar_to_gcs
                raise e
            except Exception as e:
                logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

        # Only update if there's something to update
        if not update_data:
            raise HTTPException(status_code=400, detail="No fields provided to update")

        # Update the user's fields
        await user_ref.update(update_data)

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = updated_user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                updated_user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

        updated_user = User(**updated_user_data)

        logger.info(f"Updated user {user_id} with fields: {list(update_data.keys())}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update user: {str(e)}")

async def add_user_address(user_id: str, address: Address, db_client: AsyncClient) -> User:
    """
    Add a new address to a user's addresses.

    Args:
        user_id: The ID of the user to update
        address: The Address object to add
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error adding the address
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        user_data = user_doc.to_dict()
        current_addresses = user_data.get("addresses", [])

        # Convert Address object to dictionary for Firestore
        address_dict = address.model_dump()

        # If address doesn't have an ID, generate one
        if not address_dict.get("id"):
            address_dict["id"] = f"address_{len(current_addresses) + 1}"

        # Validate address with Shippo
        await validate_address_with_shippo(address)

        # Add the new address to the list
        current_addresses.append(address_dict)

        # Update the user's addresses field
        await user_ref.update({"addresses": current_addresses})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added address with ID {address_dict['id']} to user {user_id}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding address for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add address: {str(e)}")

async def add_points_to_user(user_id: str, points: int, db_client: AsyncClient) -> User:
    """
    Add points to a user's pointsBalance.

    Args:
        user_id: The ID of the user to update
        points: The number of points to add (must be greater than 0)
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Validate points
        if points <= 0:
            raise HTTPException(status_code=400, detail="Points must be greater than 0")

        # Update the user's pointsBalance
        await user_ref.update({
            "pointsBalance": firestore.Increment(points)
        })

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added {points} points to user {user_id}. New balance: {updated_user.pointsBalance}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error adding points to user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add points to user: {str(e)}")

async def add_points_and_update_cash_recharged(user_id: str, points: int, amount_dollars: float, db_client: AsyncClient) -> User:
    """
    Add points to a user's pointsBalance and update totalCashRecharged.

    Args:
        user_id: The ID of the user to update
        points: The number of points to add (must be greater than 0)
        amount_dollars: The amount of cash recharged in dollars
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Validate points
        if points <= 0:
            raise HTTPException(status_code=400, detail="Points must be greater than 0")

        # Validate amount
        if amount_dollars <= 0:
            raise HTTPException(status_code=400, detail="Amount must be greater than 0")

        # Convert amount_dollars to int for totalCashRecharged
        amount_int = int(amount_dollars)

        # Update the user's pointsBalance and totalCashRecharged
        await user_ref.update({
            "pointsBalance": firestore.Increment(points),
            "totalCashRecharged": firestore.Increment(amount_int)
        })

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Added {points} points to user {user_id}. New balance: {updated_user.pointsBalance}")
        logger.info(f"Updated totalCashRecharged for user {user_id} by ${amount_int}. New total: {updated_user.totalCashRecharged}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update user: {str(e)}")

async def update_seed(user_id: str, db_client: AsyncClient) -> User:
    """
    Update a user's clientSeed with a new random value.

    Args:
        user_id: The ID of the user to update
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Generate a new clientSeed
        new_seed = secrets.token_hex(16)

        # Update the user's clientSeed
        await user_ref.update({
            "clientSeed": new_seed
        })

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Updated clientSeed for user {user_id}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating clientSeed for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update clientSeed: {str(e)}")

async def create_account(request: CreateAccountRequest, db_client: AsyncClient, user_id: Optional[str] = None) -> User:
    """
    Create a new user account with the specified fields and default values.

    Args:
        request: The CreateAccountRequest object containing user data
        db_client: Firestore client
        user_id: Optional user ID. If not provided, a new UUID will be generated.

    Returns:
        The created User object

    Raises:
        HTTPException: If there's an error creating the user
    """
    try:
        # Generate a unique user ID if not provided
        if not user_id:
            user_id = str(UUID.uuid4())

        # Validate email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, request.email):
            raise HTTPException(status_code=400, detail="Invalid email format")

        # Get current timestamp
        now = datetime.now()

        # No need to generate month keys anymore

        # Handle avatar upload if provided
        avatar_url = request.avatar
        if avatar_url and not avatar_url.startswith(('http://', 'https://', 'gs://')):
            try:
                # Upload avatar to GCS
                avatar_url = await upload_avatar_to_gcs(avatar_url, user_id)
            except Exception as e:
                logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
                # Continue with account creation even if avatar upload fails
                avatar_url = None

        # Validate addresses with Shippo if any are provided
        if request.addresses:
            for address in request.addresses:
                await validate_address_with_shippo(address)

        # Convert Address objects to dictionaries for Firestore
        addresses = [address.model_dump() for address in request.addresses]

        # Generate clientSeed
        clientSeed = secrets.token_hex(16)

        # Create user data
        user_data = {
            "createdAt": now,
            "displayName": request.displayName,
            "email": request.email,
            "addresses": addresses,
            "avatar": avatar_url,
            "level": 1,
            "pointsBalance": 0,
            "totalCashRecharged": 0,
            "totalPointsSpent": 0,
            "totalFusion": request.totalFusion,
            "clientSeed": clientSeed,
            "total_point_refered": 0
        }

        # Create user document in Firestore
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        await user_ref.set(user_data)

        # Generate a random 6-8 character referral code (including numbers and letters)
        code_length = random.randint(6, 8)
        refer_code = ''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', k=code_length))

        # Create a document in the refer_codes collection with the referral code as the document ID
        refer_code_data = {
            "user": user_id,
            "referer_id": user_id
        }

        # Create refer_codes document in Firestore
        refer_code_ref = db_client.collection('refer_codes').document(refer_code)
        await refer_code_ref.set(refer_code_data)
        logger.info(f"Created referral code {refer_code} for user {user_id}")

        # Get the created user
        user_doc = await user_ref.get()
        user_data = user_doc.to_dict()
        user = User(**user_data)

        logger.info(f"Created new user account with ID {user_id}")
        return user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating user account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create user account: {str(e)}")


async def delete_user_address(user_id: str, address_id: str, db_client: AsyncClient) -> str:
    """
    Delete an address from a user's addresses list.

    Args:
        user_id: The ID of the user to update
        address_id: The ID of the address to delete
        db_client: Firestore client

    Returns:
        A success message

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Get current user data
        user_data = user_doc.to_dict()

        # Get current addresses
        current_addresses = user_data.get("addresses", [])

        # Find the address with the given ID
        address_found = False
        updated_addresses = []
        for addr in current_addresses:
            if addr.get("id") != address_id:
                updated_addresses.append(addr)
            else:
                address_found = True

        if not address_found:
            raise HTTPException(status_code=404, detail=f"Address with ID {address_id} not found for user {user_id}")

        # Update the user's addresses field
        await user_ref.update({"addresses": updated_addresses})

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()
        updated_user = User(**updated_user_data)

        logger.info(f"Deleted address with ID {address_id} from user {user_id}")
        return f"Successfully deleted address with ID {address_id} from user {user_id}"
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting address for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete address: {str(e)}")


async def update_user_avatar(user_id: str, avatar: bytes, content_type: str, db_client: AsyncClient) -> User:
    """
    Update a user's avatar.

    Args:
        user_id: The ID of the user to update
        avatar: Binary image data for user's avatar
        content_type: The content type of the image (e.g., "image/jpeg")
        db_client: Firestore client

    Returns:
        The updated User object

    Raises:
        HTTPException: If there's an error updating the user
    """
    try:
        # Check if user exists
        user_ref = db_client.collection(settings.firestore_collection_users).document(user_id)
        user_doc = await user_ref.get()

        if not user_doc.exists:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")

        # Handle avatar upload
        try:
            # Upload avatar to GCS
            avatar_gcs_uri = await upload_avatar_to_gcs(avatar, user_id, content_type)

            # Update the user's avatar field
            await user_ref.update({"avatar": avatar_gcs_uri})
        except HTTPException as e:
            # Re-raise the exception from upload_avatar_to_gcs
            raise e
        except Exception as e:
            logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

        # Get the updated user data
        updated_user_doc = await user_ref.get()
        updated_user_data = updated_user_doc.to_dict()

        # Generate a signed URL for the avatar if it's a GCS URI
        avatar_url = updated_user_data.get('avatar')
        if avatar_url and avatar_url.startswith('gs://'):
            try:
                updated_user_data['avatar'] = await generate_signed_url(avatar_url)
                logger.info(f"Generated signed URL for user {user_id}'s avatar")
            except Exception as e:
                logger.error(f"Failed to generate signed URL for user {user_id}'s avatar: {e}")
                # Keep the original avatar URL if signing fails

        updated_user = User(**updated_user_data)

        logger.info(f"Updated avatar for user {user_id}")
        return updated_user
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating avatar for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update avatar: {str(e)}")
