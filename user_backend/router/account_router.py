from fastapi import APIRouter, HTTPException, Depends, Path, Query, Body, File, UploadFile
from typing import Optional, List
from google.cloud import firestore

from models.schemas import User, Address, CreateAccountRequest, UserEmailAddressUpdate, AddPointsRequest, CheckReferResponse, GetReferralsResponse, GetReferCodeResponse, LikeUserRequest, LikeUserResponse
from service.account_service import (
    get_user_by_id,
    update_user_email_and_address,
    add_user_address,
    delete_user_address,
    add_points_to_user,
    create_account,
    update_user_avatar,
    update_seed,
    check_user_referred,
    get_user_referrals,
    get_user_refer_code,
    like_user
)
from config import get_firestore_client, get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["accounts"],
)

@router.post("/create-account", response_model=User, status_code=201)
async def create_account_route(
    request: CreateAccountRequest = Body(..., description="User account data"),
    user_id: Optional[str] = Query(None, description="Optional user ID. If not provided, a new UUID will be generated."),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Create a new user account with the specified fields and default values.

    This endpoint:
    1. Takes user account data as input
    2. Creates a new user document in Firestore with the specified fields and default values
    3. Returns the created User object

    The following fields are required:
    - email: User's email address

    The following fields have default values if not provided:
    - displayName: "AnSenSei"
    - addresses: [] (empty array)
    - avatar: null
    - totalFusion: 0

    The following fields are automatically set:
    - createdAt: Current timestamp
    - level: 1
    - pointsBalance: 0
    - totalCashRecharged: 0
    - totalPointsSpent: 0
    """
    try:
        user = await create_account(request, db, user_id)
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user account: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while creating the user account")

@router.get("/{user_id}", response_model=User)
async def get_user_route(
    user_id: str = Path(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a user by ID.
    """
    try:
        user = await get_user_by_id(user_id, db)
        if not user:
            raise HTTPException(status_code=404, detail=f"User with ID {user_id} not found")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while retrieving the user")

@router.put("/{user_id}/email-and-avatar", response_model=User)
async def update_user_email_and_avatar_route(
    user_id: str = Path(...),
    update_data: UserEmailAddressUpdate = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update a user's email and avatar.
    """
    try:
        updated_user = await update_user_email_and_address(
            user_id=user_id, 
            email=update_data.email, 
            db_client=db,
            avatar=update_data.avatar
        )
        return updated_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating user email and avatar: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the user email and avatar")

@router.post("/{user_id}/addresses", response_model=User)
async def add_user_address_route(
    user_id: str = Path(...),
    address: Address = Body(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add a new address to a user's addresses.
    """
    try:
        updated_user = await add_user_address(user_id, address, db)
        return updated_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding user address: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding the address")

@router.delete("/{user_id}/addresses/{address_id}", response_model=str)
async def delete_user_address_route(
    user_id: str = Path(...),
    address_id: str = Path(...),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Delete an address from a user's addresses.
    """
    try:
        result = await delete_user_address(user_id, address_id, db)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user address: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while deleting the address")

@router.post("/{user_id}/points", response_model=dict)
async def add_points_to_user_route(
    user_id: str = Path(..., description="The ID of the user to add points to"),
    points_request: AddPointsRequest = Body(..., description="The points to add"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Add points to a user's pointsBalance.

    This endpoint:
    1. Takes a user ID and points to add as arguments
    2. Validates that the points to add are greater than 0
    3. Adds the points to the user's pointsBalance
    4. Returns a success message with the updated points balance
    """
    try:
        updated_user = await add_points_to_user(
            user_id=user_id,
            points=points_request.points,
            db_client=db
        )
        return {
            "message": f"Successfully added {points_request.points} points to user {user_id}",
            "new_balance": updated_user.pointsBalance
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding points to user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while adding points to the user")

@router.post("/{user_id}/avatar", response_model=User)
async def upload_avatar_route(
    user_id: str = Path(..., description="The ID of the user to update"),
    avatar: UploadFile = File(..., description="The avatar image file"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Upload a new avatar image for a user.

    This endpoint:
    1. Takes a user ID and avatar image file as input
    2. Uploads the avatar image to cloud storage
    3. Updates the user's avatar field with the URL
    4. Returns the updated User object
    """
    try:
        # Read the file content
        file_content = await avatar.read()

        # Get the content type
        content_type = avatar.content_type

        updated_user = await update_user_avatar(
            user_id=user_id,
            avatar=file_content,
            content_type=content_type,
            db_client=db
        )
        return updated_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading avatar for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while uploading the avatar")

@router.put("/{user_id}/seed", response_model=User)
async def update_seed_route(
    user_id: str = Path(..., description="The ID of the user to update"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Update a user's clientSeed with a new random value.

    This endpoint:
    1. Takes a user ID as input
    2. Generates a new random clientSeed
    3. Updates the user's clientSeed field
    4. Returns the updated User object
    """
    try:
        updated_user = await update_seed(
            user_id=user_id,
            db_client=db
        )
        return updated_user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating seed for user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while updating the user's seed")

@router.get("/{user_id}/check-refer", response_model=CheckReferResponse)
async def check_refer_route(
    user_id: str = Path(..., description="The ID of the user to check"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Check if a user has been referred (has the referred_by field).

    This endpoint:
    1. Takes a user ID
    2. Checks if the user has the referred_by field
    3. Returns a response indicating whether the user has been referred and the referer_id if available

    This is used to determine if the user is using a referral code for the first time.
    """
    try:
        result = await check_user_referred(user_id, db)
        return CheckReferResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking user referral status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while checking the user referral status")

@router.get("/{user_id}/referrals", response_model=GetReferralsResponse)
async def get_user_referrals_route(
    user_id: str = Path(..., description="The ID of the user to get referrals for"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get all users referred by a specific user.

    This endpoint:
    1. Takes a user ID
    2. Gets all users referred by this user from the "refers" subcollection
    3. Returns a response with the total count and details of each referred user

    This is used to track how many users have been referred by a specific user.
    """
    try:
        result = await get_user_referrals(user_id, db)
        return GetReferralsResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user referrals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting the user referrals")

@router.get("/{user_id}/refer-code", response_model=GetReferCodeResponse)
async def get_user_refer_code_route(
    user_id: str = Path(..., description="The ID of the user to get the referral code for"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Get a user's referral code.

    This endpoint:
    1. Takes a user ID
    2. Gets the user's referral code from the refer_codes collection
    3. Returns a response with the user ID and referral code

    This is used to get a user's referral code for sharing with others.
    """
    try:
        result = await get_user_refer_code(user_id, db)
        return GetReferCodeResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user referral code: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while getting the user referral code")

@router.post("/{user_id}/like", response_model=LikeUserResponse)
async def like_user_route(
    user_id: str = Path(..., description="The ID of the user who is liking another user"),
    like_request: LikeUserRequest = Body(..., description="The request containing the target user ID to like"),
    db: firestore.AsyncClient = Depends(get_firestore_client)
):
    """
    Like another user.

    This endpoint:
    1. Takes a user ID and a target user ID
    2. Creates a record in the user's 'likes' subcollection
    3. Returns a response with information about the like action

    This is used to allow users to like other users.
    """
    try:
        result = await like_user(user_id, like_request.target_user_id, db)
        return LikeUserResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error liking user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while liking the user")
