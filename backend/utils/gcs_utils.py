import os
import base64
from datetime import timedelta
from typing import Optional, Tuple
import uuid

import google.auth.transport.requests
from google.auth import compute_engine
import google.auth
from google.oauth2 import service_account
from fastapi import HTTPException
from config import get_logger, get_storage_client, settings

logger = get_logger(__name__)

async def generate_signed_url(gcs_uri: str) -> str:
    """
    Generates a signed URL for a GCS object that is valid for a limited time.
    Handles both Cloud Run/Compute Engine and local development environments.
    Requires GOOGLE_APPLICATION_CREDENTIALS for local development.
    Also handles already signed URLs by extracting the original GCS URI.
    """
    try:
        # Handle empty URI
        if not gcs_uri:
            logger.warning("Empty URI provided for signing")
            return gcs_uri

        # Handle already signed URLs (extract original GCS URI)
        if gcs_uri.startswith('https://storage.googleapis.com/'):
            # Extract bucket and blob from the URL
            # Format: https://storage.googleapis.com/BUCKET_NAME/BLOB_PATH?params...
            try:
                # Remove query parameters if present
                base_url = gcs_uri.split('?')[0]
                # Remove the https://storage.googleapis.com/ prefix
                path = base_url.replace('https://storage.googleapis.com/', '')
                # Split into bucket and blob
                parts = path.split('/', 1)
                if len(parts) < 2:
                    logger.warning(f"Could not extract bucket/blob from signed URL: {gcs_uri}")
                    return gcs_uri
                bucket_name, blob_name = parts
                # Construct a new GCS URI
                gcs_uri = f"gs://{bucket_name}/{blob_name}"
                logger.info(f"Extracted GCS URI from signed URL: {gcs_uri}")
            except Exception as e:
                logger.warning(f"Failed to extract GCS URI from signed URL: {e}")
                return gcs_uri

        # Handle regular GCS URIs
        if not gcs_uri.startswith('gs://'):
            logger.warning(f"Invalid or non-GCS URI provided for signing: {gcs_uri}")
            return gcs_uri

        # Parse bucket and object path from gs:// URI
        parts = gcs_uri[5:].split('/', 1)
        if len(parts) < 2:
            logger.warning(f"Could not parse bucket/blob name from GCS URI: {gcs_uri}")
            return gcs_uri
        bucket_name, blob_name = parts[0], parts[1]

        storage_client = get_storage_client() # Use centralized client getter
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Set expiration time (7 days)
        expiration = timedelta(days=7)

        # Determine credentials based on environment
        credentials = None
        if os.getenv("K_SERVICE") or os.getenv("GOOGLE_COMPUTE_ENGINE_PROJECT"): # Check for Cloud Run or other GCE
            # Use default service account credentials in Cloud Run/GCE
            auth_request = google.auth.transport.requests.Request()
            # Use default credentials which should be the service account in these environments
            credentials = compute_engine.IDTokenCredentials(auth_request, "")
            logger.debug("Using Compute Engine credentials for signing URL.")
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            # Use service account key file specified in env var
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
                    scopes=["https://www.googleapis.com/auth/devstorage.read_only"], # Scope for read-only access
                )
                logger.debug("Using GOOGLE_APPLICATION_CREDENTIALS for signing URL.")
            except Exception as e_sa:
                logger.error(f"Failed to load service account credentials from GOOGLE_APPLICATION_CREDENTIALS: {e_sa}")
                raise # Re-raise the exception
        else:
            logger.error("Could not determine credentials for signing URL. Set GOOGLE_APPLICATION_CREDENTIALS or run in a GCP environment.")
            raise Exception("Missing credentials for signed URL generation.")

        # Generate the signed URL
        signed_url = blob.generate_signed_url(
            expiration=expiration,
            version="v4",
            method="GET",
            credentials=credentials,
        )

        return signed_url

    except Exception as e:
        logger.error(f"Error generating signed URL for {gcs_uri}: {e}", exc_info=True)
        return gcs_uri # Fallback: Return original URI if signing fails

async def upload_avatar_to_gcs(avatar_data: bytes, user_id: str, content_type: str = None) -> str:
    """
    Uploads an avatar image to Google Cloud Storage.

    Args:
        avatar_data: Binary image data
        user_id: The ID of the user whose avatar is being uploaded
        content_type: The content type of the image (e.g., "image/jpeg")

    Returns:
        The GCS URI of the uploaded avatar

    Raises:
        HTTPException: If there's an error uploading the avatar
    """
    try:
        binary_data = avatar_data

        # If content_type is not provided, try to determine it
        if not content_type:
            # Try to determine content type from the binary data
            try:
                import magic
                mime = magic.Magic(mime=True)
                content_type = mime.from_buffer(binary_data)
            except ImportError:
                # If python-magic is not installed, default to jpeg for images
                logger.warning("python-magic not available, defaulting to image/jpeg")
                content_type = 'image/jpeg'
            except Exception as e:
                logger.warning(f"Could not determine content type from binary data: {e}")
                content_type = 'image/jpeg'

        # Generate a unique filename for the avatar
        filename = f"{user_id}_{uuid.uuid4()}.{get_file_extension(content_type)}"

        # Get the storage client
        storage_client = get_storage_client()

        # Get the bucket
        bucket = storage_client.bucket(settings.user_avator_bucket)

        # Create a blob
        blob = bucket.blob(filename)

        # Upload the image
        blob.upload_from_string(
            binary_data,
            content_type=content_type
        )

        # Return the GCS URI
        gcs_uri = f"gs://{settings.user_avator_bucket}/{filename}"
        logger.info(f"Uploaded avatar for user {user_id} to {gcs_uri}")

        return gcs_uri
    except ValueError as e:
        logger.error(f"Invalid avatar data: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error uploading avatar for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

def parse_base64_image(base64_image: str) -> Tuple[str, str]:
    """
    Parses a base64 encoded image string to extract content type and base64 data.

    Args:
        base64_image: Base64 encoded image string (e.g., "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEA...")

    Returns:
        A tuple containing the content type and base64 data

    Raises:
        ValueError: If the base64 image string is invalid
    """
    try:
        # Split the base64 image string into content type and data
        content_type_part, base64_data = base64_image.split(';base64,')
        content_type = content_type_part.split(':')[1]

        return content_type, base64_data
    except Exception as e:
        raise ValueError(f"Invalid base64 image format: {e}")

def get_file_extension(content_type: str) -> str:
    """
    Gets the file extension for a given content type.

    Args:
        content_type: The content type (e.g., "image/jpeg")

    Returns:
        The file extension (e.g., "jpg")
    """
    content_type_map = {
        'image/jpeg': 'jpg',
        'image/jpg': 'jpg',
        'image/png': 'png',
        'image/gif': 'gif',
        'image/webp': 'webp',
        'image/svg+xml': 'svg',
        'application/octet-stream': 'bin'
    }

    return content_type_map.get(content_type, 'bin')
