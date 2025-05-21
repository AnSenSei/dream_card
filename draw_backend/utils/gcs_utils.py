from google.cloud import storage
import datetime
import os
from draw_backend.config import get_logger, settings
import google.auth.transport.requests
from google.auth import compute_engine
import google.auth
from google.oauth2 import service_account

logger = get_logger(__name__)

def generate_signed_url(bucket_name: str, blob_name: str, expiration_minutes: int = 10080) -> str:
    """
    Generate a signed URL for accessing a GCS object.

    Args:
        bucket_name: The name of the GCS bucket
        blob_name: The name of the blob (object) in the bucket
        expiration_minutes: How long the signed URL should be valid for, in minutes

    Returns:
        A signed URL that can be used to access the object
    """
    try:
        # Import the storage client here to avoid circular imports
        from draw_backend.config import get_storage_client
        storage_client = get_storage_client()

        # Calculate expiration time
        expiration = datetime.timedelta(minutes=expiration_minutes)

        # Get the bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

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
        url = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET",
            credentials=credentials
        )

        logger.info(f"Generated signed URL for {blob_name} in bucket {bucket_name}")
        return url
    except Exception as e:
        logger.error(f"Error generating signed URL for {blob_name} in bucket {bucket_name}: {e}", exc_info=True)
        # Return the original GCS URI if signing fails
        return f"gs://{bucket_name}/{blob_name}"

def upload_file_to_gcs(file_content, destination_blob_name: str, bucket_name: str = None) -> str:
    """
    Upload a file to Google Cloud Storage.

    Args:
        file_content: The content of the file to upload
        destination_blob_name: The name to give the file in GCS
        bucket_name: The name of the bucket to upload to (defaults to settings.gcs_bucket_name)

    Returns:
        The public URL of the uploaded file
    """
    if bucket_name is None:
        bucket_name = settings.gcs_bucket_name

    try:
        # Import the storage client here to avoid circular imports
        from draw_backend.config import get_storage_client
        storage_client = get_storage_client()

        # Get the bucket and create a new blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        blob.upload_from_string(file_content)

        logger.info(f"File {destination_blob_name} uploaded to bucket {bucket_name}")

        # Return the public URL
        return f"gs://{bucket_name}/{destination_blob_name}"
    except Exception as e:
        logger.error(f"Error uploading file to GCS: {e}", exc_info=True)
        raise
