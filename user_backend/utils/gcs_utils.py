import os
from datetime import timedelta

import google.auth.transport.requests
from google.auth import compute_engine
import google.auth
from google.oauth2 import service_account
from config import get_logger, get_storage_client

logger = get_logger(__name__)

async def generate_signed_url(gcs_uri: str) -> str:
    """
    Generates a signed URL for a GCS object that is valid for a limited time.
    Handles both Cloud Run/Compute Engine and local development environments.
    Requires GOOGLE_APPLICATION_CREDENTIALS for local development.
    """
    try:
        if not gcs_uri or not gcs_uri.startswith('gs://'): 
            logger.warning(f"Invalid or non-GCS URI provided for signing: {gcs_uri}")
            return gcs_uri # Return as is or perhaps None/empty string?

        # Parse bucket and object path from gs:// URI
        parts = gcs_uri[5:].split('/', 1)
        if len(parts) < 2:
            logger.warning(f"Could not parse bucket/blob name from GCS URI: {gcs_uri}")
            return gcs_uri
        bucket_name, blob_name = parts[0], parts[1]
        
        storage_client = get_storage_client() # Use centralized client getter
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Set expiration time (e.g., 1 hour)
        expiration = timedelta(seconds=3600) 
        
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