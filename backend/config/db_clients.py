from google.cloud import storage
from google.cloud import firestore
from google.api_core.client_options import ClientOptions
from .settings import settings
from config import get_logger
import os

logger = get_logger(__name__)

# The Project ID to use for quota and billing attribution
# This should be the actual Project ID, not the display name.
QUOTA_PROJECT_ID = "seventh-program-433718-h8"

# Initialize Google Cloud Storage client
storage_client = None
try:
    # Use Application Default Credentials
    storage_client = storage.Client(project=QUOTA_PROJECT_ID)
    env_type = "Cloud Run" if os.getenv("K_SERVICE") else "local development"
    logger.info(f"Successfully initialized Google Cloud Storage client for project {QUOTA_PROJECT_ID} in {env_type} environment")
except Exception as e:
    logger.error(f"Failed to initialize Google Cloud Storage client: {e}", exc_info=True)
    storage_client = None  # Ensure it's None if initialization fails

# Initialize Firestore client
firestore_client = None
try:
    # Explicitly set the quota_project_id using ClientOptions
    client_options = ClientOptions(quota_project_id=QUOTA_PROJECT_ID)
    firestore_client = firestore.AsyncClient(
        project=settings.firestore_project_id, # This is the project where your Firestore DB resides
        client_options=client_options
    )
    logger.info(f"Successfully initialized Firestore AsyncClient for project {settings.firestore_project_id} with quota project {QUOTA_PROJECT_ID}.")
except Exception as e:
    logger.error(f"Failed to initialize Firestore client: {e}", exc_info=True)
    firestore_client = None # Ensure it's None if initialization fails

def get_storage_client():
    if storage_client is None:
        logger.error("Storage client is not initialized.")
        raise RuntimeError("Storage client is not initialized. Check GCS configuration and credentials.")
    return storage_client

def get_firestore_client():
    if firestore_client is None:
        logger.error("Firestore client is not initialized.")
        raise RuntimeError("Firestore client is not initialized. Check Firestore configuration and credentials.")
    return firestore_client 