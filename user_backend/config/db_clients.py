from algoliasearch.search.client import SearchClient


from google.cloud import storage
from google.cloud import firestore
from google.api_core.client_options import ClientOptions
from .settings import settings
from config import get_logger
import os

logger = get_logger(__name__)

# Initialize Google Cloud Storage client
storage_client = None
try:
    # Use Application Default Credentials
    storage_client = storage.Client(project=settings.quota_project_id)
    env_type = "Cloud Run" if os.getenv("K_SERVICE") else "local development"
    logger.info(f"Successfully initialized Google Cloud Storage client for project {settings.quota_project_id} in {env_type} environment")
except Exception as e:
    logger.error(f"Failed to initialize Google Cloud Storage client: {e}", exc_info=True)
    storage_client = None  # Ensure it's None if initialization fails

# Initialize Firestore client
firestore_client = None
try:
    # Explicitly set the quota_project_id using ClientOptions
    client_options = ClientOptions(quota_project_id=settings.quota_project_id)
    firestore_client = firestore.AsyncClient(
        project=settings.firestore_project_id, # This is the project where your Firestore DB resides
        client_options=client_options
    )
    logger.info(f"Successfully initialized Firestore AsyncClient for project {settings.firestore_project_id} with quota project {settings.quota_project_id}.")
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


algolia_client = None

try:
    algolia_client = SearchClient(settings.application_id, settings.algolia_api_key)
    logger.info(f"Successfully initialized Algolia SearchClient")
except Exception as e:
    logger.error(f"Failed to initialize Algolia client: {e}", exc_info=True)
    algolia_client = None


def get_algolia_client():
    """Get the global Algolia client"""
    if algolia_client is None:
        logger.error("Algolia client is not initialized.")
        raise RuntimeError("Algolia client is not initialized. Check Algolia configuration and credentials.")
    return algolia_client


async def get_algolia_index(index_name: str = None):
    """
    Get Algolia client and index name for search operations.
    The v4 API uses search_single_index method with index_name parameter.
    """
    index_name = index_name or settings.algolia_index_name
    client = SearchClient(settings.application_id, settings.algolia_api_key)

    return client, index_name


