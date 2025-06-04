from google.cloud import storage
from google.cloud import firestore
from google.api_core.client_options import ClientOptions
from algoliasearch.search.client import SearchClient
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

# Initialize Algolia client
algolia_client = None
try:
    algolia_client = SearchClient(settings.application_id, settings.algolia_api_key)
    logger.info(f"Successfully initialized Algolia SearchClient")
except Exception as e:
    logger.error(f"Failed to initialize Algolia client: {e}", exc_info=True)
    algolia_client = None

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

def get_algolia_client():
    """Get the global Algolia client"""
    if algolia_client is None:
        logger.error("Algolia client is not initialized.")
        raise RuntimeError("Algolia client is not initialized. Check Algolia configuration and credentials.")
    return algolia_client

async def get_algolia_index(index_name: str = None, collection_id: str = None):
    """
    Get Algolia client and index name for search operations.
    The v4 API uses search_single_index method with index_name parameter.

    If index_name is provided, it will be used directly.
    Otherwise, it will determine the index name based on collection_id:
    - If collection_id is "pokemon", it uses settings.algolia_index_name_pokemon
    - If collection_id is "one_piece", it uses settings.algolia_index_name_one_piece
    - Otherwise, it defaults to settings.algolia_index_name_pokemon
    """
    if not index_name:
        if collection_id == "pokemon":
            index_name = settings.algolia_index_name_pokemon
        elif collection_id == "one_piece":
            index_name = settings.algolia_index_name_one_piece
        else:
            # Default to pokemon if collection_id is not specified or not recognized
            index_name = settings.algolia_index_name_pokemon

    client = SearchClient(settings.application_id, settings.algolia_api_key)

    return client, index_name

def get_sorted_index_name(sort_by: str = None, sort_order: str = "desc", collection_id: str = None):
    """
    Get the appropriate Algolia index name based on sort criteria and collection_id.
    """
    # First, determine the base index name based on collection_id
    if collection_id == "pokemon":
        base_index_name = settings.algolia_index_name_pokemon
    elif collection_id == "one_piece":
        base_index_name = settings.algolia_index_name_one_piece
    else:
        # Default to pokemon if collection_id is not specified or not recognized
        base_index_name = settings.algolia_index_name_pokemon

    # For now, we're using the base index name for all sort criteria
    # In the future, we could create replicas for different sort orders
    return base_index_name
