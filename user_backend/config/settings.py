from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "User Service API"
    gcs_project_id: str = "seventh-program-433718-h8"
    gcs_bucket_name: str = "user_profiles"
    firestore_project_id: str = "seventh-program-433718-h8" # Use the actual Project ID
    firestore_collection_users: str = "users"

    # Card expiration settings (in days)
    card_expire_days: int = 10  # Cards expire after 30 days
    card_buyback_expire_days: int = 7  # Buyback option expires after 7 days

    # Backend service URLs
    storage_service_url: str = "http://0.0.0.0:8080"  # For local testing

    # Add other configurations here, e.g., database URLs, API keys

    class Config:
        env_file = ".env" # If you want to use an.env file for configuration
        env_file_encoding = 'utf-8'

settings = Settings()
