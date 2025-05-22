from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "User Service API"
    gcs_project_id: str = "seventh-program-433718-h8"
    gcs_bucket_name: str = "user_profiles"
    user_avator_bucket: str = "user_avator"
    firestore_project_id: str = "seventh-program-433718-h8" # Use the actual Project ID
    firestore_collection_users: str = "users"

    # Card expiration settings (in days)
    card_expire_days: int = 10  # Cards expire after 30 days
    card_buyback_expire_days: int = 7  # Buyback option expires after 7 days

    # Backend service URLs
    storage_service_url: str = "http://0.0.0.0:8080"  # For local testing

    # Stripe API settings
    stripe_api_key: str = "sk_test_51RRaUk4STmMQIYMZkwfGsWvGFIEC6gW4zu6KXq56iUSeTmNmSDsa0zrdNO8KNTGL5YwWRM6sOurnc8tNww2o2aOM00ezEKdaKK"
    # This should be set to the actual webhook signing secret from the Stripe dashboard in production
    stripe_webhook_secret: str = "whsec_bb78eae4c3e2298b72421adc7832706371a28e7ef24aa2c78741a7698040c935"

    # Add other configurations here, e.g., database URLs, API keys

    class Config:
        env_file = ".env" # If you want to use an.env file for configuration
        env_file_encoding = 'utf-8'

settings = Settings()
