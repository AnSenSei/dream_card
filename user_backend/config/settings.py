from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Application settings
    app_name: str

    # Google Cloud Storage settings
    gcs_project_id: str
    gcs_bucket_name: str
    user_avator_bucket: str

    # Firestore settings
    firestore_project_id: str
    firestore_collection_users: str
    quota_project_id: str

    # Card expiration settings (in days)
    card_expire_days: int
    card_buyback_expire_days: int

    # Backend service URLs
    storage_service_url: str

    # Stripe API settings
    stripe_api_key: str
    stripe_webhook_secret: str
    # Database connection settings
    db_instance_connection_name: str
    db_user: str
    db_pass: str
    db_name: str
    db_port: int

    # Logging settings
    log_level: str

    class Config:
        env_file = ".env" # If you want to use an.env file for configuration
        env_file_encoding = 'utf-8'

settings = Settings()
