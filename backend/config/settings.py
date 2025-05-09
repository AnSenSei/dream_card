from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Card Gacha API"
    gcs_project_id: str = "seventh-program-433718-h8"
    gcs_bucket_name: str = "pokemon_cards_pull"
    firestore_project_id: str = "seventh-program-433718-h8" # Use the actual Project ID
    firestore_collection_cards: str = "pokemon_card_info"
    meta_data_collection: str = "collection_meta_data"
    # Add other configurations here, e.g., database URLs, API keys

    class Config:
        env_file = ".env" # If you want to use an.env file for configuration
        env_file_encoding = 'utf-8'

settings = Settings() 