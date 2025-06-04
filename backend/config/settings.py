from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Application settings
    app_name: str = "Card Gacha API"

    # Google Cloud Storage settings
    gcs_project_id: str = "seventh-program-433718-h8"
    gcs_bucket_name: str = "pokemon_cards_pull"
    PACKS_BUCKET: str = "pack_covers"
    user_avator_bucket: str = "user_avatars"
    emblem_bucket: str = "achievement_emblems"

    # Firestore settings
    firestore_project_id: str = "seventh-program-433718-h8"
    firestore_collection_cards: str = "pokemon"
    meta_data_collection: str = "collection_meta_data"
    quota_project_id: str = "seventh-program-433718-h8"

    shippo_api_key: str

    # User backend service configuration
    user_backend_url: str = "http://localhost:8082/users/api/v1"

    # Algolia settings
    application_id: str
    algolia_api_key: str
    algolia_index_name_pokemon: str
    algolia_index_name_one_piece: str

    # Logging settings
    log_level: str = "INFO"

    class Config:
        env_file = ".env" # If you want to use an.env file for configuration
        env_file_encoding = 'utf-8'

settings = Settings() 
