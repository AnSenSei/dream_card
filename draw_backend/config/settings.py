from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Draw Backend API"
    gcs_project_id: str = "seventh-program-433718-h8"
    gcs_bucket_name: str = "pokemon_cards_pull"
    firestore_project_id: str = "seventh-program-433718-h8"
    firestore_collection_cards: str = "pokemon"
    meta_data_collection: str = "collection_meta_data"
    PACKS_BUCKET: str = "pack_covers"
    # Add other configurations specific to the draw backend

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()