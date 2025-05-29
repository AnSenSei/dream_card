# User Backend Service

## Environment Variables

This service uses environment variables for configuration. These can be set in a `.env` file in the root directory of the project.

### Setting Up Environment Variables

1. Copy the `.env.template` file to `.env`:
   ```bash
   cp .env.template .env
   ```

2. Edit the `.env` file and fill in your specific values:
   ```
   # Example
   GCS_PROJECT_ID="your-project-id"
   ```

3. For development purposes, you can also use the provided `.env.sample` file which contains sample values:
   ```bash
   cp .env.sample .env
   ```
   Note: The sample values are for development only and should not be used in production.

### Available Environment Variables

The following environment variables are used by the application:

#### Application Settings
- `APP_NAME`: The name of the application

#### Google Cloud Storage Settings
- `GCS_PROJECT_ID`: Google Cloud Project ID for storage
- `GCS_BUCKET_NAME`: Name of the GCS bucket for user profiles
- `USER_AVATOR_BUCKET`: Name of the GCS bucket for user avatars

#### Firestore Settings
- `FIRESTORE_PROJECT_ID`: Google Cloud Project ID for Firestore
- `FIRESTORE_COLLECTION_USERS`: Name of the Firestore collection for users
- `QUOTA_PROJECT_ID`: Project ID for quota and billing attribution

#### Card Expiration Settings
- `CARD_EXPIRE_DAYS`: Number of days after which cards expire
- `CARD_BUYBACK_EXPIRE_DAYS`: Number of days after which buyback option expires

#### Backend Service URLs
- `STORAGE_SERVICE_URL`: URL of the storage service

#### Stripe API Settings
- `STRIPE_API_KEY`: Stripe API key for payment processing
- `STRIPE_WEBHOOK_SECRET`: Stripe webhook signing secret

#### Database Connection Settings
- `DB_INSTANCE_CONNECTION_NAME`: Cloud SQL instance connection name
- `DB_USER`: Database username
- `DB_PASS`: Database password
- `DB_NAME`: Database name
- `DB_PORT`: Database port

## Running the Application

To run the application with the environment variables:

```bash
python main.py
```

The application will automatically load the environment variables from the `.env` file.