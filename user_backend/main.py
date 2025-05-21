import secrets
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.middleware.sessions import SessionMiddleware

from config import get_logger, instrument_app, settings, test_connection, close_connector
from router import account_router, card_router, marketplace_router, rank_router

# Configure logging with structured logger
logger = get_logger("main")

# Service configuration
SERVICE_TITLE = "User Service API"
SERVICE_PATH = "users"
API_VERSION = "v1"

# Main application instance
app = FastAPI(title=f"{SERVICE_TITLE} - Main Gateway")

# Add database connection test to startup event
@app.on_event("startup")
async def startup_db_client():
    """Initialize database connection on startup"""
    logger.info("Testing database connection...")
    if test_connection():
        logger.info("Database connection successful")
    else:
        logger.warning("Failed to establish database connection")

# Add database connection cleanup to shutdown event
@app.on_event("shutdown")
async def shutdown_db_client():
    """Close database connections on shutdown"""
    logger.info("Closing database connections...")
    close_connector()
    logger.info("Database connections closed")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenTelemetry instrumentation on the main app
instrument_app(app)

@app.get("/", response_class=HTMLResponse)
@app.get(f"/{SERVICE_PATH}", response_class=HTMLResponse)
@app.get(f"/{SERVICE_PATH}/", response_class=HTMLResponse)
async def hello_service():
    logger.info(f"Root or service path /{SERVICE_PATH} accessed.")
    return f"""
    <html>
        <head>
            <title>{SERVICE_TITLE}</title>
        </head>
        <body>
            <h1>You've reached the {SERVICE_TITLE}.</h1>
            <p>See <a href='/{SERVICE_PATH}/api/{API_VERSION}/docs'>API docs</a> for user operations.</p>
        </body>
    </html>
    """

# Sub-API for actual user operations
api_v1 = FastAPI(
    title=SERVICE_TITLE,
    description="API for managing users and user profiles.",
    version=API_VERSION,
)

# Middleware for the sub-API (api_v1)
SECRET_KEY = secrets.token_urlsafe(32)
api_v1.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
logger.info(f"SessionMiddleware added to /api/{API_VERSION} with a generated SECRET_KEY.")

# Include routers
api_v1.include_router(account_router.router)
logger.info("Account router included in the sub-API.")

api_v1.include_router(card_router.router)
logger.info("Card router included in the sub-API.")

api_v1.include_router(marketplace_router.router)
logger.info("Marketplace router included in the sub-API.")

api_v1.include_router(rank_router.router)
logger.info("Rank router included in the sub-API.")

# Mount the sub-API (api_v1) under the main app (app)
app.mount(f"/{SERVICE_PATH}/api/{API_VERSION}", api_v1)
logger.info(f"Sub-API mounted at /{SERVICE_PATH}/api/{API_VERSION}")

if __name__ == "__main__":
    port = 8082  # Use a different port than the other services
    host = "0.0.0.0"  # Listen on all available IPs

    logger.info(f"Starting Uvicorn server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, log_level="info", reload=True)
