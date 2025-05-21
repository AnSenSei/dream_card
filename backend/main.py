import secrets
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.middleware.sessions import SessionMiddleware

from config import get_logger, instrument_app, settings # Assuming settings might be used later
from router import packs_router # Your existing routers
from router import storage_router # Import the storage router
from router import fusion_router # Import the fusion router
from router import marketplace_router # Import the marketplace router

# Configure logging with structured logger
logger = get_logger("main") # Use the logger from config

# These can be loaded from config.settings if you move them there
SERVICE_TITLE = "Card Gacha Service API"
SERVICE_PATH = "gacha" # Example service path
API_VERSION = "v1"

# Main application instance
app = FastAPI(title=f"{SERVICE_TITLE} - Main Gateway") # Main app can have its own title

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenTelemetry instrumentation on the main app
instrument_app(app)


@app.get("/", response_class=HTMLResponse)
@app.get(f"/{SERVICE_PATH}", response_class=HTMLResponse)
@app.get(f"/{SERVICE_PATH}/", response_class=HTMLResponse) # Added trailing slash variant
async def hello_service(): # Changed function name to avoid conflict if you have other 'hello'
    logger.info(f"Root or service path /{SERVICE_PATH} accessed.")
    return f"""
    <html>
        <head>
            <title>{SERVICE_TITLE}</title>
        </head>
        <body>
            <h1>You've reached the {SERVICE_TITLE}.</h1>
            <p>See <a href='/{SERVICE_PATH}/api/{API_VERSION}/docs'>API docs</a> for card gacha operations.</p>
        </body>
    </html>
    """

# Sub-API for actual gacha operations, to be mounted
api_v1 = FastAPI(
    title=SERVICE_TITLE,
    description="API for drawing cards from packs and managing card collections.",
    version=API_VERSION, # Version for this sub-API
    # docs_url="/docs", # Default, can be customized
    # redoc_url="/redoc" # Default, can be customized
)

# Middleware for the sub-API (api_v1)
# SessionMiddleware might be more relevant for user-specific operations if you add them
SECRET_KEY = secrets.token_urlsafe(32)
api_v1.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)
logger.info(f"SessionMiddleware added to /api/{API_VERSION} with a generated SECRET_KEY.")


# Include your existing routers into the sub-API
api_v1.include_router(packs_router.router)
api_v1.include_router(storage_router.router) # Include the storage router
api_v1.include_router(fusion_router.router) # Include the fusion router
api_v1.include_router(marketplace_router.router) # Include the marketplace router
logger.info("Gacha routers (packs, cards, draw), storage router, fusion router, and marketplace router included in the sub-API.")


# Mount the sub-API (api_v1) under the main app (app)
app.mount(f"/{SERVICE_PATH}/api/{API_VERSION}", api_v1)
logger.info(f"Sub-API mounted at /{SERVICE_PATH}/api/{API_VERSION}")


if __name__ == "__main__":
    port = 8080 # Default port
    host = "0.0.0.0" # Listen on all available IPs

    # You could also load host/port from config.settings
    # port = settings.APP_PORT or 8080
    # host = settings.APP_HOST or "0.0.0.0"

    logger.info(f"Starting Uvicorn server on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, log_level="info", reload=True)

# To run this app (ensure your current directory is the project root, where main.py is):
# 1. Ensure FastAPI and Uvicorn are installed: pip install fastapi uvicorn
# 2. Run with Uvicorn: uvicorn main:app --reload
# 
# Your directory structure should look something like:
# .gitignore
# main.py
# models/
#   schemas.py
#   __init__.py (optional, for package)
# router/
#   packs_router.py
#   cards_router.py
#   draw_router.py
#   __init__.py (optional, for package)
# service/
#   data.py
#   draw_service.py
#   __init__.py (optional, for package)
# config/ (still to be created/used)
# requirements.txt (recommended) 
