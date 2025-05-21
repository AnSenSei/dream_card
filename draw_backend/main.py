from fastapi import FastAPI
from draw_backend.config import instrument_app, get_logger

# Initialize logger
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Draw Backend API",
    description="API for the drawing functionality",
    version="0.1.0",
)

# Instrument the app (placeholder for now)
instrument_app(app)

# Import and include routers
from draw_backend.router import draw_router
app.include_router(draw_router.router)

@app.get("/")
async def root():
    return {"message": "Welcome to the Draw Backend API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
