import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Create FastAPI app
app = FastAPI(title="Admin Frontend Server")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the admin_frontend directory to serve static files
app.mount("/", StaticFiles(directory="admin_frontend", html=True), name="admin_frontend")

if __name__ == "__main__":
    port = 8001  # Specified port for admin frontend
    host = "0.0.0.0"  # Listen on all available IPs
    
    print(f"Starting Admin Frontend server on http://{host}:{port}")
    print(f"Access the admin interface at http://localhost:{port}")
    uvicorn.run("admin_server:app", host=host, port=port, log_level="info", reload=True)