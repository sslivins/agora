import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from api import __version__
from api.auth import WebAuthRequired
from api.config import load_settings
from api.routers import assets, cms, playback, status, system
from api.ui import router as ui_router

app = FastAPI(
    title="Agora",
    description="Media playback system for Raspberry Pi — REST API for asset management, playback control, and status monitoring.",
    version=__version__,
)

# Load config and initialize directories
settings = load_settings()
settings.ensure_dirs()
app.state.settings = settings
app.state.start_time = time.time()

# Static files
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# Redirect to login when web auth is missing
@app.exception_handler(WebAuthRequired)
async def web_auth_redirect(request: Request, exc: WebAuthRequired):
    return RedirectResponse("/login", status_code=303)


# API routers
app.include_router(assets.router, prefix="/api/v1", tags=["assets"])
app.include_router(playback.router, prefix="/api/v1", tags=["playback"])
app.include_router(status.router, prefix="/api/v1", tags=["status"])
app.include_router(cms.router, prefix="/api/v1", tags=["cms"])
app.include_router(system.router, prefix="/api/v1", tags=["system"])

# Web UI router
app.include_router(ui_router)
