#!/usr/bin/env python3
"""
Shipvoid Forecast Dashboard - FastAPI Application

Interactive dashboard for cross-referencing Shipvoid Forecast and Legacy Unbilled Cartons.
Features live data refresh from configurable network share.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import config
import data_loader

# Initialize FastAPI app
app = FastAPI(
    title=config.APP_TITLE,
    description=config.APP_DESCRIPTION
)

# Setup templates
templates_dir = Path(__file__).parent / "templates"
templates_dir.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(templates_dir))

# Static files (for team logo)
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
if (Path(__file__).parent / "team-logo.png").exists():
    import shutil
    shutil.copy(Path(__file__).parent / "team-logo.png", static_dir / "team-logo.png")
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Global data cache
_cached_data = None


def get_cached_data() -> dict:
    """Get cached data, loading if not available."""
    global _cached_data
    if _cached_data is None:
        try:
            _cached_data = data_loader.load_all_data()
        except Exception as e:
            # Return empty state with error message
            _cached_data = {
                'data': [],
                'stats': {'total': 0, 'inhouse': 0, 'crossdock': 0, 'oldest_date': 'N/A'},
                'files': {'shipvoid': None, 'legacy': None},
                'error': str(e),
                'load_time': None
            }
    return _cached_data


def refresh_data(source_path: str = None, legacy_path: str = None) -> dict:
    """Refresh data from source."""
    global _cached_data
    try:
        _cached_data = data_loader.load_all_data(source_path, legacy_path)
    except Exception as e:
        _cached_data = {
            'data': [],
            'stats': {'total': 0, 'inhouse': 0, 'crossdock': 0, 'oldest_date': 'N/A'},
            'files': {'shipvoid': None, 'legacy': None},
            'error': str(e),
            'load_time': None
        }
    return _cached_data


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main dashboard page."""
    data = get_cached_data()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "data": json.dumps(data['data']),
        "stats": data['stats'],
        "error": data['error'],
        "load_time": data['load_time'],
        "source_path": config.SOURCE_PATH,
        "config": config.get_config_summary()
    })


@app.post("/api/refresh", response_class=HTMLResponse)
async def api_refresh(request: Request, source_path: str = Form(None), legacy_path: str = Form(None)):
    """Refresh data from source - returns updated stats partial."""
    shipvoid_path = source_path.strip() if source_path and source_path.strip() else None
    legacy_path_clean = legacy_path.strip() if legacy_path and legacy_path.strip() else None
    
    data = refresh_data(shipvoid_path, legacy_path_clean)
    
    # Return a partial HTML response for HTMX
    return templates.TemplateResponse("partials/stats.html", {
        "request": request,
        "stats": data['stats'],
        "error": data['error'],
        "load_time": data['load_time'],
        "source_path": config.SOURCE_PATH
    })


@app.get("/api/data")
async def api_data():
    """Get current data as JSON."""
    data = get_cached_data()
    return JSONResponse(content=data)


@app.post("/api/config")
async def update_config(source_path: str = Form(...)):
    """Update source path configuration."""
    config.set_source_path(source_path)
    return JSONResponse(content={"status": "ok", "source_path": source_path})


@app.get("/api/config")
async def get_config():
    """Get current configuration."""
    return JSONResponse(content=config.get_config_summary())


@app.post("/api/change-dc")
async def change_dc(dc_code: str = Form(...)):
    """Change the current DC and update source path."""
    global _cached_data
    try:
        new_path = config.set_current_dc(dc_code)
        _cached_data = None  # Clear cache to force reload
        return JSONResponse(content={
            "status": "ok", 
            "dc_code": dc_code,
            "source_path": new_path,
            "message": f"Switched to DC {dc_code}"
        })
    except ValueError as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=400)


@app.get("/api/dcs")
async def get_available_dcs():
    """Get list of available DCs."""
    return JSONResponse(content={
        "current_dc": config.CURRENT_DC,
        "available_dcs": config.get_available_dcs()
    })


if __name__ == "__main__":
    import uvicorn
    print(f"\n{'='*60}")
    print(f"  Shipvoid Forecast Dashboard")
    print(f"{'='*60}")
    print(f"  Source Path: {config.SOURCE_PATH}")
    print(f"  Server: http://{config.HOST}:{config.PORT}")
    print(f"{'='*60}\n")
    uvicorn.run(app, host=config.HOST, port=config.PORT)
