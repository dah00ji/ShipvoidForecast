#!/usr/bin/env python3
"""
Configuration for Shipvoid Forecast Application

File paths can be configured here or via environment variables.
"""

import os
from datetime import datetime
from pathlib import Path


# =============================================================================
# DC CONFIGURATION - Distribution Center Path Mappings
# =============================================================================

DC_CONFIGS = {
    "6006": {
        "name": "DC 6006",
        "base_path": r"\\us06006w8000d2a.s06006.us.wal-mart.com\Rdrive\Ship_Void_Forecast",
        "uses_monthly_folders": False,
    },
    "6040": {
        "name": "DC 6040",
        "base_path": r"\\us06040w8000d2a.s06040.us.wal-mart.com\Rdrive",
        "uses_monthly_folders": False,
    },
    "6031": {
        "name": "DC 6031",
        "base_path": r"\\s06031nts800us.s06031.us\Rdrive\Shipvoid Forecast",
        "uses_monthly_folders": True,  # Uses YYYY/MMM YYYY folder structure
    },
}

# Default DC
DEFAULT_DC = "6006"
CURRENT_DC = os.environ.get("SHIPVOID_DC", DEFAULT_DC)


def get_dc_path(dc_code: str, target_date: datetime = None) -> str:
    """
    Get the file path for a specific DC.
    
    For DCs with monthly folders (like 6031), builds the path based on the target date.
    """
    if dc_code not in DC_CONFIGS:
        raise ValueError(f"Unknown DC: {dc_code}. Available: {list(DC_CONFIGS.keys())}")
    
    config = DC_CONFIGS[dc_code]
    base_path = config["base_path"]
    
    if config.get("uses_monthly_folders"):
        # Use target date or current date
        date = target_date or datetime.now()
        year = date.strftime("%Y")
        month_folder = date.strftime("%b %Y").upper()  # e.g., "JAN 2026"
        return os.path.join(base_path, year, month_folder)
    
    return base_path


def get_available_dcs() -> list:
    """Get list of available DC configurations."""
    return [{"code": code, "name": cfg["name"]} for code, cfg in DC_CONFIGS.items()]


# =============================================================================
# DATA SOURCE CONFIGURATION
# =============================================================================

# Network share path for daily-updated Shipvoid files (uses DC config)
DEFAULT_SHIPVOID_PATH = get_dc_path(CURRENT_DC)
SHIPVOID_SOURCE_PATH = os.environ.get("SHIPVOID_SOURCE_PATH", DEFAULT_SHIPVOID_PATH)

# Local path for Legacy Unbilled Cartons CSV
# ============================================================================
# ⚠️ INSTRUCTIONS FOR EACH USER:
# Set your own path to the Legacy Unbilled Cartons folder/file.
# You can either:
#   1. Set the LEGACY_SOURCE_PATH environment variable, OR
#   2. Enter the path directly in the Dashboard UI
#
# Example paths:
#   - Windows network share: \\server\share\Legacy_Unbilled
#   - Local folder: C:\Users\YourName\Documents\Legacy_Unbilled
#   - Relative to project: .\data
# ============================================================================
DEFAULT_LEGACY_PATH = ""  # <-- Leave blank! Each user must provide their own path
LEGACY_SOURCE_PATH = os.environ.get("LEGACY_SOURCE_PATH", DEFAULT_LEGACY_PATH)

# Combined source path for display (backward compatibility)
SOURCE_PATH = SHIPVOID_SOURCE_PATH

# File patterns to match
SHIPVOID_PATTERN = "Shipvoid*.xlsm"
LEGACY_PATTERN = "Legacy*.csv"

# Fallback patterns if primary not found
SHIPVOID_FALLBACK_PATTERNS = ["Shipvoid*.xlsx", "Shipvoid*.xls"]


# =============================================================================
# APPLICATION CONFIGURATION  
# =============================================================================

# Server settings
HOST = os.environ.get("SHIPVOID_HOST", "127.0.0.1")
PORT = int(os.environ.get("SHIPVOID_PORT", "8050"))

# App metadata
APP_TITLE = "Shipvoid Forecast Dashboard"
APP_DESCRIPTION = "Cross-reference report for Shipvoid Forecast & Legacy Unbilled Cartons"


def get_source_path() -> Path:
    """Get the configured source path as a Path object."""
    return Path(SOURCE_PATH)


def set_source_path(new_path: str) -> None:
    """Update the source path at runtime (backward compat)."""
    global SOURCE_PATH
    SOURCE_PATH = new_path


def set_shipvoid_source_path(new_path: str) -> None:
    """Update the Shipvoid source path at runtime."""
    global SHIPVOID_SOURCE_PATH, SOURCE_PATH
    SHIPVOID_SOURCE_PATH = new_path
    SOURCE_PATH = new_path  # Keep in sync for display


def set_current_dc(dc_code: str, target_date: datetime = None) -> str:
    """Set the current DC and update the source path accordingly."""
    global CURRENT_DC, SHIPVOID_SOURCE_PATH, SOURCE_PATH
    
    if dc_code not in DC_CONFIGS:
        raise ValueError(f"Unknown DC: {dc_code}. Available: {list(DC_CONFIGS.keys())}")
    
    CURRENT_DC = dc_code
    new_path = get_dc_path(dc_code, target_date)
    SHIPVOID_SOURCE_PATH = new_path
    SOURCE_PATH = new_path
    return new_path


def get_config_summary() -> dict:
    """Get a summary of current configuration."""
    return {
        "source_path": SOURCE_PATH,
        "shipvoid_source_path": SHIPVOID_SOURCE_PATH,
        "legacy_source_path": LEGACY_SOURCE_PATH,
        "shipvoid_pattern": SHIPVOID_PATTERN,
        "legacy_pattern": LEGACY_PATTERN,
        "host": HOST,
        "port": PORT,
        "current_dc": CURRENT_DC,
        "available_dcs": get_available_dcs(),
    }
