"""
Configuration settings for FlowForge ETL Platform
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
SAMPLES_DIR = DATA_DIR / "samples"
EXPORTS_DIR = DATA_DIR / "exports"
LOGS_DIR = BASE_DIR / "logs"
ASSETS_DIR = BASE_DIR / "assets"

# Ensure directories exist
for dir_path in [DATA_DIR, SAMPLES_DIR, EXPORTS_DIR, LOGS_DIR, ASSETS_DIR]:
    dir_path.mkdir(exist_ok=True)

# Application settings
APP_TITLE = "🌊 FlowForge"
APP_SUBTITLE = "Interactive ETL Platform for Data Engineering & Learning"
APP_ICON = "🌊"

# Theme colors
THEME_COLORS = {
    "primary_blue": "#1E3A8A",
    "accent_orange": "#F59E0B",
    "background_white": "#FFFFFF",
    "text_dark": "#1F2937",
    "success_green": "#10B981",
    "error_red": "#EF4444",
    "warning_yellow": "#F59E0B",
    "light_gray": "#F3F4F6",
    "medium_gray": "#9CA3AF",
    "border_gray": "#E5E7EB"
}

# File upload settings
MAX_FILE_SIZE_MB = 100
ALLOWED_FILE_TYPES = {
    "csv": "text/csv",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "json": "application/json"
}

# Database settings
DEFAULT_DB_PATH = EXPORTS_DIR / "flowforge_data.db"

# API settings
DEFAULT_TIMEOUT = 30
MAX_API_RETRIES = 3

# Logging settings
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = LOGS_DIR / "flowforge.log"