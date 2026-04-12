"""
UrbanPulse VN — Helper Utilities.

Common helper functions used across the project:
- Structured logging setup
- YAML config loading
- Safe environment variable access

Usage:
    from utils.helper import setup_logging, load_config, get_env

    setup_logging()
    config = load_config("configs/datalake.yaml")
    api_key = get_env("NASA_EARTHDATA_TOKEN", required=True)
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load .env file from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def setup_logging(
    level: str = "INFO",
    log_format: str = "structured",
    log_file: str | None = None,
) -> logging.Logger:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_format: 'structured' for JSON-like format, 'simple' for plain text
        log_file: Optional file path for log output

    Returns:
        Root logger instance
    """
    if log_format == "structured":
        fmt = (
            "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s"
        )
    else:
        fmt = "%(asctime)s - %(levelname)s - %(message)s"

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)

    logger = logging.getLogger("urbanpulse")
    logger.info("Logging initialized (level=%s, format=%s)", level, log_format)
    return logger


def load_config(config_path: str) -> dict:
    """
    Load a YAML configuration file.

    Args:
        config_path: Path to YAML file (relative to project root or absolute)

    Returns:
        Parsed YAML as dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
    """
    path = Path(config_path)
    if not path.is_absolute():
        path = _PROJECT_ROOT / path

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    logging.getLogger(__name__).debug("Loaded config: %s", path)
    return config or {}


def get_env(key: str, default: str | None = None, required: bool = False) -> str:
    """
    Safely get an environment variable.

    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raises ValueError when not set and no default

    Returns:
        Environment variable value

    Raises:
        ValueError: If required=True and variable is not set
    """
    value = os.getenv(key, default)
    if value is None and required:
        raise ValueError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file or environment."
        )
    return value


def get_project_root() -> Path:
    """Get the absolute path to the project root directory."""
    return _PROJECT_ROOT
