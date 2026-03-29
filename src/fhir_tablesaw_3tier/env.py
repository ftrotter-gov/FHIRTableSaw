"""Minimal .env loader.

We intentionally avoid adding a new dependency (e.g., python-dotenv).
This loader supports simple KEY=VALUE lines and ignores comments/blank lines.
"""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> dict[str, str]:
    """Load a .env file into process environment.

    Returns dict of loaded key/value pairs.
    """

    p = Path(path)
    if not p.exists():
        return {}

    loaded: dict[str, str] = {}
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        loaded[k] = v
        if override or k not in os.environ:
            os.environ[k] = v

    return loaded


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_fhir_basic_auth() -> tuple[str, str] | None:
    """Get optional FHIR Basic Auth credentials from environment.

    Preferred:
      - FHIR_API_USERNAME
      - FHIR_API_PASSWORD

    Accepted aliases (legacy):
      - FHIR_USERNAME
      - FHIR_PASSWORD

    Returns:
      (username, password) if both are present, otherwise None.

    Notes:
      This return value is intentionally shaped to be passed directly to
      `httpx.Client(auth=...)`, which will generate an `Authorization: Basic ...`
      header for each request.

    Raises:
      ValueError if only one of username/password is set.
    """

    username = os.environ.get("FHIR_API_USERNAME") or os.environ.get("FHIR_USERNAME")
    password = os.environ.get("FHIR_API_PASSWORD") or os.environ.get("FHIR_PASSWORD")

    if (username and not password) or (password and not username):
        raise ValueError(
            "Missing Basic Auth credentials. Set FHIR_API_USERNAME/FHIR_API_PASSWORD (preferred) "
            "or FHIR_USERNAME/FHIR_PASSWORD in .env"
        )

    if not username or not password:
        return None
    return username, password


def require_fhir_basic_auth() -> tuple[str, str]:
    """Like get_fhir_basic_auth(), but requires credentials to be set."""

    creds = get_fhir_basic_auth()
    if creds is None:
        raise ValueError(
            "Missing Basic Auth credentials. Set FHIR_API_USERNAME/FHIR_API_PASSWORD (preferred) "
            "or FHIR_USERNAME/FHIR_PASSWORD in .env"
        )
    return creds


def get_db_url() -> str:
    """Get the DATABASE_URL from environment.

    Returns:
        PostgreSQL connection URL

    Raises:
        ValueError: If DATABASE_URL is not set
    """
    return require_env("DATABASE_URL")


def get_db_schema() -> str:
    """Get the DB_SCHEMA from environment.

    Returns:
        Schema name (defaults to 'public' if not set)
    """
    return os.environ.get("DB_SCHEMA", "public")


def get_data_source_config(*, source: str) -> tuple[str, str]:
    """Get the data directory and schema for a specific data source.
    
    Args:
        source: One of 'test', 'cms', or 'palantir'
    
    Returns:
        Tuple of (data_directory, schema_name)
    
    Raises:
        ValueError: If source is invalid or required env vars are not set
    """
    source_upper = source.upper()
    
    if source.lower() == "test":
        dir_var = "TEST_FHIR_DIR"
        schema_var = "TEST_FHIR_SCHEMA"
    elif source.lower() == "cms":
        dir_var = "CMS_FHIR_DIR"
        schema_var = "CMS_FHIR_SCHEMA"
    elif source.lower() == "palantir":
        dir_var = "PALANTIR_FHIR_DIR"
        schema_var = "PALANTIR_FHIR_SCHEMA"
    else:
        raise ValueError(f"Invalid data source: {source}. Must be 'test', 'cms', or 'palantir'")
    
    data_dir = os.environ.get(dir_var)
    if not data_dir or data_dir.strip() == "" or "/REPLACEME/" in data_dir:
        raise ValueError(
            f"Missing or invalid {dir_var} environment variable. "
            f"Please set it to a valid directory path in your .env file."
        )
    
    schema = os.environ.get(schema_var)
    if not schema or schema.strip() == "":
        raise ValueError(
            f"Missing {schema_var} environment variable. "
            f"Please set it in your .env file."
        )
    
    return data_dir, schema


def get_fhir_cache_folder() -> str:
    """Get FHIR cache folder for backward compatibility.
    
    Returns:
        FHIR_API_CACHE_FOLDER value
    
    Raises:
        ValueError: If not set
    """
    folder = os.environ.get("FHIR_API_CACHE_FOLDER")
    if not folder or folder.strip() == "":
        raise ValueError(
            "Missing FHIR_API_CACHE_FOLDER environment variable. "
            "For new multi-source setup, use get_data_source_config() instead."
        )
    return folder
