#!/usr/bin/env python3
"""Check that all required dependencies are installed.

This module can be imported by scripts to fail fast if dependencies are missing.
"""

from __future__ import annotations

import sys
from typing import Any


def check_dependencies() -> dict[str, Any]:
    """Check that all required dependencies are installed.
    
    Returns:
        Dictionary with check results including missing packages and suggestions.
    
    Raises:
        SystemExit: If critical dependencies are missing.
    """
    missing_packages = []
    missing_import_names = {}
    
    # Define required packages and their import names
    required_deps = {
        "httpx": "httpx",
        "yaml": "PyYAML",
        "sqlalchemy": "SQLAlchemy",
        "pydantic": "pydantic",
        "fhir_core": "fhir-core",
        "psycopg": "psycopg[binary]",
        "fhir4ds": "fhir4ds",
        "great_expectations": "great-expectations",
        "pandas": "pandas",
        "duckdb": "duckdb",
        "dotenv": "python-dotenv",
    }
    
    for import_name, package_name in required_deps.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
            missing_import_names[import_name] = package_name
    
    if missing_packages:
        print("=" * 70, file=sys.stderr)
        print("ERROR: Missing required dependencies!", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print("\nThe following packages are required but not installed:", file=sys.stderr)
        for pkg in missing_packages:
            print(f"  - {pkg}", file=sys.stderr)
        
        print("\nTo install all required dependencies, run:", file=sys.stderr)
        print("  pip install -r requirements.txt", file=sys.stderr)
        print("\nOr install missing packages individually:", file=sys.stderr)
        print(f"  pip install {' '.join(missing_packages)}", file=sys.stderr)
        print("\nOr install the package in development mode:", file=sys.stderr)
        print("  pip install -e .", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        
        return {
            "status": "failed",
            "missing_packages": missing_packages,
            "missing_imports": missing_import_names,
        }
    
    return {
        "status": "success",
        "missing_packages": [],
    }


def require_dependencies() -> None:
    """Check dependencies and exit if any are missing."""
    result = check_dependencies()
    if result["status"] == "failed":
        sys.exit(1)


if __name__ == "__main__":
    # When run directly, check dependencies and exit with appropriate code
    result = check_dependencies()
    if result["status"] == "success":
        print("✓ All required dependencies are installed!")
        sys.exit(0)
    else:
        sys.exit(1)
