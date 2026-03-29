#!/usr/bin/env python3
"""Runner for Test Server FHIR data.

This script loads the test server configuration and runs the main go.py pipeline.
It automatically detects completed stages and resumes from where it left off.

Usage:
    python go_testserver.py [OPTIONS]

All command-line options from go.py are supported.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure repo src/ is importable
REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Check dependencies
try:
    from check_dependencies import require_dependencies  # noqa: E402
    require_dependencies()
except ImportError:
    print("ERROR: check_dependencies.py not found. Please ensure it exists in the repo root.", file=sys.stderr)
    sys.exit(1)

from fhir_tablesaw_3tier.env import load_dotenv, get_data_source_config  # noqa: E402

# Import main go module
import go  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    """Main entry point for test server processing."""
    # Load environment variables
    load_dotenv(override=True)
    
    # Get test server configuration
    try:
        data_dir, schema = get_data_source_config(source="test")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        print("\nPlease ensure TEST_FHIR_DIR and TEST_FHIR_SCHEMA are set in your .env file.", file=sys.stderr)
        return 1
    
    # Set environment variables for this run
    os.environ["FHIR_API_CACHE_FOLDER"] = data_dir
    os.environ["DB_SCHEMA"] = schema
    
    print("=" * 80)
    print("Test Server FHIR Data Processing")
    print("=" * 80)
    print(f"Data directory: {data_dir}")
    print(f"Database schema: {schema}")
    print("=" * 80)
    print()
    
    # Call the main go.py with the same arguments
    return go.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
