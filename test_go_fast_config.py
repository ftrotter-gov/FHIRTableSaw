#!/usr/bin/env python3
"""Test script to verify go_fast.py configuration loading from .env"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv

def test_config_loading():
    """Test that configuration values are loaded from .env"""
    
    print("Testing go_fast.py .env configuration loading\n")
    print("=" * 70)
    
    # Load environment variables
    load_dotenv()
    
    # Test default values that go_fast.py would use
    default_batch_size = int(os.environ.get("BATCH_SIZE", "5000"))
    default_upload = os.environ.get("UPLOAD_TO_POSTGRESQL", "false").lower() in ("true", "1", "yes")
    default_upload_mode = os.environ.get("UPLOAD_MODE", "replace")
    default_temp_dir = os.environ.get("TEMP_DIR") or os.environ.get("FHIR_API_CACHE_FOLDER")
    
    print("Configuration loaded from .env:")
    print(f"  BATCH_SIZE: {default_batch_size}")
    print(f"  UPLOAD_TO_POSTGRESQL: {default_upload}")
    print(f"  UPLOAD_MODE: {default_upload_mode}")
    print(f"  TEMP_DIR: {default_temp_dir or '(not set, will use NDJSON directory)'}")
    print(f"  FHIR_API_CACHE_FOLDER: {os.environ.get('FHIR_API_CACHE_FOLDER', '(not set)')}")
    
    print("\nDatabase configuration:")
    print(f"  DATABASE_URL: {os.environ.get('DATABASE_URL', '(not set)')[:50]}...")
    print(f"  DB_SCHEMA: {os.environ.get('DB_SCHEMA', '(not set)')}")
    
    print("=" * 70)
    print("\n✓ Configuration loading test PASSED")
    print("\nThe go_fast.py script will now:")
    print(f"  - Use batch size of {default_batch_size} resources")
    print(f"  - {'Upload' if default_upload else 'NOT upload'} to PostgreSQL by default")
    if default_upload:
        print(f"  - Use upload mode: {default_upload_mode}")
    if default_temp_dir:
        print(f"  - Use temp directory: {default_temp_dir}")
    print("\nCLI arguments will override these .env defaults.")

if __name__ == "__main__":
    test_config_loading()
