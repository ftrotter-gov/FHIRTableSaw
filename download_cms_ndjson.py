#!/usr/bin/env python3
"""
Simple script to download FHIR resources from CMS API to NDJSON files.

This script handles the CMS FHIR API authentication and downloads resources
to a specified directory. It's meant to be a simple, focused downloader.

Usage:
    python download_cms_ndjson.py output_directory

Example:
    python download_cms_ndjson.py /Users/ftrotter/internal_npd_fhir_api_ndjson
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

# Ensure repo src/ is importable
REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fhir_tablesaw_3tier.env import load_dotenv


# CMS internal FHIR server - this is the actual CMS server, not the test server
CMS_FHIR_URL = "https://dev.cnpd.internal.cms.gov/fhir/"


def _require_env(*, name):
    """Get required environment variable or exit."""
    value = os.environ.get(name)
    if not value or value.strip() == "":
        print(f"❌ ERROR: Required environment variable '{name}' is not set in .env")
        sys.exit(1)
    return value


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="download_cms_ndjson.py",
        description="Download FHIR resources from CMS API to NDJSON files",
    )

    parser.add_argument(
        "output_dir",
        help="Output directory for NDJSON files (required)"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Page size for FHIR API requests (default: 1000)"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        dest="stop_after_this_many",
        help="Stop after downloading this many resources (for testing)"
    )

    parser.add_argument(
        "--resource-types",
        default=None,
        help="Comma-separated list of resource types (default: all supported types)"
    )

    parser.add_argument(
        "--cms-url",
        default=None,
        help="Override CMS FHIR server URL (default: https://dev.cnpd.internal.cms.gov/fhir/)"
    )

    args = parser.parse_args()

    # Load environment variables from .env
    load_dotenv(override=True)

    # Determine output directory
    # Intentionally require the output directory to be explicitly provided.
    # This script should fail fast if the caller hasn't decided where files go.
    output_dir = Path(args.output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get CMS FHIR server URL - ALWAYS use CMS server, not test server
    # Priority: 1. Command line arg, 2. CMS_FHIR_URL env var, 3. Hardcoded CMS default
    # Explicitly ignore FHIR_SERVER_URL to avoid using test server
    if args.cms_url:
        fhir_url = args.cms_url
    else:
        fhir_url = os.getenv('CMS_FHIR_URL') or CMS_FHIR_URL

    # Require authentication credentials
    username = _require_env(name='FHIR_API_USERNAME')
    password = _require_env(name='FHIR_API_PASSWORD')

    # Safety check: warn if we're about to use test server
    if 'ndh-server.fast.hl7.org' in fhir_url.lower():
        print("⚠️  WARNING: You're about to download from the TEST server!")
        print(f"    URL: {fhir_url}")
        print("    This is NOT the CMS internal server.")
        print(f"    Expected CMS URL: {CMS_FHIR_URL}")
        print("\nTo use the CMS server, either:")
        print("  1. Don't set FHIR_SERVER_URL in .env (or set CMS_FHIR_URL)")
        print("  2. Use --cms-url flag")
        print("\nContinuing in 3 seconds...")
        import time
        time.sleep(3)

    # Print configuration
    print("\n" + "=" * 80)
    print("CMS FHIR NDJSON Downloader")
    print("=" * 80)
    print(f"FHIR Server:    {fhir_url}")
    print(f"Output Dir:     {output_dir}")
    print(f"Username:       {username}")
    print(f"Password:       {'*' * len(password)}")
    print(f"Page Size:      {args.count}")
    if args.stop_after_this_many:
        print(f"Limit:          {args.stop_after_this_many} resources (testing mode)")
    if args.resource_types:
        print(f"Resource Types: {args.resource_types}")
    print("=" * 80)
    print()

    # Build command to run create_ndjson_from_api.py
    cmd = [
        sys.executable,
        str(REPO_ROOT / "create_ndjson_from_api.py"),
        fhir_url,
        str(output_dir),
        "--count", str(args.count),
    ]

    if args.stop_after_this_many:
        cmd.extend(["--stop-after-this-many", str(args.stop_after_this_many)])

    if args.resource_types:
        cmd.extend(["--resource-types", args.resource_types])

    # Print command being executed
    print("Executing download command:")
    print("  " + " ".join(cmd))
    print("\n" + "=" * 80)
    print()

    # Run the download script
    # The script will use FHIR_API_USERNAME and FHIR_API_PASSWORD from environment
    result = subprocess.run(cmd, cwd=str(REPO_ROOT))

    if result.returncode == 0:
        print("\n" + "=" * 80)
        print("✅ Download Complete!")
        print("=" * 80)
        print(f"\nNDJSON files saved to: {output_dir}")

        # List downloaded files
        ndjson_files = list(output_dir.glob("*.ndjson"))
        if ndjson_files:
            print(f"\nDownloaded {len(ndjson_files)} file(s):")
            for ndjson_file in sorted(ndjson_files):
                size_mb = ndjson_file.stat().st_size / (1024 * 1024)
                print(f"  📄 {ndjson_file.name} ({size_mb:.2f} MB)")
    else:
        print("\n" + "=" * 80)
        print(f"❌ Download failed with exit code {result.returncode}")
        print("=" * 80)
        sys.exit(result.returncode)

    return 0


if __name__ == "__main__":
    sys.exit(main())
