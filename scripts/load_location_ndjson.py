#!/usr/bin/env python3
"""
Script to load Location NDJSON data using fhir4ds.

This demonstrates the SQL on FHIR approach using ViewDefinitions.

Usage:
    python scripts/load_location_ndjson.py <path_to_ndjson_file>

Example:
    python scripts/load_location_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv
from fhir_tablesaw_3tier.fhir4ds_integration import process_location_ndjson


def main() -> None:
    """Main entry point."""
    # Load environment variables
    load_dotenv()

    if len(sys.argv) < 2:
        print("ERROR: Missing NDJSON file path")
        print()
        print("Usage:")
        print(f"  python {sys.argv[0]} <path_to_ndjson_file>")
        print()
        print("Example:")
        print(f"  python {sys.argv[0]} /Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson")
        sys.exit(1)

    ndjson_path = sys.argv[1]

    print(f"Loading Location data from: {ndjson_path}")
    print()

    try:
        result = process_location_ndjson(
            ndjson_path=ndjson_path,
            if_exists="append",  # Change to 'replace' to drop/recreate table
        )

        print("=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        print(f"Status: {result['status']}")
        print(f"Total resources in file: {result.get('total_resources', 'N/A')}")
        print(f"Matching Locations: {result.get('matching_resources', 'N/A')}")
        print(f"Resource type: {result.get('resource_type', 'N/A')}")
        print(f"Saved to: {result.get('full_table_path', 'N/A')}")
        print(f"Rows in table (verified): {result.get('rows_in_table', 'N/A')}")
        print(f"Mode: {result.get('if_exists', 'N/A')}")
        print("=" * 60)

        if result["status"] == "success":
            print()
            print("✓ Data successfully loaded to PostgreSQL!")
            print(f"✓ Verified: {result.get('rows_in_table', 0)} rows in table")
        elif result["status"] == "no_data":
            print()
            print("⚠ No data found in NDJSON file")
        elif result["status"] == "no_matching_resources":
            print()
            print(f"⚠ No {result['expected_type']} resources found in file")
            print(f"  File contains {result['total_resources']} resources of other types")

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except ImportError as e:
        print(f"ERROR: {e}")
        print()
        print("Make sure fhir4ds is installed:")
        print("  pip install fhir4ds")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
