#!/usr/bin/env python3
"""
Example script to load Practitioner NDJSON data using fhir4ds.

This demonstrates the new SQL on FHIR approach using ViewDefinitions.

Usage:
    python scripts/load_practitioner_ndjson.py <path_to_ndjson_file>

Example:
    python scripts/load_practitioner_ndjson.py data/practitioners.ndjson
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv
from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load Practitioner NDJSON data using fhir4ds (SQL on FHIR approach)"
    )
    parser.add_argument("ndjson_file", help="Path to the NDJSON file")
    parser.add_argument(
        "--batchsize",
        type=int,
        default=5000,
        help="Number of resources to process per batch (default: 5000)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing table instead of appending",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of matching resources to process (for testing on production files)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    ndjson_path = args.ndjson_file
    if_exists = "replace" if args.replace else "append"

    print(f"Loading Practitioner data from: {ndjson_path}")
    print(f"Batch size: {args.batchsize} resources per batch")
    if args.limit:
        print(f"Row limit: {args.limit} resources (testing mode)")
    print()

    try:
        result = process_practitioner_ndjson(
            ndjson_path=ndjson_path,
            if_exists=if_exists,
            batch_size=args.batchsize,
            max_rows=args.limit,
        )

        print("=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        print(f"Status: {result['status']}")
        print(f"Total resources in file: {result.get('total_resources', 'N/A')}")
        print(f"Matching Practitioners: {result.get('matching_resources', 'N/A')}")
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
