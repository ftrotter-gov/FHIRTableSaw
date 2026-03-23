#!/usr/bin/env python3
"""
Test script with detailed timing to identify bottlenecks.

This script times each phase of processing to identify where time is spent:
1. Reading NDJSON
2. Loading into fhir4ds
3. Executing ViewDefinition
4. Writing to PostgreSQL

Usage:
    python scripts/test_with_timing.py <path_to_ndjson_file> [--limit ROWS] [--batchsize SIZE]

Example:
    python scripts/test_with_timing.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson --limit 10000 --batchsize 5000
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test FHIR loading with detailed timing")
    parser.add_argument("ndjson_file", help="Path to the NDJSON file")
    parser.add_argument(
        "--batchsize",
        type=int,
        default=5000,
        help="Number of resources to process per batch (default: 5000)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of matching resources to process",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    ndjson_path = args.ndjson_file

    print(f"Testing file: {ndjson_path}")
    print(f"Batch size: {args.batchsize}")
    print(f"Row limit: {args.limit or 'UNLIMITED'}")
    print()

    # Import here after env is loaded
    from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson

    overall_start = time.time()

    try:
        result = process_practitioner_ndjson(
            ndjson_path=ndjson_path,
            if_exists="replace",
            batch_size=args.batchsize,
            max_rows=args.limit,
        )

        overall_end = time.time()
        overall_elapsed = overall_end - overall_start

        print("\n" + "=" * 70)
        print("TIMING RESULTS")
        print("=" * 70)
        print(f"Total elapsed time: {overall_elapsed:.2f} seconds")
        print(f"Rows loaded: {result.get('rows_in_table', 0)}")
        print(f"Batches processed: {result.get('batches_processed', 0)}")
        print(f"Throughput: {result.get('rows_in_table', 0) / overall_elapsed:.2f} rows/second")
        print("=" * 70)

        if result["status"] == "success":
            print("\n✓ Test completed successfully!")
        else:
            print(f"\n⚠ Status: {result['status']}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
