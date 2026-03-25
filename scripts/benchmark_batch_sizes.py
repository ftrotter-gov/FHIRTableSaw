#!/usr/bin/env python3
"""
Benchmark different batch sizes on production data.

This script tests various batch sizes to determine optimal performance.
Limited to 100,000 rows to ensure reasonable runtime.

Usage:
    python scripts/benchmark_batch_sizes.py <ndjson_file>

Example:
    python scripts/benchmark_batch_sizes.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv
from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson


def benchmark_batch_size(*, ndjson_path: str, batch_size: int, max_rows: int = 100000) -> dict:
    """Run a single benchmark with given batch size.

    Args:
        ndjson_path: Path to NDJSON file
        batch_size: Batch size to test
        max_rows: Maximum rows to process

    Returns:
        dict with timing and stats
    """
    print(f"\n{'=' * 70}")
    print(f"Testing batch_size={batch_size} (max_rows={max_rows})")
    print(f"{'=' * 70}")

    start_time = time.time()

    result = process_practitioner_ndjson(
        ndjson_path=ndjson_path,
        if_exists="replace",
        batch_size=batch_size,
        max_rows=max_rows,
    )

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\n⏱  Elapsed time: {elapsed:.2f} seconds")
    print(f"📊 Rows loaded: {result.get('rows_in_table', 0)}")
    print(f"📦 Batches processed: {result.get('batches_processed', 0)}")
    print(f"⚡ Rows per second: {result.get('rows_in_table', 0) / elapsed:.2f}")

    return {
        "batch_size": batch_size,
        "elapsed_seconds": elapsed,
        "rows_loaded": result.get("rows_in_table", 0),
        "batches_processed": result.get("batches_processed", 0),
        "rows_per_second": result.get("rows_in_table", 0) / elapsed if elapsed > 0 else 0,
        "status": result.get("status"),
    }


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Benchmark different batch sizes on production data"
    )
    parser.add_argument("ndjson_file", help="Path to the NDJSON file")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100000,
        help="Maximum rows to process per test (default: 100000)",
    )
    parser.add_argument(
        "--batch-sizes",
        type=str,
        default="1000,5000,10000,25000,50000",
        help="Comma-separated list of batch sizes to test (default: 1000,5000,10000,25000,50000)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Validate file exists
    ndjson_path = args.ndjson_file
    if not Path(ndjson_path).exists():
        print(f"ERROR: File not found: {ndjson_path}")
        sys.exit(1)

    # Parse batch sizes
    try:
        batch_sizes = [int(x.strip()) for x in args.batch_sizes.split(",")]
    except ValueError as e:
        print(f"ERROR: Invalid batch sizes: {e}")
        sys.exit(1)

    print(f"\n{'=' * 70}")
    print("BATCH SIZE BENCHMARK")
    print(f"{'=' * 70}")
    print(f"File: {ndjson_path}")
    print(f"Max rows per test: {args.max_rows}")
    print(f"Batch sizes to test: {batch_sizes}")
    print(f"{'=' * 70}")

    # Run benchmarks
    results = []
    for batch_size in batch_sizes:
        try:
            result = benchmark_batch_size(
                ndjson_path=ndjson_path,
                batch_size=batch_size,
                max_rows=args.max_rows,
            )
            results.append(result)
            # Brief pause between tests
            time.sleep(2)
        except Exception as e:
            print(f"\n❌ ERROR with batch_size={batch_size}: {e}")
            import traceback

            traceback.print_exc()

    # Summary
    print(f"\n{'=' * 70}")
    print("BENCHMARK RESULTS SUMMARY")
    print(f"{'=' * 70}")
    print(
        f"{'Batch Size':>12} | {'Time (s)':>10} | {'Rows':>10} | {'Batches':>10} | {'Rows/sec':>12}"
    )
    print(f"{'-' * 70}")

    for r in results:
        print(
            f"{r['batch_size']:>12} | "
            f"{r['elapsed_seconds']:>10.2f} | "
            f"{r['rows_loaded']:>10} | "
            f"{r['batches_processed']:>10} | "
            f"{r['rows_per_second']:>12.2f}"
        )

    # Find best
    if results:
        best = max(results, key=lambda x: x["rows_per_second"])
        print(f"\n{'=' * 70}")
        print(f"🏆 BEST PERFORMANCE: batch_size={best['batch_size']}")
        print(f"   {best['rows_per_second']:.2f} rows/second")
        print(f"   {best['elapsed_seconds']:.2f} seconds total")
        print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
