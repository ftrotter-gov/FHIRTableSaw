#!/usr/bin/env python3
"""
Fast FHIR NDJSON processing using local DuckDB + CSV export + bulk PostgreSQL upload.

This script provides a complete pipeline:
1. Load NDJSON → DuckDB (local, fast)
2. Execute ViewDefinition → CSV export
3. Bulk upload CSV → PostgreSQL (optional)

Usage:
    python scripts/process_ndjson_fast.py <ndjson_file> --viewdef <viewdef_file> [OPTIONS]

Examples:
    # Full pipeline with defaults
    python scripts/process_ndjson_fast.py \
        /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
        --viewdef viewdefs/practitioner.json \
        --upload

    # Custom CSV location
    python scripts/process_ndjson_fast.py \
        /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
        --viewdef viewdefs/practitioner.json \
        --csv-path /output/practitioners.csv

    # Test with limit
    python scripts/process_ndjson_fast.py \
        /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
        --viewdef viewdefs/practitioner.json \
        --limit 10000 \
        --upload
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Add src to path for development
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "src"))
# Add repo root for check_dependencies
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# Check dependencies before importing project modules
try:
    from check_dependencies import require_dependencies  # noqa: E402
    require_dependencies()
except ImportError:
    print("ERROR: check_dependencies.py not found. Please ensure it exists in the repo root.", file=sys.stderr)
    sys.exit(1)

from fhir_tablesaw_3tier.csv_exporter import ViewDefinitionCSVExporter
from fhir_tablesaw_3tier.csv_uploader import CSVPostgreSQLUploader
from fhir_tablesaw_3tier.duckdb_loader import FHIRDuckDBLoader
from fhir_tablesaw_3tier.env import load_dotenv


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fast FHIR NDJSON processing pipeline (DuckDB → CSV → PostgreSQL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument("ndjson_file", help="Path to NDJSON file")
    parser.add_argument("--viewdef", required=True, help="Path to ViewDefinition JSON file")

    # Path configuration
    parser.add_argument(
        "--duckdb-path",
        help="Path to DuckDB file (default: same dir as NDJSON with .duckdb extension)",
    )
    parser.add_argument(
        "--csv-path",
        help="Path to output CSV file (default: same dir as NDJSON with _{view_name}.csv)",
    )
    parser.add_argument(
        "--temp-dir",
        help="Directory for DuckDB temp files (default: same dir as NDJSON)",
    )

    # Processing options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Resources per batch for DuckDB loading (default: 5000)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum resources to process (for testing)",
    )

    # Control flags
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Reload DuckDB even if it exists",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Overwrite CSV even if it exists",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload CSV to PostgreSQL after export",
    )
    parser.add_argument(
        "--table",
        help="PostgreSQL table name (default: from ViewDefinition name)",
    )
    parser.add_argument(
        "--upload-mode",
        choices=["replace", "append", "fail"],
        default="replace",
        help="PostgreSQL upload mode (default: replace)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Start timer
    overall_start = time.time()

    print("=" * 70)
    print("FAST FHIR NDJSON PROCESSING PIPELINE")
    print("=" * 70)
    print(f"Source NDJSON: {args.ndjson_file}")
    print(f"ViewDefinition: {args.viewdef}")
    if args.limit:
        print(f"Row limit: {args.limit} (testing mode)")
    print()

    try:
        # === STAGE 1: Load NDJSON to DuckDB ===
        print("STAGE 1: Loading NDJSON to DuckDB (local)")
        print("-" * 70)

        loader = FHIRDuckDBLoader()
        load_result = loader.load_ndjson_to_duckdb(
            ndjson_path=args.ndjson_file,
            duckdb_path=args.duckdb_path,
            temp_dir=args.temp_dir,
            force_reload=args.force_reload,
            batch_size=args.batch_size,
            max_rows=args.limit,
        )

        if load_result["status"] == "skipped":
            print("Skipping DuckDB load (already exists)")
        elif load_result["status"] != "success":
            print(f"Error: DuckDB loading failed: {load_result}")
            sys.exit(1)

        # Get datastore for next stage
        datastore = load_result.get("datastore")
        if not datastore:
            print("Error: No datastore returned from loader")
            sys.exit(1)

        print()

        # === STAGE 2: Execute ViewDefinition and Export CSV ===
        print("STAGE 2: Executing ViewDefinition and exporting CSV")
        print("-" * 70)

        exporter = ViewDefinitionCSVExporter()
        export_result = exporter.export_view_to_csv(
            datastore=datastore,
            viewdef_path=args.viewdef,
            ndjson_path=args.ndjson_file,
            csv_path=args.csv_path,
            force_overwrite=args.force_overwrite,
        )

        if export_result["status"] == "empty":
            print("Warning: No data matched ViewDefinition")
            sys.exit(0)
        elif export_result["status"] == "skipped":
            print("Skipping CSV export (already exists)")
        elif export_result["status"] != "success":
            print(f"Error: CSV export failed: {export_result}")
            sys.exit(1)

        csv_path = export_result["csv_path"]
        view_name = export_result.get("view_name", "output")

        print()

        # === STAGE 3: Upload CSV to PostgreSQL (Optional) ===
        if args.upload:
            print("STAGE 3: Uploading CSV to PostgreSQL")
            print("-" * 70)

            table_name = args.table or view_name

            uploader = CSVPostgreSQLUploader()
            upload_result = uploader.upload_csv(
                csv_path=csv_path,
                table_name=table_name,
                if_exists=args.upload_mode,
            )

            if upload_result["status"] != "success":
                print(f"Error: PostgreSQL upload failed: {upload_result}")
                sys.exit(1)

            uploader.close()
            print()

        # === SUMMARY ===
        overall_end = time.time()
        overall_elapsed = overall_end - overall_start

        print("=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"Total time: {overall_elapsed:.2f} seconds")
        print(f"Resources processed: {load_result.get('total_resources', 0)}")
        print(f"Rows exported: {export_result.get('rows_exported', 0)}")
        print(f"CSV file: {csv_path}")
        if args.upload:
            print(f"PostgreSQL table: {upload_result.get('full_table_path', 'N/A')}")
        print("=" * 70)
        print()
        print("✓ Pipeline completed successfully!")

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
