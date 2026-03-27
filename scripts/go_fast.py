#!/usr/bin/env python3
"""
Batch load FHIR NDJSON files using the FAST pipeline (DuckDB → CSV → PostgreSQL).

This script automatically detects resource types from filenames and processes them
using the fast local DuckDB approach (31x faster than the old method).

Usage:
    python scripts/go_fast.py <directory> [OPTIONS]

Examples:
    # Load test files (*.5.ndjson) with upload to PostgreSQL
    python scripts/go_fast.py /Volumes/eBolt/palantir/ndjson/initial --test --upload

    # Load production files without PostgreSQL upload (CSV only)
    python scripts/go_fast.py /path/to/ndjson/directory

    # Custom temp directory for DuckDB
    python scripts/go_fast.py /path/to/ndjson/directory --temp-dir /fast/ssd/temp --upload
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.csv_exporter import ViewDefinitionCSVExporter
from fhir_tablesaw_3tier.csv_uploader import CSVPostgreSQLUploader
from fhir_tablesaw_3tier.duckdb_loader import FHIRDuckDBLoader
from fhir_tablesaw_3tier.env import load_dotenv

# Map resource type names to ViewDefinition files
VIEWDEF_MAP = {
    "endpoint": "viewdefs/endpoint.json",
    "location": "viewdefs/location.json",
    "organization": "viewdefs/organization.json",
    "organizationaffiliation": "viewdefs/organization_affiliation.json",
    "practitioner": "viewdefs/practitioner.json",
    "practitionerrole": "viewdefs/practitioner_role.json",
}


def detect_resource_type(*, filename: str) -> str | None:
    """Detect FHIR resource type from filename.

    Args:
        filename: Name of the NDJSON file (e.g., 'Practitioner.5.ndjson')

    Returns:
        Lowercase resource type (e.g., 'practitioner') or None if not recognized
    """
    # Remove .ndjson extension and any .### pattern
    name = filename.lower()
    name = name.replace(".ndjson", "")
    # Remove .### pattern (e.g., .5, .100)
    name = re.sub(r"\.\d+$", "", name)

    return name if name in VIEWDEF_MAP else None


def find_ndjson_files(*, directory: Path, test_mode: bool) -> list[Path]:
    """Find NDJSON files in directory based on mode.

    Args:
        directory: Directory to search
        test_mode: If True, find *.5.ndjson; if False, find *.ndjson excluding *.###.ndjson

    Returns:
        List of matching file paths
    """
    if test_mode:
        # Test mode: only *.5.ndjson files
        pattern = "*.5.ndjson"
        files = list(directory.glob(pattern))
    else:
        # Production mode: *.ndjson excluding *.###.ndjson pattern
        all_ndjson = directory.glob("*.ndjson")
        # Exclude files matching *.###.ndjson pattern
        numbered_pattern = re.compile(r".*\.\d+\.ndjson$")
        files = [f for f in all_ndjson if not numbered_pattern.match(f.name)]

    return sorted(files)


def process_file(
    *,
    file_path: Path,
    resource_type: str,
    viewdef_path: str,
    temp_dir: str | None,
    batch_size: int,
    upload: bool,
    upload_mode: str,
) -> dict:
    """Process a single NDJSON file using the fast pipeline.

    Args:
        file_path: Path to NDJSON file
        resource_type: Detected resource type
        viewdef_path: Path to ViewDefinition JSON
        temp_dir: Directory for DuckDB temp files
        batch_size: Number of resources to process per batch
        upload: Whether to upload to PostgreSQL
        upload_mode: PostgreSQL upload mode (replace/append/fail)

    Returns:
        Result dictionary with processing stats
    """
    start_time = time.time()

    print(f"\n{'=' * 70}")
    print(f"Processing: {file_path.name}")
    print(f"Resource Type: {resource_type.title()}")
    print(f"ViewDefinition: {viewdef_path}")
    print(f"Batch Size: {batch_size} resources per batch")
    if upload:
        print(f"Upload Mode: {upload_mode}")
    print(f"{'=' * 70}\n")

    try:
        # Stage 1: Load to DuckDB
        print("STAGE 1: Loading NDJSON to DuckDB (local)")
        print("-" * 70)

        loader = FHIRDuckDBLoader()
        load_result = loader.load_ndjson_to_duckdb(
            ndjson_path=str(file_path),
            temp_dir=temp_dir,
            force_reload=True,  # Always reload for batch processing
            batch_size=batch_size,
        )

        if load_result["status"] != "success":
            return load_result

        datastore = load_result.get("datastore")
        if not datastore:
            return {"status": "error", "message": "No datastore returned"}

        print()

        # Stage 2: Export to CSV
        print("STAGE 2: Executing ViewDefinition and exporting CSV")
        print("-" * 70)

        exporter = ViewDefinitionCSVExporter()
        export_result = exporter.export_view_to_csv(
            datastore=datastore,
            viewdef_path=viewdef_path,
            ndjson_path=str(file_path),
            force_overwrite=True,  # Always overwrite for batch processing
        )

        if export_result["status"] not in ["success", "empty"]:
            return export_result

        csv_path = export_result.get("csv_path")
        view_name = export_result.get("view_name", resource_type)

        print()

        # Stage 3: Upload to PostgreSQL (optional)
        upload_result = None
        if upload and export_result["status"] == "success" and csv_path:
            print("STAGE 3: Uploading CSV to PostgreSQL")
            print("-" * 70)

            uploader = CSVPostgreSQLUploader()
            upload_result = uploader.upload_csv(
                csv_path=csv_path,
                table_name=view_name,
                if_exists=upload_mode,
            )
            uploader.close()

            if upload_result["status"] != "success":
                return upload_result

            print()

        # Calculate total time
        elapsed = time.time() - start_time

        return {
            "status": "success",
            "resources_processed": load_result.get("total_resources", 0),
            "rows_exported": export_result.get("rows_exported", 0),
            "csv_path": csv_path,
            "uploaded": upload,
            "upload_table": upload_result.get("full_table_path") if upload_result else None,
            "elapsed_seconds": elapsed,
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def main() -> None:
    """Main entry point."""
    # Load environment variables FIRST so we can use them for defaults
    load_dotenv()

    # Get defaults from environment variables
    default_batch_size = int(os.environ.get("BATCH_SIZE", "5000"))
    default_upload = os.environ.get("UPLOAD_TO_POSTGRESQL", "false").lower() in ("true", "1", "yes")
    default_upload_mode = os.environ.get("UPLOAD_MODE", "replace")
    default_temp_dir = os.environ.get("TEMP_DIR") or os.environ.get("FHIR_API_CACHE_FOLDER")

    parser = argparse.ArgumentParser(
        description="Batch load FHIR NDJSON files using FAST pipeline (31x faster!)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test mode with PostgreSQL upload
  python scripts/go_fast.py /Volumes/eBolt/palantir/ndjson/initial --test --upload

  # Production mode, CSV only (no upload)
  python scripts/go_fast.py /path/to/ndjson/directory

  # Production with custom temp directory
  python scripts/go_fast.py /path/to/ndjson/directory --temp-dir /fast/ssd/temp --upload

Configuration:
  Settings can be configured via .env file or CLI arguments (CLI takes precedence):
    BATCH_SIZE - Resources per batch (default: 5000)
    UPLOAD_TO_POSTGRESQL - Upload to PostgreSQL: true/false (default: false)
    UPLOAD_MODE - PostgreSQL mode: replace/append/fail (default: replace)
    TEMP_DIR or FHIR_API_CACHE_FOLDER - Directory for DuckDB temp files
        """,
    )
    parser.add_argument("directory", help="Directory containing NDJSON files")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: load only *.5.ndjson files",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        default=default_upload,
        help=f"Upload CSV files to PostgreSQL (default from .env: {default_upload})",
    )
    parser.add_argument(
        "--no-upload",
        action="store_false",
        dest="upload",
        help="Disable PostgreSQL upload (CSV only)",
    )
    parser.add_argument(
        "--upload-mode",
        choices=["replace", "append", "fail"],
        default=default_upload_mode,
        help=f"PostgreSQL upload mode (default from .env: {default_upload_mode})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=default_batch_size,
        help=f"Resources per batch for DuckDB loading (default from .env: {default_batch_size})",
    )
    parser.add_argument(
        "--temp-dir",
        default=default_temp_dir,
        help=f"Directory for DuckDB temp files (default from .env: {default_temp_dir or 'same as NDJSON'})",
    )

    args = parser.parse_args()

    # Validate directory
    directory = Path(args.directory)
    if not directory.exists():
        print(f"ERROR: Directory not found: {directory}")
        sys.exit(1)
    if not directory.is_dir():
        print(f"ERROR: Not a directory: {directory}")
        sys.exit(1)

    # Find matching files
    mode_text = "TEST MODE (*.5.ndjson)" if args.test else "PRODUCTION MODE (*.ndjson)"

    print(f"\n{'=' * 70}")
    print("FAST FHIR NDJSON Batch Loader (31x faster!)")
    print(f"{'=' * 70}")
    print(f"Directory: {directory}")
    print(f"Mode: {mode_text}")
    print(f"Batch size: {args.batch_size} resources")
    print(f"Upload to PostgreSQL: {'YES' if args.upload else 'NO (CSV only)'}")
    if args.upload:
        print(f"Upload mode: {args.upload_mode}")
    if args.temp_dir:
        print(f"Temp directory: {args.temp_dir}")
    print(f"Configuration: Using .env + CLI arguments")
    print(f"{'=' * 70}")

    files = find_ndjson_files(directory=directory, test_mode=args.test)

    if not files:
        print(f"\n⚠ No matching NDJSON files found in {directory}")
        sys.exit(0)

    print(f"\nFound {len(files)} matching file(s):")
    processable_files: list[tuple[Path, str, str]] = []

    for f in files:
        resource_type = detect_resource_type(filename=f.name)
        if resource_type:
            viewdef = VIEWDEF_MAP[resource_type]
            processable_files.append((f, resource_type, viewdef))
            print(f"  ✓ {f.name} → {resource_type} ({viewdef})")
        else:
            print(f"  ✗ {f.name} → UNKNOWN resource type")

    if not processable_files:
        print("\n⚠ No supported resource types found in matching files")
        sys.exit(0)

    print(f"\nWill process {len(processable_files)} file(s)")

    # Process each file
    results = []
    errors = []
    total_start = time.time()

    for file_path, resource_type, viewdef_path in processable_files:
        try:
            result = process_file(
                file_path=file_path,
                resource_type=resource_type,
                viewdef_path=viewdef_path,
                temp_dir=args.temp_dir,
                batch_size=args.batch_size,
                upload=args.upload,
                upload_mode=args.upload_mode,
            )
            results.append((file_path.name, resource_type, result))

            if result["status"] == "success":
                elapsed = result.get("elapsed_seconds", 0)
                resources = result.get("resources_processed", 0)
                rows = result.get("rows_exported", 0)
                print(f"✓ SUCCESS: {resources} resources → {rows} rows in {elapsed:.2f}s")
            else:
                print(f"⚠ {result['status']}: {result.get('message', 'Unknown issue')}")

        except Exception as e:
            print(f"✗ ERROR: {e}")
            import traceback

            traceback.print_exc()
            errors.append((file_path.name, str(e)))

    total_elapsed = time.time() - total_start

    # Final summary
    print(f"\n{'=' * 70}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'=' * 70}")

    successful = [r for r in results if r[2]["status"] == "success"]
    print(f"Files processed: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Errors: {len(errors)}")
    print(f"Total time: {total_elapsed / 60:.1f} minutes ({total_elapsed:.1f} seconds)")

    if successful:
        print("\n✓ Successfully processed:")
        total_resources = 0
        total_rows = 0
        for filename, res_type, result in successful:
            resources = result.get("resources_processed", 0)
            rows = result.get("rows_exported", 0)
            elapsed = result.get("elapsed_seconds", 0)
            csv_path = result.get("csv_path", "N/A")

            total_resources += resources
            total_rows += rows

            print(f"\n  {filename}:")
            print(f"    Resources: {resources}")
            print(f"    Rows exported: {rows}")
            print(f"    Time: {elapsed:.2f}s")
            print(f"    CSV: {csv_path}")

            if result.get("uploaded"):
                table = result.get("upload_table", "N/A")
                print(f"    PostgreSQL: {table}")

        print("\n  TOTALS:")
        print(f"    Resources processed: {total_resources:,}")
        print(f"    Rows exported: {total_rows:,}")
        print(f"    Average throughput: {total_resources / total_elapsed:.1f} resources/sec")

    if errors:
        print("\n✗ Errors:")
        for filename, error in errors:
            print(f"  - {filename}: {error}")

    print(f"\n{'=' * 70}")

    # Exit with error code if any failures
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
