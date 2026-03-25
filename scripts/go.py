#!/usr/bin/env python3
"""
Batch load FHIR NDJSON files from a directory.

This script automatically detects resource types from filenames and loads them
using the appropriate SQL-on-FHIR ViewDefinitions.

Usage:
    python scripts/go.py <directory> [--test]

Examples:
    # Load test files (*.5.ndjson)
    python scripts/go.py /Volumes/eBolt/palantir/ndjson/initial --test

    # Load production files (*.ndjson, excluding *.###.ndjson patterns)
    python scripts/go.py /path/to/ndjson/directory

Modes:
    --test: Load only *.5.ndjson files
    (no flag): Load only *.ndjson files that don't match *.###.ndjson pattern
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fhir_tablesaw_3tier.env import load_dotenv
from fhir_tablesaw_3tier.fhir4ds_integration import (
    process_endpoint_ndjson,
    process_location_ndjson,
    process_organization_affiliation_ndjson,
    process_organization_ndjson,
    process_practitioner_ndjson,
    process_practitioner_role_ndjson,
)

# Map resource type names (from filenames) to processing functions
RESOURCE_PROCESSORS = {
    "endpoint": process_endpoint_ndjson,
    "location": process_location_ndjson,
    "organization": process_organization_ndjson,
    "organizationaffiliation": process_organization_affiliation_ndjson,
    "practitioner": process_practitioner_ndjson,
    "practitionerrole": process_practitioner_role_ndjson,
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

    return name if name in RESOURCE_PROCESSORS else None


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
    *, file_path: Path, resource_type: str, if_exists: str = "append", batch_size: int = 5000
) -> dict:
    """Process a single NDJSON file.

    Args:
        file_path: Path to NDJSON file
        resource_type: Detected resource type
        if_exists: How to handle existing table
        batch_size: Number of resources to process per batch

    Returns:
        Result dictionary from processing function
    """
    processor = RESOURCE_PROCESSORS[resource_type]

    print(f"\n{'=' * 70}")
    print(f"Processing: {file_path.name}")
    print(f"Resource Type: {resource_type.title()}")
    print(f"Batch Size: {batch_size} resources per batch")
    print(f"{'=' * 70}")

    result = processor(ndjson_path=str(file_path), if_exists=if_exists, batch_size=batch_size)
    return result


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Batch load FHIR NDJSON files from a directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load test files (*.5.ndjson)
  python scripts/go.py /Volumes/eBolt/palantir/ndjson/initial --test

  # Load production files (*.ndjson, excluding *.###.ndjson)
  python scripts/go.py /path/to/ndjson/directory
        """,
    )
    parser.add_argument("directory", help="Directory containing NDJSON files")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: load only *.5.ndjson files",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing tables instead of appending",
    )
    parser.add_argument(
        "--batchsize",
        type=int,
        default=5000,
        help="Number of resources to process per batch (default: 5000)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

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
    if_exists = "replace" if args.replace else "append"

    print(f"\n{'=' * 70}")
    print("FHIR NDJSON Batch Loader")
    print(f"{'=' * 70}")
    print(f"Directory: {directory}")
    print(f"Mode: {mode_text}")
    print(f"Table handling: {if_exists}")
    print(f"{'=' * 70}")

    files = find_ndjson_files(directory=directory, test_mode=args.test)

    if not files:
        print(f"\n⚠ No matching NDJSON files found in {directory}")
        sys.exit(0)

    print(f"\nFound {len(files)} matching file(s):")
    for f in files:
        resource_type = detect_resource_type(filename=f.name)
        status = "✓ Supported" if resource_type else "✗ Unknown resource type"
        print(f"  - {f.name} → {resource_type or 'UNKNOWN'} ({status})")

    # Filter out unsupported resource types
    processable_files: list[tuple[Path, str]] = []
    for f in files:
        resource_type = detect_resource_type(filename=f.name)
        if resource_type is not None:
            processable_files.append((f, resource_type))

    if not processable_files:
        print("\n⚠ No supported resource types found in matching files")
        sys.exit(0)

    print(f"\nWill process {len(processable_files)} file(s)")

    # Process each file
    results = []
    errors = []

    for file_path, resource_type in processable_files:
        try:
            result = process_file(
                file_path=file_path,
                resource_type=resource_type,
                if_exists=if_exists,
                batch_size=args.batchsize,
            )
            results.append((file_path.name, resource_type, result))

            if result["status"] == "success":
                batches = result.get("batches_processed", "N/A")
                print(
                    f"✓ SUCCESS: {result.get('matching_resources', 0)} resources loaded in {batches} batches"
                )
            else:
                print(f"⚠ {result['status']}: {result.get('message', 'Unknown issue')}")

        except Exception as e:
            print(f"✗ ERROR: {e}")
            errors.append((file_path.name, str(e)))

    # Final summary
    print(f"\n{'=' * 70}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'=' * 70}")

    successful = [r for r in results if r[2]["status"] == "success"]
    print(f"Files processed: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Errors: {len(errors)}")

    if successful:
        print("\n✓ Successfully loaded resources:")
        for filename, res_type, result in successful:
            rows = result.get("matching_resources", 0)
            table = result.get("full_table_path", "N/A")
            print(f"  - {filename}: {rows} {res_type}(s) → {table}")

    if errors:
        print("\n✗ Errors:")
        for filename, error in errors:
            print(f"  - {filename}: {error}")

    print(f"\n{'=' * 70}")

    # Exit with error code if any failures
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
