"""
DuckDB loader for FHIR NDJSON files using fhir4ds.

This module provides fast local processing of FHIR data by loading into
persistent DuckDB databases instead of remote PostgreSQL.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def get_default_duckdb_path(*, ndjson_path: str) -> str:
    """Get default DuckDB path from NDJSON path.

    Args:
        ndjson_path: Path to NDJSON file (e.g., '/path/to/Practitioner.ndjson')

    Returns:
        Path to DuckDB file (e.g., '/path/to/Practitioner.duckdb')
    """
    return str(Path(ndjson_path).with_suffix(".duckdb"))


def iterate_ndjson_batches(*, path: str, batch_size: int = 5000):
    """Iterate over FHIR resources from an NDJSON file in batches.

    Args:
        path: Path to the NDJSON file
        batch_size: Number of resources to yield per batch

    Yields:
        Batches of FHIR resource dictionaries
    """
    path_obj = Path(path)

    if not path_obj.exists():
        raise FileNotFoundError(f"NDJSON file not found: {path}")

    batch = []
    with open(path_obj, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            try:
                resource = json.loads(line)
                batch.append(resource)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {line_num} in {path}: {e}") from e

        # Yield any remaining resources
        if batch:
            yield batch


class FHIRDuckDBLoader:
    """Load FHIR NDJSON to persistent local DuckDB using fhir4ds."""

    def load_ndjson_to_duckdb(
        self,
        *,
        ndjson_path: str,
        duckdb_path: str | None = None,
        temp_dir: str | None = None,
        force_reload: bool = False,
        batch_size: int = 5000,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """Load NDJSON to persistent DuckDB database.

        Args:
            ndjson_path: Path to NDJSON file
            duckdb_path: Path to DuckDB file (default: same dir as NDJSON with .duckdb)
            temp_dir: Directory for DuckDB temp files (default: same dir as NDJSON)
            force_reload: Reload even if DuckDB exists
            batch_size: Number of resources per batch
            max_rows: Maximum rows to load (for testing)

        Returns:
            Dictionary with loading stats
        """
        # Import fhir4ds here so it's only required when using this functionality
        try:
            from fhir4ds import DuckDBDialect, FHIRDataStore
        except ImportError as e:
            raise ImportError(
                "fhir4ds is not installed. Install it with: pip install fhir4ds"
            ) from e

        # Determine DuckDB path
        db_path = duckdb_path or get_default_duckdb_path(ndjson_path=ndjson_path)

        # Create parent directories if needed
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Check if already loaded
        if Path(db_path).exists() and not force_reload:
            print(f"✓ DuckDB already exists: {db_path}")
            print("  (use --force-reload to reload)")
            return {
                "status": "skipped",
                "duckdb_path": db_path,
                "message": "DuckDB already exists",
            }

        # Determine temp directory (default: same directory as NDJSON file)
        if temp_dir is None:
            temp_dir = str(Path(ndjson_path).parent)

        # Create temp directory if it doesn't exist
        Path(temp_dir).mkdir(parents=True, exist_ok=True)

        print("Loading NDJSON to DuckDB...")
        print(f"  Source: {ndjson_path}")
        print(f"  Target: {db_path}")
        print(f"  Temp directory: {temp_dir}")
        print(f"  Batch size: {batch_size}")
        if max_rows:
            print(f"  Max rows: {max_rows}")
        print()

        # Initialize DuckDB datastore with larger memory limits
        # Configure DuckDB to handle large datasets
        import duckdb

        # Create DuckDB connection with persistence
        # Use the specified db_path for persistent storage
        conn = duckdb.connect(database=db_path)

        # Set memory and temp directory limits
        conn.execute("SET memory_limit='8GB'")  # Increase memory limit
        conn.execute(f"SET temp_directory='{temp_dir}'")  # Use specified temp dir
        conn.execute("SET max_temp_directory_size='50GB'")  # Allow large temp files
        conn.execute("SET preserve_insertion_order=false")  # Disable for performance

        # Initialize fhir4ds with configured DuckDB
        dialect = DuckDBDialect()
        dialect.connection = conn  # Use our configured connection
        datastore = FHIRDataStore(dialect=dialect, initialize_table=True)

        # Load resources in batches
        total_resources = 0
        batch_count = 0

        for batch in iterate_ndjson_batches(path=ndjson_path, batch_size=batch_size):
            # Check max_rows limit
            if max_rows and total_resources >= max_rows:
                print(f"\n✓ Reached max_rows limit ({max_rows})")
                break

            # Trim batch if needed
            if max_rows and (total_resources + len(batch)) > max_rows:
                batch = batch[: max_rows - total_resources]

            # Load batch
            datastore.load_resources(batch)
            total_resources += len(batch)
            batch_count += 1

            print(".", end="", flush=True)

        print()
        print(f"\n✓ Loaded {total_resources} resources into DuckDB")
        print(f"✓ DuckDB saved: {db_path}")
        print(f"  Database size: {Path(db_path).stat().st_size / (1024 * 1024):.1f} MB")

        return {
            "status": "success",
            "duckdb_path": db_path,
            "datastore": datastore,  # Return datastore for immediate use
            "total_resources": total_resources,
            "batches_processed": batch_count,
            "batch_size": batch_size,
        }
