"""
CSV exporter for FHIR ViewDefinitions using fhir4ds.

This module executes SQL-on-FHIR ViewDefinitions against a FHIRDataStore
and exports the flattened results to CSV files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def get_default_csv_path(*, ndjson_path: str, view_name: str) -> str:
    """Get default CSV path from NDJSON path and view name.

    Args:
        ndjson_path: Path to NDJSON file (e.g., '/path/to/Practitioner.ndjson')
        view_name: ViewDefinition name (e.g., 'practitioner')

    Returns:
        Path to CSV file (e.g., '/path/to/Practitioner_practitioner.csv')
    """
    path_obj = Path(ndjson_path)
    base_name = path_obj.stem  # 'Practitioner' from 'Practitioner.ndjson'
    directory = path_obj.parent
    return str(directory / f"{base_name}_{view_name}.csv")


class ViewDefinitionCSVExporter:
    """Execute ViewDefinitions and export results to CSV."""

    @staticmethod
    def load_viewdef(*, path: str) -> dict[str, Any]:
        """Load a ViewDefinition from JSON file.

        Args:
            path: Path to ViewDefinition JSON file

        Returns:
            ViewDefinition dictionary
        """
        path_obj = Path(path)

        if not path_obj.exists():
            raise FileNotFoundError(f"ViewDefinition file not found: {path}")

        with open(path_obj, "r", encoding="utf-8") as f:
            viewdef = json.load(f)

        # Validate it's a ViewDefinition
        if viewdef.get("resourceType") != "ViewDefinition":
            raise ValueError(
                f"Invalid ViewDefinition: resourceType is {viewdef.get('resourceType')}, "
                f"expected 'ViewDefinition'"
            )

        return viewdef

    def export_view_to_csv(
        self,
        *,
        datastore: Any,
        viewdef_path: str,
        ndjson_path: str,
        csv_path: str | None = None,
        force_overwrite: bool = False,
    ) -> dict[str, Any]:
        """Execute ViewDefinition on datastore and export to CSV.

        Args:
            datastore: FHIRDataStore instance (already loaded with resources)
            viewdef_path: Path to ViewDefinition JSON file
            ndjson_path: Original NDJSON path (for default CSV naming)
            csv_path: Path to output CSV (default: same dir as NDJSON)
            force_overwrite: Overwrite existing CSV file

        Returns:
            Dictionary with export stats
        """
        # Load ViewDefinition
        viewdef = self.load_viewdef(path=viewdef_path)
        view_name = viewdef.get("name", "output")

        # Determine CSV path
        csv_file = csv_path or get_default_csv_path(ndjson_path=ndjson_path, view_name=view_name)

        # Create parent directories if needed
        Path(csv_file).parent.mkdir(parents=True, exist_ok=True)

        # Check if CSV already exists
        if Path(csv_file).exists() and not force_overwrite:
            print(f"✓ CSV already exists: {csv_file}")
            print("  (use --force-overwrite to regenerate)")
            return {
                "status": "skipped",
                "csv_path": csv_file,
                "message": "CSV already exists",
            }

        print("Executing ViewDefinition...")
        print(f"  ViewDefinition: {viewdef_path}")
        print(f"  View name: {view_name}")
        print()

        # Execute ViewDefinition
        view_runner = datastore.view_runner()
        result = view_runner.execute_view_definition(viewdef)

        # Convert to DataFrame
        result_df = result.to_dataframe()

        if result_df.empty:
            print("⚠ Warning: ViewDefinition returned no rows")
            return {
                "status": "empty",
                "csv_path": csv_file,
                "rows_exported": 0,
                "message": "No data matched ViewDefinition",
            }

        # Export to CSV
        print(f"Exporting to CSV: {csv_file}")
        result_df.to_csv(csv_file, index=False)

        print(f"✓ Exported {len(result_df)} rows")
        print(f"✓ Created: {csv_file}")

        return {
            "status": "success",
            "csv_path": csv_file,
            "rows_exported": len(result_df),
            "view_name": view_name,
            "columns": list(result_df.columns),
        }
