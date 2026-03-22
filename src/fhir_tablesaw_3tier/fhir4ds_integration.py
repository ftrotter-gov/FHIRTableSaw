"""
Integration module for using fhir4ds with FHIRTableSaw.

This module provides utilities to:
1. Load SQL on FHIR ViewDefinitions
2. Process NDJSON FHIR data using fhir4ds
3. Load flattened data into PostgreSQL
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fhir_tablesaw_3tier.env import get_db_url


class ViewDefinitionLoader:
    """Load and manage SQL on FHIR ViewDefinitions."""

    @staticmethod
    def load_viewdef(*, path: Path | str) -> dict[str, Any]:
        """Load a ViewDefinition from a JSON file.

        Args:
            path: Path to the ViewDefinition JSON file

        Returns:
            ViewDefinition as a dictionary
        """
        path_obj = Path(path) if isinstance(path, str) else path

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

    @staticmethod
    def get_resource_type(*, viewdef: dict[str, Any]) -> str:
        """Extract the resource type from a ViewDefinition.

        Args:
            viewdef: ViewDefinition dictionary

        Returns:
            FHIR resource type (e.g., 'Practitioner')
        """
        return str(viewdef.get("resource", ""))


class NDJSONLoader:
    """Load FHIR resources from NDJSON files."""

    @staticmethod
    def load_ndjson(*, path: Path | str) -> list[dict[str, Any]]:
        """Load FHIR resources from an NDJSON file.

        NDJSON (Newline Delimited JSON) format has one JSON object per line.
        Each line should be a complete FHIR resource.

        Args:
            path: Path to the NDJSON file

        Returns:
            List of FHIR resource dictionaries
        """
        path_obj = Path(path) if isinstance(path, str) else path

        if not path_obj.exists():
            raise FileNotFoundError(f"NDJSON file not found: {path}")

        resources = []
        with open(path_obj, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue  # Skip empty lines

                try:
                    resource = json.loads(line)
                    resources.append(resource)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON on line {line_num} in {path}: {e}") from e

        return resources


class FHIR4DSRunner:
    """Run fhir4ds ViewDefinitions against FHIR data and load to PostgreSQL."""

    def __init__(self, *, viewdef_path: Path | str, db_url: str | None = None):
        """Initialize the runner.

        Args:
            viewdef_path: Path to ViewDefinition JSON file
            db_url: PostgreSQL connection URL (default: from environment)
        """
        self.viewdef = ViewDefinitionLoader.load_viewdef(path=viewdef_path)
        self.resource_type = ViewDefinitionLoader.get_resource_type(viewdef=self.viewdef)
        self.db_url = db_url or get_db_url()
        self.table_name = self.viewdef.get("name", "unknown_table")

    def process_ndjson(
        self, *, ndjson_path: Path | str, if_exists: str = "append"
    ) -> dict[str, Any]:
        """Process an NDJSON file using the ViewDefinition and load to PostgreSQL.

        Args:
            ndjson_path: Path to NDJSON file containing FHIR resources
            if_exists: How to handle existing table ('append', 'replace', 'fail')

        Returns:
            Summary dictionary with processing stats
        """
        # Import fhir4ds here so it's only required when actually using this functionality
        try:
            from fhir4ds import FHIRPath
        except ImportError as e:
            raise ImportError(
                "fhir4ds is not installed. Install it with: pip install fhir4ds"
            ) from e

        # Load FHIR resources from NDJSON
        resources = NDJSONLoader.load_ndjson(path=ndjson_path)

        if not resources:
            return {
                "status": "no_data",
                "resources_loaded": 0,
                "message": "No resources found in NDJSON file",
            }

        # Filter resources by type
        matching_resources = [r for r in resources if r.get("resourceType") == self.resource_type]

        if not matching_resources:
            return {
                "status": "no_matching_resources",
                "total_resources": len(resources),
                "matching_resources": 0,
                "expected_type": self.resource_type,
                "message": f"No {self.resource_type} resources found in file",
            }

        # Use fhir4ds to process and load
        fhir = FHIRPath(view_definition=self.viewdef)
        fhir.load(matching_resources)

        # Load to PostgreSQL
        fhir.to_postgres(
            connection_string=self.db_url, table_name=self.table_name, if_exists=if_exists
        )

        return {
            "status": "success",
            "total_resources": len(resources),
            "matching_resources": len(matching_resources),
            "resource_type": self.resource_type,
            "table_name": self.table_name,
            "if_exists": if_exists,
        }


def process_practitioner_ndjson(
    *, ndjson_path: Path | str, viewdef_path: Path | str | None = None, if_exists: str = "append"
) -> dict[str, Any]:
    """Convenience function to process Practitioner NDJSON files.

    Args:
        ndjson_path: Path to Practitioner NDJSON file
        viewdef_path: Path to ViewDefinition (default: viewdefs/practitioner.json)
        if_exists: How to handle existing table ('append', 'replace', 'fail')

    Returns:
        Summary dictionary with processing stats
    """
    if viewdef_path is None:
        # Default to viewdefs/practitioner.json
        project_root = Path(__file__).parent.parent.parent
        viewdef_path = project_root / "viewdefs" / "practitioner.json"

    runner = FHIR4DSRunner(viewdef_path=viewdef_path)
    return runner.process_ndjson(ndjson_path=ndjson_path, if_exists=if_exists)
