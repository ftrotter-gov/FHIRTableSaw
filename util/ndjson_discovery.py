"""
Utility functions for discovering NDJSON files following naming conventions.

Naming conventions from NamingConventions.md:
- ResourceType.ndjson
- ResourceType.descriptor.ndjson
- ResourceType.descriptor1.descriptor2.ndjson

ONLY files ending in .ndjson are processed (NOT .ndjson.gz)
"""

from pathlib import Path
from typing import Dict, List, Optional


def find_ndjson_files(*, directory: Path, resource_types: Optional[List[str]] = None) -> Dict[str, Path]:
    """
    Discover NDJSON files for FHIR resource types in a directory.
    
    Follows the naming convention from NamingConventions.md:
    - ResourceType.ndjson (exact match, preferred)
    - ResourceType.descriptor.ndjson (with descriptors)
    - ResourceType.descriptor1.descriptor2.ndjson (multiple descriptors)
    
    Args:
        directory: Directory to search for NDJSON files
        resource_types: List of resource types to look for. If None, searches for all supported types.
    
    Returns:
        Dictionary mapping resource type to file path
        
    Raises:
        FileNotFoundError: If directory does not exist
        ValueError: If directory is not a directory
    """
    if not directory.exists():
        raise FileNotFoundError(f"ndjson_discovery.py Error: Directory not found: {directory}")
    
    if not directory.is_dir():
        raise ValueError(f"ndjson_discovery.py Error: Not a directory: {directory}")
    
    # Default supported resource types if none specified
    if resource_types is None:
        resource_types = [
            "Practitioner",
            "PractitionerRole",
            "Organization",
            "OrganizationAffiliation",
            "Location",
            "Endpoint",
        ]
    
    discovered: Dict[str, Path] = {}
    
    for resource_type in resource_types:
        file_path = _find_single_resource_file(resource_type=resource_type, directory=directory)
        if file_path:
            discovered[resource_type] = file_path
    
    return discovered


def _find_single_resource_file(*, resource_type: str, directory: Path) -> Optional[Path]:
    """
    Find NDJSON file for a single resource type.
    
    Args:
        resource_type: FHIR resource type to search for
        directory: Directory to search in
    
    Returns:
        Path to the file, or None if not found
    """
    # Look for exact match first: ResourceType.ndjson
    exact_match = directory / f"{resource_type}.ndjson"
    if exact_match.exists():
        return exact_match
    
    # Look for files with descriptors: ResourceType.*.ndjson
    # Using glob to find all matching files
    pattern = f"{resource_type}.*.ndjson"
    matches = list(directory.glob(pattern))
    
    # Filter to ensure we only get .ndjson files (not .ndjson.gz, etc.)
    valid_matches = [f for f in matches if f.suffix == ".ndjson"]
    
    if valid_matches:
        # Return the first valid match
        # If multiple files exist, user should be warned (handled by caller)
        return valid_matches[0]
    
    return None


def list_all_ndjson_files(*, directory: Path) -> List[Path]:
    """
    List all .ndjson files in a directory (for inspection/debugging).
    
    Args:
        directory: Directory to search
        
    Returns:
        List of all .ndjson file paths
    """
    if not directory.exists() or not directory.is_dir():
        return []
    
    # Find all .ndjson files (NOT .ndjson.gz)
    all_files = directory.glob("*.ndjson")
    return [f for f in all_files if f.suffix == ".ndjson"]
