#!/usr/bin/env python3
"""
Main FHIR NDJSON to Neo4j import orchestrator.

This script discovers FHIR NDJSON files in a directory and imports them into Neo4j.
It uses exact filename matching to prevent misclassification (e.g., OrganizationAffiliation
vs Organization).

Usage:
    python import_ndjson.py /path/to/ndjson/directory [options]

Example:
    python import_ndjson.py /data/fhir --batch-size 1000 --verbose
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Type, List

# Import all resource importers
from importers import (
    BaseImporter,
    PractitionerImporter,
    PractitionerRoleImporter,
    OrganizationImporter,
    OrganizationAffiliationImporter,
    EndpointImporter,
    LocationImporter,
)


# Map resource types to their importers
IMPORTER_MAP: Dict[str, Type[BaseImporter]] = {
    'Practitioner': PractitionerImporter,
    'PractitionerRole': PractitionerRoleImporter,
    'Organization': OrganizationImporter,
    'OrganizationAffiliation': OrganizationAffiliationImporter,
    'Endpoint': EndpointImporter,
    'Location': LocationImporter,
}


def discover_ndjson_files(*, directory: Path) -> Dict[str, Path]:
    """
    Discover FHIR NDJSON files in the specified directory.
    
    Uses EXACT filename matching to prevent misclassification:
    - Organization.ndjson matches Organization
    - OrganizationAffiliation.ndjson matches OrganizationAffiliation (NOT Organization)
    - Practitioner.ndjson matches Practitioner
    - PractitionerRole.ndjson matches PractitionerRole (NOT Practitioner)
    
    Args:
        directory: Path to directory containing NDJSON files
    
    Returns:
        Dictionary mapping resource type to file path
    """
    discovered = {}
    
    if not directory.exists():
        print(f"Error: Directory does not exist: {directory}")
        return discovered
    
    if not directory.is_dir():
        print(f"Error: Path is not a directory: {directory}")
        return discovered
    
    # Look for files matching supported resource types
    for resource_type in IMPORTER_MAP.keys():
        # Try exact match: ResourceType.ndjson
        filepath = directory / f"{resource_type}.ndjson"
        if filepath.exists() and filepath.is_file():
            discovered[resource_type] = filepath
            print(f"Found: {resource_type} -> {filepath}")
    
    return discovered


def load_neo4j_config() -> Dict[str, str]:
    """
    Load Neo4j connection configuration from environment variables.
    
    Returns:
        Dictionary with connection details
    """
    config = {
        'uri': os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
        'user': os.getenv('NEO4J_USER', 'neo4j'),
        'password': os.getenv('NEO4J_PASSWORD', ''),
    }
    
    if not config['password']:
        print("Warning: NEO4J_PASSWORD environment variable not set!")
        print("Set it using: export NEO4J_PASSWORD=your_password")
        print("Or create a .env file in the analysis_neo4j directory")
        sys.exit(1)
    
    return config


def import_resource_files(*, files: Dict[str, Path], batch_size: int, verbose: bool) -> None:
    """
    Import all discovered NDJSON files into Neo4j.
    
    Args:
        files: Dictionary mapping resource type to file path
        batch_size: Number of records to process per batch
        verbose: Enable verbose output
    """
    if not files:
        print("No FHIR NDJSON files found to import.")
        return
    
    # Load Neo4j configuration
    config = load_neo4j_config()
    
    print(f"\nConnecting to Neo4j at {config['uri']}...")
    print(f"Importing {len(files)} resource types with batch size {batch_size}\n")
    
    # Import order matters for relationship creation
    # Import base resources before resources that reference them
    import_order = [
        'Practitioner',      # Base resource
        'Organization',      # Base resource
        'Location',          # Base resource
        'Endpoint',          # Base resource
        'PractitionerRole',  # References Practitioner, Organization, Location, Endpoint
        'OrganizationAffiliation',  # References Organization, Location, Endpoint
    ]
    
    # Import in the specified order
    for resource_type in import_order:
        if resource_type not in files:
            if verbose:
                print(f"Skipping {resource_type} (not found)")
            continue
        
        filepath = files[resource_type]
        importer_class = IMPORTER_MAP[resource_type]
        
        print(f"\n{'='*60}")
        print(f"Importing {resource_type}")
        print(f"{'='*60}")
        
        try:
            importer = importer_class(
                neo4j_uri=config['uri'],
                neo4j_user=config['user'],
                neo4j_password=config['password'],
                verbose=verbose
            )
            
            importer.import_file(filepath=filepath, batch_size=batch_size)
            importer.close()
            
            print(f"✓ Successfully imported {resource_type}")
            
        except Exception as e:
            print(f"✗ Error importing {resource_type}: {e}")
            if verbose:
                import traceback
                traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("Import completed!")
    print(f"{'='*60}\n")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Import FHIR NDJSON files into Neo4j graph database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import all FHIR resources from a directory
  python import_ndjson.py /data/fhir
  
  # Use custom batch size and verbose output
  python import_ndjson.py /data/fhir --batch-size 500 --verbose
  
  # Load environment from .env file (recommended)
  export NEO4J_PASSWORD=your_password
  python import_ndjson.py /data/fhir

File Naming:
  Files must be named exactly as the resource type with .ndjson extension:
  - Practitioner.ndjson
  - PractitionerRole.ndjson
  - Organization.ndjson
  - OrganizationAffiliation.ndjson
  - Endpoint.ndjson
  - Location.ndjson
        """
    )
    
    parser.add_argument(
        'directory',
        type=Path,
        help='Directory containing FHIR NDJSON files'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records to process per batch (default: 1000)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    # Load .env file if it exists
    env_file = Path(__file__).parent.parent / '.env'
    if env_file.exists():
        print(f"Loading environment from {env_file}")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    # Discover NDJSON files
    print(f"Scanning directory: {args.directory}\n")
    files = discover_ndjson_files(directory=args.directory)
    
    if not files:
        print("\nNo supported FHIR NDJSON files found.")
        print("Expected filenames: Practitioner.ndjson, Organization.ndjson, etc.")
        sys.exit(1)
    
    # Import files
    import_resource_files(
        files=files,
        batch_size=args.batch_size,
        verbose=args.verbose
    )


if __name__ == '__main__':
    main()
