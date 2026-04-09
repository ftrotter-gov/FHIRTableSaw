#!/usr/bin/env python3
"""
FAST FHIR NDJSON to Neo4j import script using CREATE (not idempotent).

This script uses CREATE instead of MERGE for 10-100x faster imports.
WARNING: This is NOT idempotent - running twice will create duplicates!
Only use this for initial loads into an empty database.

For updates to existing data, use update_ndjson.py instead.

Usage:
    python import_ndjson_fast.py /path/to/ndjson/directory [options]

Example:
    python import_ndjson_fast.py /data/fhir --batch-size 10000
"""

import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Type, List, Optional
import glob

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
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


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
    
    Supports wildcard matching with confusing pattern detection:
    - Organization.ndjson matches Organization
    - Organization.Something.ndjson matches Organization
    - OrganizationAffiliation.ndjson matches OrganizationAffiliation
    - Practitioner.ndjson matches Practitioner
    - Practitioner.Something.ndjson matches Practitioner
    - PractitionerRole.ndjson matches PractitionerRole
    
    ERRORS on confusing patterns:
    - Practitioner.Role.ndjson (ERROR - use PractitionerRole instead)
    - Organization.Affiliation.ndjson (ERROR - use OrganizationAffiliation instead)
    
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
    
    # Confusing patterns that should error (case-insensitive)
    confusing_patterns = {
        'practitioner.role': 'PractitionerRole',
        'organization.affiliation': 'OrganizationAffiliation',
    }
    
    # Check for confusing patterns first (case-insensitive)
    all_files = list(directory.glob("*.ndjson"))
    for filepath in all_files:
        filename_lower = filepath.name.lower()
        for pattern_lower, correct_name in confusing_patterns.items():
            if filename_lower.startswith(f"{pattern_lower}."):
                print(f"\n{'!'*60}")
                print(f"ERROR: Confusing filename detected!")
                print(f"File: {filepath.name}")
                print(f"This pattern is ambiguous. Please rename to:")
                print(f"  {correct_name}.*.ndjson")
                parts = filepath.stem.split('.', 2)
                if len(parts) > 2:
                    print(f"Example: {correct_name}.{parts[-1]}.ndjson")
                else:
                    print(f"Example: {correct_name}.ndjson")
                print(f"{'!'*60}\n")
                sys.exit(1)
    
    # Look for files matching supported resource types with wildcards (case-insensitive)
    for resource_type in IMPORTER_MAP.keys():
        valid_files = []
        
        # Get all .ndjson files in the directory
        all_ndjson_files = list(directory.glob("*.ndjson"))
        
        for filepath in all_ndjson_files:
            filename = filepath.name
            filename_lower = filename.lower()
            resource_type_lower = resource_type.lower()
            
            # Check if filename starts with resource type (case-insensitive)
            if filename_lower.startswith(f"{resource_type_lower}."):
                # Make sure it's not a longer resource type
                # e.g., organization.* should match, but not organizationaffiliation.*
                after_type = filename_lower[len(resource_type_lower):]
                
                # Valid if it's .ndjson or .*.ndjson
                if after_type == '.ndjson' or (after_type.startswith('.') and after_type.endswith('.ndjson')):
                    valid_files.append(filepath)
        
        # If we found multiple files for the same resource type, take the first one
        # (exact match if exists, otherwise first wildcard match)
        if valid_files:
            chosen_file = valid_files[0]
            discovered[resource_type] = chosen_file
            print(f"Found: {resource_type} -> {chosen_file.name}")
            
            # Warn if multiple files match
            if len(valid_files) > 1:
                print(f"  Warning: Multiple files match {resource_type}, using {chosen_file.name}")
                print(f"  Other matches: {[f.name for f in valid_files[1:]]}")
    
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


def check_neo4j_connection(*, config: Dict[str, str]) -> bool:
    """
    Check if Neo4j server is running and accessible.
    
    Args:
        config: Neo4j connection configuration
    
    Returns:
        True if connection successful, exits program otherwise
    """
    print(f"Checking Neo4j connection at {config['uri']}...")
    
    try:
        driver = GraphDatabase.driver(config['uri'], auth=(config['user'], config['password']))
        
        # Try to verify connectivity
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        
        driver.close()
        print("✓ Neo4j connection successful!\n")
        return True
        
    except ServiceUnavailable as e:
        print(f"\n{'='*60}")
        print("ERROR: Cannot connect to Neo4j server!")
        print(f"{'='*60}")
        print(f"\nNeo4j server at {config['uri']} is not available.")
        print("\nPossible solutions:")
        print("  1. Start Neo4j: cd analysis_neo4j && docker-compose up -d")
        print("  2. Check if Neo4j is running: docker-compose ps")
        print("  3. Check Neo4j logs: docker-compose logs neo4j")
        print(f"\nError details: {e}\n")
        sys.exit(1)
        
    except AuthError as e:
        print(f"\n{'='*60}")
        print("ERROR: Authentication failed!")
        print(f"{'='*60}")
        print(f"\nIncorrect username or password for Neo4j at {config['uri']}.")
        print("\nPossible solutions:")
        print("  1. Check your .env file has the correct password")
        print("  2. Verify NEO4J_PASSWORD environment variable")
        print("  3. Reset Neo4j password if needed")
        print(f"\nError details: {e}\n")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n{'='*60}")
        print("ERROR: Unexpected error connecting to Neo4j!")
        print(f"{'='*60}")
        print(f"\nError: {e}")
        print(f"\nPlease check your Neo4j configuration at {config['uri']}\n")
        sys.exit(1)


def import_resource_files(*, files: Dict[str, Path], batch_size: int, import_tag: Optional[str] = None, limit: Optional[int] = None) -> None:
    """
    Import all discovered NDJSON files into Neo4j.
    
    Args:
        files: Dictionary mapping resource type to file path
        batch_size: Number of records to process per batch
        import_tag: Optional tag to identify this import run (only set on new nodes)
        limit: Optional limit on number of records to import per resource type
    """
    if not files:
        print("No FHIR NDJSON files found to import.")
        return
    
    # Load Neo4j configuration
    config = load_neo4j_config()
    
    # Check Neo4j connection before proceeding
    check_neo4j_connection(config=config)
    
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
                import_tag=import_tag,
                use_create=True  # FAST MODE: Use CREATE instead of MERGE
            )
            
            importer.import_file(filepath=filepath, batch_size=batch_size, limit=limit)
            importer.close()
            
            print(f"✓ Successfully imported {resource_type}")
            
        except Exception as e:
            print(f"✗ Error importing {resource_type}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("Import completed!")
    print(f"{'='*60}\n")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='FAST import of FHIR NDJSON files into Neo4j using CREATE (not idempotent)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # FAST initial import with large batch size (recommended)
  python import_ndjson_fast.py /data/fhir --batch-size 10000
  
  # Test with smaller dataset first
  python import_ndjson_fast.py /data/fhir --batch-size 10000 --limit 1000
  
  # Tag this import run (useful for tracking data sources)
  python import_ndjson_fast.py /data/fhir --batch-size 10000 --import-tag "initial_load_2026"
  
  # Load environment from .env file (recommended)
  export NEO4J_PASSWORD=your_password
  python import_ndjson_fast.py /data/fhir --batch-size 10000

WARNING:
  This script uses CREATE, not MERGE! Running it twice on the same data
  will create duplicate nodes. Only use for initial loads into empty database.
  
  For updates, use: import_or_update_ndjson.py

File Naming:
  Files can be named with the resource type with .ndjson extension:
  - Practitioner.ndjson or Practitioner.*.ndjson
  - PractitionerRole.ndjson or PractitionerRole.*.ndjson
  - Organization.ndjson or Organization.*.ndjson
  - OrganizationAffiliation.ndjson or OrganizationAffiliation.*.ndjson
  - Endpoint.ndjson or Endpoint.*.ndjson
  - Location.ndjson or Location.*.ndjson
  
  Examples:
  - Practitioner.Wyoming.ndjson
  - Organization.Hospitals.ndjson
  - PractitionerRole.Active.ndjson
  
  AVOID confusing patterns (will cause errors):
  - Practitioner.Role.ndjson (use PractitionerRole.*.ndjson instead)
  - Organization.Affiliation.ndjson (use OrganizationAffiliation.*.ndjson instead)
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
        '--import-tag',
        type=str,
        default=None,
        help='Optional tag to identify this import run (only applied to newly created nodes)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit the number of records to import per resource type (useful for testing)'
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
        import_tag=args.import_tag,
        limit=args.limit
    )


if __name__ == '__main__':
    main()
