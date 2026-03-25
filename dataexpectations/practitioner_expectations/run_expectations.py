#!/usr/bin/env python3
"""
Run all Practitioner validation expectations.

This script loads the database configuration, builds a config dictionary,
and runs all InLaw validation tests in this directory.

Usage:
    python dataexpectations/practitioner_expectations/run_expectations.py
"""
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.fhir_tablesaw_3tier.env import load_dotenv, get_db_url, get_db_schema
from src.fhir_tablesaw_3tier.db.engine import create_engine_with_schema
from src.utils.inlaw import InLaw


def main():
    """Run all Practitioner validation expectations."""
    
    # Load environment variables
    load_dotenv()
    
    # Get database connection details
    db_url = get_db_url()
    db_schema = get_db_schema()
    
    print("=" * 60)
    print("Practitioner Validation Expectations")
    print("=" * 60)
    print(f"Database: {db_url.split('@')[-1] if '@' in db_url else 'local'}")
    print(f"Schema: {db_schema}")
    print()
    
    # Create database engine
    engine = create_engine_with_schema(db_url=db_url, schema=db_schema)
    
    # Build configuration dictionary
    config = {
        'schema': db_schema,
        'practitioner_table': 'practitioners',
        'min_expected_rows': 1,
        'max_expected_rows': 10000000,
    }
    
    # Run all InLaw tests in this directory
    try:
        results = InLaw.run_all(
            engine=engine,
            inlaw_dir=str(Path(__file__).parent),
            config=config
        )
        
        print()
        print("=" * 60)
        print(f"✅ All validation tests completed successfully!")
        print(f"   Passed: {results['passed']}")
        print(f"   Failed: {results['failed']}")
        print(f"   Errors: {results['errors']}")
        print("=" * 60)
        
        # Exit with appropriate code
        if results['failed'] > 0 or results['errors'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Validation tests failed with error:")
        print(f"   {str(e)}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
