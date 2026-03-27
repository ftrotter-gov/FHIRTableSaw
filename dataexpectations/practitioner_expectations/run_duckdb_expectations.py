#!/usr/bin/env python3
"""
Run all Practitioner validation expectations against DuckDB cache.

This demonstrates InLaw working with DuckDB instead of PostgreSQL.
"""
import sys
from pathlib import Path
from sqlalchemy import create_engine

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.inlaw import InLaw


def main():
    """Run all Practitioner validation expectations against DuckDB."""
    
    print("=" * 60)
    print("Practitioner InLaw Validation (DuckDB)")
    print("=" * 60)
    print()
    
    # Create DuckDB engine
    duckdb_path = project_root / ".." / "saw_cache" / "practitioner.duckdb"
    
    if not duckdb_path.exists():
        print(f"❌ ERROR: DuckDB file not found: {duckdb_path}")
        print("   Run: python go.py --stop-after-this-many 1000")
        sys.exit(1)
    
    print(f"📂 DuckDB file: {duckdb_path}")
    
    # Create SQLAlchemy engine for DuckDB
    engine = create_engine(f"duckdb:///{duckdb_path}")
    
    # Build configuration dictionary
    # Note: DuckDB has a different schema - it stores raw FHIR JSON
    config = {
        'duckdb_path': str(duckdb_path),
        'min_total_practitioners': 1500,  # We saw 1687 earlier
        'max_total_practitioners': 2000,
    }
    
    print(f"📊 Expected practitioner count: {config['min_total_practitioners']}-{config['max_total_practitioners']}")
    print()
    
    # Get all InLaw test files in this directory
    test_dir = Path(__file__).parent
    test_files = [
        str(test_dir / 'validate_row_count.py'),
        str(test_dir / 'validate_npi.py'),
        str(test_dir / 'validate_required_fields.py'),
    ]
    
    # Run all InLaw tests
    try:
        results = InLaw.run_all(
            engine=engine,
            inlaw_files=test_files,
            config=config
        )

        
        print()
        print("=" * 60)
        print("FINAL RESULTS")
        print("=" * 60)
        print(f"✅ Passed: {results['passed']}")
        print(f"❌ Failed: {results['failed']}")
        print(f"💥 Errors: {results['errors']}")
        print(f"📋 Total:  {results['total']}")
        
        # Exit with appropriate code
        if results['failed'] > 0 or results['errors'] > 0:
            sys.exit(1)
        else:
            print()
            print("🎉 All validation tests passed!")
            sys.exit(0)
        
    except Exception as e:
        print(f"❌ Validation suite failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
