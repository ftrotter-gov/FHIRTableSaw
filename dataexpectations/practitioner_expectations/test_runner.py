#!/usr/bin/env python3
"""
Test runner for Practitioner validation expectations using DuckDB.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dataexpectations.practitioner_expectations.validate_total_count import ValidateTotalPractitionerCount


def main():
    """Run Practitioner validation expectations against DuckDB."""
    
    print("=" * 60)
    print("Practitioner Validation Expectations (DuckDB)")
    print("=" * 60)
    
    # Build configuration dictionary
    config = {
        'cache_dir': '../saw_cache',
        'min_total_practitioners': 1000,  # Initial guess
        'max_total_practitioners': 2000,  # Initial guess
    }
    
    # Run the test
    test = ValidateTotalPractitionerCount()
    print(f"▶ Running: {test.title}")
    
    try:
        result = test.run(engine=None, config=config)
        
        if result is True:
            print("  ✅ PASS")
            sys.exit(0)
        else:
            print(f"  ❌ FAIL: {result}")
            sys.exit(1)
            
    except Exception as e:
        print(f"  💥 ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
