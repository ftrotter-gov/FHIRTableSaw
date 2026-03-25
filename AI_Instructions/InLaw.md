# InLaw - Data Validation Framework

InLaw creates validation tests to ensure data quality using Great Expectations. It provides a simple, class-based pattern for writing declarative data validation tests.

**Adapted for FHIRTableSaw**: This version has been simplified from the Dagster-integrated npd_etl version. It uses configuration dictionaries and works as a standalone validation framework.

## Quick Start

```python
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable

class ValidateRowCount(InLaw):
    title = "Table should have expected number of rows"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        # Build table reference
        my_DBTable = DBTable(
            schema=config['schema'],
            table=config['my_table']
        )
        
        # Write SQL to get data to validate
        sql = f"SELECT COUNT(*) AS row_count FROM {my_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Use Great Expectations to validate
        result = gx_df.expect_column_values_to_be_between(
            column="row_count",
            min_value=config['min_rows'],
            max_value=config['max_rows']
        )
        
        if result.success:
            return True
        return f"Row count validation failed: expected {config['min_rows']}-{config['max_rows']}"
```

## Core Pattern

The InLaw pattern follows these steps:

1. **Write SQL that returns data to validate** (not violations)
2. **Convert to Great Expectations DataFrame** using `InLaw.to_gx_dataframe()`
3. **Use Great Expectations methods** to validate the data
4. **Return True or error message** based on validation result

## Directory-Based Organization

InLaw tests are organized in directories by resource type:

```
dataexpectations/
├── __init__.py
├── practitioner_expectations/
│   ├── __init__.py
│   ├── run_expectations.py       # Runner script
│   ├── validate_row_count.py     # Individual test
│   ├── validate_npi.py           # Individual test
│   └── validate_required_fields.py
└── organization_expectations/
    ├── __init__.py
    ├── run_expectations.py
    └── ...
```

## Runner Script Pattern

Each validation suite has a runner script that:

1. Loads database configuration
2. Builds a config dictionary
3. Runs all InLaw tests in the directory

```python
#!/usr/bin/env python3
"""Run all Practitioner validation expectations."""
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
        
        print(f"✅ Passed: {results['passed']}")
        print(f"❌ Failed: {results['failed']}")
        print(f"💥 Errors: {results['errors']}")
        
        sys.exit(0 if results['failed'] == 0 and results['errors'] == 0 else 1)
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

## Configuration Dictionary Pattern

Pass configuration via a dictionary to `InLaw.run_all()`:

```python
config = {
    # Database info
    'schema': 'fhir_tablesaw',
    
    # Table names
    'practitioner_table': 'practitioners',
    'organization_table': 'organizations',
    
    # Validation thresholds
    'min_expected_rows': 1000,
    'max_expected_rows': 10000000,
    
    # Custom parameters
    'allow_null_npi': False,
    'max_null_percentage': 0.05,
}

# Pass to run_all
results = InLaw.run_all(
    engine=engine,
    inlaw_dir='/path/to/tests',
    config=config
)
```

## Common Great Expectations Methods

InLaw uses Great Expectations for validation. Common expectations:

### Range Validation
```python
gx_df.expect_column_values_to_be_between(
    column="row_count",
    min_value=1000,
    max_value=1000000
)
```

### Uniqueness Validation
```python
gx_df.expect_column_values_to_be_unique(column="npi")
```

### Null Validation
```python
# Expect NO nulls
gx_df.expect_column_values_to_not_be_null(column="resource_uuid")

# Expect ALL nulls
gx_df.expect_column_values_to_be_null(column="optional_field")
```

### Count Validation
```python
gx_df.expect_table_row_count_to_equal(value=expected_count)

gx_df.expect_table_row_count_to_be_between(
    min_value=1000,
    max_value=10000
)
```

### Pattern Matching
```python
gx_df.expect_column_values_to_match_regex(
    column="npi",
    regex=r"^\d{10}$"
)
```

## Example: Row Count Validation

```python
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable


class ValidateRowCount(InLaw):
    title = "Practitioner table should have expected number of rows"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        # Build table reference
        practitioner_DBTable = DBTable(
            schema=config.get('schema', 'public'),
            table=config.get('practitioner_table', 'practitioners')
        )
        
        # Query row count
        sql = f"SELECT COUNT(*) AS row_count FROM {practitioner_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Validate
        result = gx_df.expect_column_values_to_be_between(
            column="row_count",
            min_value=config.get('min_expected_rows', 1),
            max_value=config.get('max_expected_rows', 10000000)
        )
        
        if result.success:
            return True
        return "Row count validation failed"
```

## Example: Field Validation

```python
class ValidateNpiFormat(InLaw):
    title = "NPI values should be 10 digits when present"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        practitioner_DBTable = DBTable(
            schema=config['schema'],
            table=config['practitioner_table']
        )
        
        # Query for INVALID records
        sql = f"""
            SELECT COUNT(*) AS invalid_npi_count
            FROM {practitioner_DBTable}
            WHERE npi IS NOT NULL
              AND LENGTH(npi) != 10
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Expect 0 invalid records
        result = gx_df.expect_column_values_to_be_between(
            column="invalid_npi_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found NPIs with invalid length"
```

## Example: Uniqueness Validation

```python
class ValidateNpiUniqueness(InLaw):
    title = "NPI values should be unique (no duplicates)"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        practitioner_DBTable = DBTable(
            schema=config['schema'],
            table=config['practitioner_table']
        )
        
        # Query for duplicate NPIs
        sql = f"""
            SELECT COUNT(*) AS duplicate_npi_count
            FROM (
                SELECT npi, COUNT(*) AS npi_count
                FROM {practitioner_DBTable}
                WHERE npi IS NOT NULL
                GROUP BY npi
                HAVING COUNT(*) > 1
            ) AS duplicates
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Expect 0 duplicates
        result = gx_df.expect_column_values_to_be_between(
            column="duplicate_npi_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found duplicate NPI values"
```

## Example: Complex Multi-Test Suite

```python
class ValidateDataIntegrity(InLaw):
    """Suite of related validation tests."""
    
    title = "Data integrity suite for Practitioners"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        practitioner_DBTable = DBTable(
            schema=config['schema'],
            table=config['practitioner_table']
        )
        
        failures = []
        
        # Test 1: Check for orphaned records
        sql1 = f"""
            SELECT COUNT(*) AS orphan_count
            FROM {practitioner_DBTable} AS pract
            LEFT JOIN {config['schema']}.practitioner_roles AS role
              ON pract.id = role.practitioner_id
            WHERE role.id IS NULL
        """
        gx_df1 = InLaw.to_gx_dataframe(sql1, engine)
        result1 = gx_df1.expect_column_values_to_be_between(
            column="orphan_count",
            min_value=0,
            max_value=100  # Allow some orphans
        )
        if not result1.success:
            failures.append("Too many orphaned practitioners")
        
        # Test 2: Check required fields
        sql2 = f"""
            SELECT COUNT(*) AS missing_required_count
            FROM {practitioner_DBTable}
            WHERE resource_uuid IS NULL
               OR last_name IS NULL
        """
        gx_df2 = InLaw.to_gx_dataframe(sql2, engine)
        result2 = gx_df2.expect_column_values_to_be_between(
            column="missing_required_count",
            min_value=0,
            max_value=0
        )
        if not result2.success:
            failures.append("Found records with missing required fields")
        
        # Return results
        if not failures:
            return True
        return "; ".join(failures)
```

## Running Validations

### From Command Line

```bash
# Run a specific validation suite
python dataexpectations/practitioner_expectations/run_expectations.py

# Set environment variables first if needed
export DATABASE_URL="postgresql://user:pass@localhost/fhirdb"
export DB_SCHEMA="fhir_tablesaw"
python dataexpectations/practitioner_expectations/run_expectations.py
```

### Programmatically

```python
from src.fhir_tablesaw_3tier.db.engine import create_engine_with_schema
from src.utils.inlaw import InLaw

engine = create_engine_with_schema(db_url=db_url, schema=schema)

config = {
    'schema': 'fhir_tablesaw',
    'practitioner_table': 'practitioners',
    'min_expected_rows': 1000,
}

results = InLaw.run_all(
    engine=engine,
    inlaw_dir='dataexpectations/practitioner_expectations',
    config=config
)

print(f"Passed: {results['passed']}, Failed: {results['failed']}")
```

## Skipping Tests

Set the `SKIP_TESTS` environment variable to skip all InLaw tests:

```bash
export SKIP_TESTS=1
python run_expectations.py  # Will skip all tests
```

Or override in code:

```python
results = InLaw.run_all(
    engine=engine,
    inlaw_dir=test_dir,
    config=config,
    ignore_skip_test=True  # Run even if SKIP_TESTS is set
)
```

## Best Practices

1. **Write SQL for data, not violations**: Query the data you want to validate
2. **Use descriptive test titles**: Make failures easy to understand
3. **Return clear error messages**: Include context about what failed
4. **Use config for thresholds**: Don't hardcode limits in test classes
5. **Organize by resource type**: Keep related tests together in directories
6. **Test one thing per class**: Small, focused tests are better
7. **Use DBTable for table references**: Consistent, schema-aware queries

## Key Differences from npd_etl Version

- ✅ **Removed**: Dagster decorators, AssetResult, PostgresResource
- ✅ **Changed**: `settings` parameter → `config` dictionary
- ✅ **Simplified**: No Dagster asset integration
- ✅ **Retained**: All core validation functionality, GX integration, reporting
- ✅ **Added**: Better PostgreSQL schema support via config dictionary

## DuckDB Support (Future)

The InLaw abstraction is designed to support DuckDB in the future:

- Works with any SQLAlchemy engine
- Config dictionary can specify connection type
- SQL dialect handling can be added as needed

Current implementation focuses on PostgreSQL, but the foundation is ready for DuckDB expansion.
