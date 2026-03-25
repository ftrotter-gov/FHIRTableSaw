# Data Expectations

This directory contains InLaw-based data validation tests organized by FHIR resource type.

## Structure

Each resource type has its own subdirectory containing:
- `run_expectations.py` - Runner script that executes all tests in the directory
- Individual validation test files (e.g., `validate_*.py`)

## Current Validation Suites

### Practitioner Expectations

Located in `practitioner_expectations/`

Validates:
- Row count is within expected range
- NPI values are properly formatted (10 digits)
- NPI values are unique (no duplicates)
- Required fields are present (resource_uuid, last_name)

**Run with:**
```bash
python dataexpectations/practitioner_expectations/run_expectations.py
```

## Creating New Validation Suites

1. Create a new directory for the resource type:
   ```bash
   mkdir -p dataexpectations/resource_name_expectations
   touch dataexpectations/resource_name_expectations/__init__.py
   ```

2. Copy and adapt the runner script from `practitioner_expectations/run_expectations.py`

3. Create validation test files following the InLaw pattern

4. Run your validation suite:
   ```bash
   python dataexpectations/resource_name_expectations/run_expectations.py
   ```

## Writing Validation Tests

Each validation test should:
- Inherit from `InLaw`
- Define a descriptive `title` attribute
- Implement a `run(engine, config)` staticmethod
- Return `True` for success or an error message string for failure

Example:
```python
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable

class ValidateMyData(InLaw):
    title = "My data should meet criteria"
    
    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"
        
        my_table = DBTable(
            schema=config['schema'],
            table=config['my_table']
        )
        
        sql = f"SELECT COUNT(*) AS count FROM {my_table} WHERE invalid_condition"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        result = gx_df.expect_column_values_to_be_between(
            column="count",
            min_value=0,
            max_value=0
        )
        
        return True if result.success else "Validation failed"
```

## Documentation

- See [../DATA_VALIDATION.md](../DATA_VALIDATION.md) for quick start guide
- See [../AI_Instructions/InLaw.md](../AI_Instructions/InLaw.md) for detailed InLaw documentation
- See [../AI_Instructions/DBTable.md](../AI_Instructions/DBTable.md) for DBTable documentation
