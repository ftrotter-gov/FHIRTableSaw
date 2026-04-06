# Data Validation with InLaw

FHIRTableSaw includes a data validation framework based on **InLaw** (built on Great Expectations) for validating FHIR data quality in PostgreSQL databases.

## Quick Start

### 1. Install Dependencies

```bash
pip install -e .
```

This installs Great Expectations and pandas along with other FHIRTableSaw dependencies.

### 2. Set Up Environment

Create a `.env` file with your database credentials:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/fhirdb
DB_SCHEMA=fhir_tablesaw
```

### 3. Run Validation Tests

```bash
# Run Practitioner validations
python dataexpectations/practitioner_expectations/run_expectations.py
```

## What Gets Validated

The Practitioner validation suite includes:

- **Row Count**: Ensures table has expected number of records
- **NPI Format**: Validates NPI values are exactly 10 digits
- **NPI Uniqueness**: Ensures no duplicate NPIs
- **Required Fields**: Validates resource_uuid and last_name are present

## Directory Structure

```
FHIRTableSaw/
├── src/
│   └── utils/
│       ├── dbtable.py          # Database table reference utility
│       └── inlaw.py            # Data validation framework
├── dataexpectations/
│   └── practitioner_expectations/
│       ├── run_expectations.py           # Runner script
│       ├── validate_row_count.py        # Row count validation
│       ├── validate_npi.py              # NPI validations
│       └── validate_required_fields.py  # Required field validations
└── AI_Instructions/
    ├── DBTable.md              # DBTable documentation
    └── InLaw.md                # InLaw documentation
```

## Creating Your Own Validations

### Step 1: Create a new validation class

```python
# dataexpectations/practitioner_expectations/validate_active_status.py
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable


class ValidateActiveStatus(InLaw):
    title = "Active field should be boolean"

    @staticmethod
    def run(engine, config=None):
        if config is None:
            return "SKIPPED: No config provided"

        # Build table reference
        practitioner_DBTable = DBTable(
            schema=config['schema'],
            table=config['practitioner_table']
        )

        # Query for invalid active values
        sql = f"""
            SELECT COUNT(*) AS invalid_active_count
            FROM {practitioner_DBTable}
            WHERE active NOT IN (true, false)
               OR active IS NULL
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)

        # Validate
        result = gx_df.expect_column_values_to_be_between(
            column="invalid_active_count",
            min_value=0,
            max_value=0
        )

        if result.success:
            return True
        return "Found practitioners with invalid active status"
```

### Step 2: Run the validation

The validation will automatically be discovered and run by `run_expectations.py` because it's in the same directory.

```bash
python dataexpectations/practitioner_expectations/run_expectations.py
```

## Creating Validation Suites for Other Resources

### Step 1: Create directory structure

```bash
mkdir -p dataexpectations/organization_expectations
touch dataexpectations/organization_expectations/__init__.py
```

### Step 2: Create runner script

```python
# dataexpectations/organization_expectations/run_expectations.py
#!/usr/bin/env python3
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.fhir_tablesaw_3tier.env import load_dotenv, get_db_url, get_db_schema
from src.fhir_tablesaw_3tier.db.engine import create_engine_with_schema
from src.utils.inlaw import InLaw


def main():
    load_dotenv()

    engine = create_engine_with_schema(
        db_url=get_db_url(),
        schema=get_db_schema()
    )

    config = {
        'schema': get_db_schema(),
        'organization_table': 'organizations',
        'min_expected_rows': 1,
        'max_expected_rows': 10000000,
    }

    results = InLaw.run_all(
        engine=engine,
        inlaw_dir=str(Path(__file__).parent),
        config=config
    )

    sys.exit(0 if results['failed'] == 0 and results['errors'] == 0 else 1)


if __name__ == "__main__":
    main()
```

### Step 3: Add validation tests

Create individual validation files in the directory (e.g., `validate_npi.py`, `validate_address.py`, etc.)

## Available Great Expectations Methods

Common validation methods you can use:

- `expect_column_values_to_be_between(column, min_value, max_value)` - Range validation
- `expect_column_values_to_be_unique(column)` - Uniqueness check
- `expect_column_values_to_not_be_null(column)` - Non-null check
- `expect_table_row_count_to_equal(value)` - Exact count
- `expect_table_row_count_to_be_between(min_value, max_value)` - Count range
- `expect_column_values_to_match_regex(column, regex)` - Pattern matching

See [AI_Instructions/InLaw.md](AI_Instructions/InLaw.md) for complete documentation.

## Best Practices

1. **One test per class**: Keep validation tests small and focused
2. **Use descriptive titles**: Make failures easy to understand
3. **Return clear error messages**: Include context about what failed
4. **Use config for thresholds**: Don't hardcode limits in test classes
5. **Write SQL for data**: Query the data you want to validate, not the violations
6. **Use DBTable**: Always use DBTable for schema-aware table references

## Skipping Tests

Set the `SKIP_TESTS` environment variable to skip all validations:

```bash
export SKIP_TESTS=1
python dataexpectations/practitioner_expectations/run_expectations.py
```

## Integration with CI/CD

Add validation tests to your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
- name: Run data validation
  run: |
    export DATABASE_URL=${{ secrets.DATABASE_URL }}
    export DB_SCHEMA=fhir_tablesaw
    python dataexpectations/practitioner_expectations/run_expectations.py

```

## Verifying that an NDJSON download is complete

If you have downloaded raw FHIR resources to `*.ndjson` files (for example using
`download_cms_ndjson.py` / `create_ndjson_from_api.py`), you can do a quick
integrity + completeness check against the source FHIR server.

If you want to download just a single resource type from the CMS API, the repo
root includes convenience runner scripts:

```bash
./runner_practitioner.sh
./runner_practitioner_role.sh
./runner_organization.sh
./runner_organization_affiliation.sh
./runner_endpoint.sh
./runner_location.sh
```

Each runner accepts an optional first argument `output_dir` (defaults to
`/Users/tgda/2026_03_31_cms_first_pass_ndjson/`), followed by any extra
`download_cms_ndjson.py` flags.

This answers:

* Are all NDJSON lines parseable JSON?
* Are there duplicate resource ids (often caused by paging issues)?
* Does `unique(id)` match what the server reports via `?_summary=count` (with fallbacks
  for CMS-like servers)?

Run:

```bash
# directory first, fhir base url second
python verify_fhir_download.py /path/to/download_dir https://dev.cnpd.internal.cms.gov/fhir/
```

The verifier retries each API "count" URL up to **6 times**. The first attempt uses
`--timeout` seconds (default: 120s), and after each failed attempt the timeout doubles.

By default it writes a CSV report to:

* `<download_dir>/verify_fhir_download_report.csv`

You can override that location with `--csv-out /path/to/report.csv`.

The CSV is intentionally minimal and contains exactly:

* `fhir_resource_type`
* `resource_id_count_from_file` (unique `resource.id` values found in the NDJSON file)
* `resource_id_count_from_url` (server-reported total via `?_summary=count` / fallback)

The script reads Basic Auth credentials from `.env` using:

* `FHIR_API_USERNAME`
* `FHIR_API_PASSWORD`

It exits non-zero on any mismatch.

## Troubleshooting

### Great Expectations Import Error

```bash
pip install great-expectations pandas
```

### Database Connection Error

Verify your `.env` file has correct credentials:

```bash
DATABASE_URL=postgresql://user:password@host:port/database
DB_SCHEMA=your_schema
```

### No Tests Found

Ensure:
- Test files are in the correct directory
- Test classes inherit from `InLaw`
- Test classes have a `run()` staticmethod
- Runner script points to correct directory

## Documentation

- [InLaw Framework Documentation](AI_Instructions/InLaw.md)
- [DBTable Utility Documentation](AI_Instructions/DBTable.md)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)

## Example Output

```
============================================================
Practitioner Validation Expectations
============================================================
Database: localhost:5432/fhirdb
Schema: fhir_tablesaw

===== IN-LAW TESTS =====
▶ Running: Practitioner table should have expected number of rows ✅ PASS
▶ Running: NPI values should be 10 digits when present ✅ PASS
▶ Running: NPI values should be unique (no duplicates) ✅ PASS
▶ Running: All practitioners should have a resource_uuid ✅ PASS
▶ Running: Practitioners should have last names ✅ PASS
============================================
Summary: 5 passed

============================================================
✅ All validation tests completed successfully!
   Passed: 5
   Failed: 0
   Errors: 0
============================================================
```
