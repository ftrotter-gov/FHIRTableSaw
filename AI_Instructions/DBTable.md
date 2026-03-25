# DBTable - Database Table References

DBTable creates reusable table references that work across different schemas, providing a clean way to reference tables in SQL statements.

## Basic Usage

```python
from src.utils.dbtable import DBTable

# Define table with schema (PostgreSQL style)
# Always name the variables with something_DBTable at the end
practitioner_DBTable = DBTable(schema='fhir_tablesaw', table='practitioners')
organization_DBTable = DBTable(schema='fhir_tablesaw', table='organizations')

# For similar table names, use the make_child or create_child syntax
practitioner_telecom_DBTable = practitioner_DBTable.create_child('telecom')
practitioner_address_DBTable = practitioner_DBTable.create_child('addresses')

# Use in SQL via f-strings
sql = f"SELECT * FROM {practitioner_DBTable} WHERE npi IS NOT NULL"

# Also works for child tables
sql = f"SELECT * FROM {practitioner_telecom_DBTable} WHERE type = 'phone'"
```

## FHIRTableSaw Integration

DBTable works seamlessly with FHIRTableSaw's schema-aware PostgreSQL connections:

```python
from src.fhir_tablesaw_3tier.env import load_dotenv, get_db_schema
from src.utils.dbtable import DBTable

# Load environment
load_dotenv()
schema = get_db_schema()  # Gets DB_SCHEMA from .env

# Create table reference
practitioners_DBTable = DBTable(schema=schema, table='practitioners')

# Use in queries
query = f"""
    SELECT resource_uuid, npi, first_name, last_name
    FROM {practitioners_DBTable}
    WHERE active = true
"""
```

## Key Features

- **Clean table reference management** across schemas
- **Consistent naming convention** (always end with `_DBTable`)
- **Child table creation** for related tables
- **Works seamlessly with f-string SQL** formatting
- **Prevents hardcoded schema/table names** in SQL
- **Multi-level hierarchy support** (catalog, database, schema, table)

## Naming Convention

Always name your DBTable variables with `_DBTable` at the end to maintain consistency throughout your codebase:

```python
# Good
practitioner_DBTable = DBTable(schema='public', table='practitioners')
location_DBTable = DBTable(schema='public', table='locations')

# Avoid
pract_table = DBTable(schema='public', table='practitioners')
loc = DBTable(schema='public', table='locations')
```

## Child Table Pattern

Use `create_child()` or `make_child()` to create related table references:

```python
# Parent table
practitioner_DBTable = DBTable(schema='public', table='practitioners')

# Child tables for normalized repeating patterns
telecom_DBTable = practitioner_DBTable.create_child('telecom')
address_DBTable = practitioner_DBTable.create_child('addresses')
identifier_DBTable = practitioner_DBTable.create_child('identifiers')

# Results in: public.practitioners_telecom, public.practitioners_addresses, etc.
```

## Advanced: Multiple Hierarchy Levels

DBTable supports various database hierarchy patterns:

```python
# PostgreSQL (3 levels: database.schema.table)
table = DBTable(database='fhirdb', schema='public', table='practitioners')
# Results in: fhirdb.public.practitioners

# Simple schema.table (most common in FHIRTableSaw)
table = DBTable(schema='fhir_tablesaw', table='practitioners')
# Results in: fhir_tablesaw.practitioners

# With catalog (for systems like Databricks)
table = DBTable(catalog='main', database='fhirdb', table='practitioners')
# Results in: main.fhirdb.practitioners
```

## Parameter Aliases

DBTable accepts various parameter aliases for flexibility:

```python
# These are equivalent
DBTable(schema='public', table='users')
DBTable(schema_name='public', table_name='users')

# Database aliases
DBTable(database='mydb', schema='public', table='users')
DBTable(db='mydb', schema='public', table='users')
```

## Validation Rules

DBTable validates all names to ensure they're valid SQL identifiers:

- Must start with a letter
- Can contain letters, numbers, underscores, and dashes
- Maximum length of 60 characters
- At least 2 hierarchy levels required (e.g., schema + table)

```python
# Valid
DBTable(schema='my_schema', table='my_table')
DBTable(schema='schema123', table='table-456')

# Invalid - will raise DBTableValidationError
DBTable(schema='123invalid', table='users')  # Starts with number
DBTable(schema='my@schema', table='users')   # Invalid character
DBTable(schema='a' * 61, table='users')      # Too long
```

## Usage with InLaw Validation Tests

DBTable is designed to work seamlessly with InLaw data validation:

```python
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable

class ValidateRowCount(InLaw):
    title = "Practitioner table should have expected rows"
    
    @staticmethod
    def run(engine, config=None):
        # Use config to get schema name
        schema = config.get('schema', 'public')
        
        # Create table reference
        practitioner_DBTable = DBTable(
            schema=schema,
            table='practitioners'
        )
        
        # Use in SQL
        sql = f"SELECT COUNT(*) AS row_count FROM {practitioner_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Run validation
        result = gx_df.expect_column_values_to_be_between(
            column="row_count",
            min_value=1,
            max_value=10000000
        )
        
        return True if result.success else "Row count validation failed"
```

## Error Handling

DBTable provides specific exceptions for different error types:

```python
from src.utils.dbtable import (
    DBTable,
    DBTableError,              # Base exception
    DBTableValidationError,    # Invalid names/parameters
    DBTableHierarchyError      # Hierarchy requirement violations
)

try:
    table = DBTable(table='users')  # Missing second level
except DBTableHierarchyError as e:
    print(f"Hierarchy error: {e}")

try:
    table = DBTable(schema='123bad', table='users')  # Invalid name
except DBTableValidationError as e:
    print(f"Validation error: {e}")
```

## Best Practices

1. **Always use named parameters**: Makes code more readable
2. **Use consistent naming**: End variables with `_DBTable`
3. **Leverage child tables**: For related/normalized tables
4. **Store in config**: Pass schema names via config dictionaries
5. **Validate early**: Create DBTable objects early to catch errors

```python
# Good practice
config = {
    'schema': get_db_schema(),
    'practitioner_table': 'practitioners'
}

practitioner_DBTable = DBTable(
    schema=config['schema'],
    table=config['practitioner_table']
)

# Use throughout your code
sql1 = f"SELECT COUNT(*) FROM {practitioner_DBTable}"
sql2 = f"SELECT * FROM {practitioner_DBTable} WHERE active = true"
```
