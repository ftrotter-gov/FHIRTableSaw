# DuckDB Query Guide

## Overview

The fast pipeline now creates persistent DuckDB databases that can be queried directly for analysis. These databases contain the full FHIR resources loaded by fhir4ds.

## DuckDB Files Created

When you run the fast pipeline, DuckDB databases are created alongside the NDJSON files:

```
/Volumes/eBolt/palantir/ndjson/initial/
├── Practitioner.ndjson          # Source FHIR data
├── Practitioner.duckdb          # DuckDB database (for querying)
├── Practitioner_practitioner.csv # Flattened CSV output
├── Organization.ndjson
├── Organization.duckdb
├── Organization_organization.csv
... (etc for each resource type)
```

## Querying DuckDB Files

### Option 1: Using DuckDB CLI

```bash
# Install DuckDB CLI if needed
brew install duckdb

# Query a DuckDB file
duckdb /Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb

# Once in DuckDB CLI:
D .tables                          # List tables
D DESCRIBE resources;              # Show schema
D SELECT COUNT(*) FROM resources;  # Count resources
D SELECT * FROM resources LIMIT 5; # View sample data
```

### Option 2: Using Python

```python
import duckdb

# Connect to DuckDB database
conn = duckdb.connect('/Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb')

# Query resources
result = conn.execute("SELECT COUNT(*) FROM resources").fetchone()
print(f"Total resources: {result[0]}")

# Query specific fields from FHIR JSON
query = """
SELECT
    data->>'$.id' as id,
    data->>'$.name[0].family' as family_name,
    data->>'$.name[0].given[0]' as given_name
FROM resources
LIMIT 10
"""
df = conn.execute(query).df()
print(df)

conn.close()
```

### Option 3: Using fhir4ds ViewDefinitions

```python
from fhir4ds import DuckDBDialect, FHIRDataStore
import duckdb

# Connect to existing DuckDB database
conn = duckdb.connect('/Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb')

# Create fhir4ds datastore
dialect = DuckDBDialect()
dialect.connection = conn
datastore = FHIRDataStore(dialect=dialect, initialize_table=False)

# Load and execute ViewDefinition
viewdef_path = 'viewdefs/practitioner.json'
result = datastore.execute_viewdef(viewdef_path)

# Get results as DataFrame
df = result.to_dataframe()
print(df.head())
```

## Common Queries

### Count Resources

```sql
SELECT COUNT(*) as total_resources FROM resources;
```

### View Raw FHIR JSON

```sql
SELECT data FROM resources LIMIT 1;
```

### Extract Specific Fields

```sql
-- Practitioner names
SELECT
    data->>'$.id' as id,
    json_extract_string(data, '$.name[0].family') as family_name,
    json_extract_string(data, '$.name[0].given[0]') as given_name,
    json_extract_string(data, '$.name[0].prefix[0]') as prefix
FROM resources
LIMIT 10;

-- Organization names and types
SELECT
    data->>'$.id' as id,
    json_extract_string(data, '$.name') as org_name,
    json_extract_string(data, '$.type[0].coding[0].display') as org_type
FROM resources
LIMIT 10;
```

### Join Multiple DuckDB Files

```sql
-- Attach multiple databases
ATTACH '/Volumes/eBolt/palantir/ndjson/initial/Organization.duckdb' AS org_db;
ATTACH '/Volumes/eBolt/palantir/ndjson/initial/Location.duckdb' AS loc_db;

-- Query across databases
SELECT
    json_extract_string(org_db.resources.data, '$.name') as org_name,
    json_extract_string(loc_db.resources.data, '$.address.city') as city
FROM org_db.resources
JOIN loc_db.resources ON
    json_extract_string(org_db.resources.data, '$.id') =
    json_extract_string(loc_db.resources.data, '$.managingOrganization.reference');
```

## Advanced: Custom Analysis Scripts

### Analyze Practitioner Specialties

```python
import duckdb

conn = duckdb.connect('/Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb')

query = """
SELECT
    json_extract_string(data, '$.qualification[0].code.coding[0].display') as specialty,
    COUNT(*) as count
FROM resources
WHERE json_extract_string(data, '$.qualification[0].code.coding[0].display') IS NOT NULL
GROUP BY specialty
ORDER BY count DESC
LIMIT 20
"""

df = conn.execute(query).df()
print(df)
```

### Export Custom Views

```python
import duckdb

conn = duckdb.connect('/Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb')

# Create custom view
conn.execute("""
    COPY (
        SELECT
            json_extract_string(data, '$.id') as practitioner_id,
            json_extract_string(data, '$.name[0].family') as last_name,
            json_extract_string(data, '$.name[0].given[0]') as first_name,
            json_extract_string(data, '$.gender') as gender
        FROM resources
    ) TO '/tmp/practitioners_simple.csv' (HEADER, DELIMITER ',')
""")
```

## Performance Tips

1. **DuckDB is fast** - Queries on millions of rows complete in seconds
2. **Use JSON extraction wisely** - DuckDB has excellent JSON support
3. **Create indexes** if needed for repeated queries:
   ```sql
   CREATE INDEX idx_id ON resources(json_extract_string(data, '$.id'));
   ```
4. **Export to Parquet** for even faster analysis:
   ```sql
   COPY resources TO 'practitioners.parquet' (FORMAT PARQUET);
   ```

## Comparing DuckDB vs PostgreSQL

| Feature | DuckDB | PostgreSQL |
|---------|--------|------------|
| **Query Speed** | Very fast (analytical) | Fast (transactional) |
| **Setup** | Just a file | Requires server |
| **Memory** | Efficient columnar | Row-based |
| **Use Case** | Analysis, reporting | Production apps |
| **File Size** | ~500KB - 5GB | Full server |

**When to use DuckDB:**
- Ad-hoc analysis
- Exploring data
- Quick queries
- No server needed

**When to use PostgreSQL:**
- Production applications
- Concurrent writes
- Complex transactions
- Multi-user access

## File Sizes

Typical DuckDB database sizes:

| Resource | NDJSON Size | DuckDB Size | Compression |
|----------|-------------|-------------|-------------|
| Practitioner | 12 GB | ~2-3 GB | ~75% |
| Organization | 3 GB | ~600 MB | ~80% |
| Location | 8 GB | ~1.5 GB | ~80% |

DuckDB databases are much smaller due to columnar compression.

## Backup and Distribution

DuckDB files can be:
- ✅ Copied to other machines
- ✅ Backed up like any file
- ✅ Shared with collaborators
- ✅ Queried without installation
- ✅ Used on laptops/desktops

## Troubleshooting

### "Database is locked"
Only one write connection at a time. Close other connections.

### "Out of memory"
DuckDB automatically spills to disk. Check temp directory has space.

### "Cannot open database"
Ensure the file exists and you have read permissions.

## Next Steps

1. **Explore your data:**
   ```bash
   duckdb /Volumes/eBolt/palantir/ndjson/initial/Practitioner.duckdb
   ```

2. **Run ViewDefinitions:**
   Use fhir4ds to execute ViewDefinitions on the DuckDB files

3. **Build dashboards:**
   Connect tools like Tableau, Power BI, or Python notebooks

4. **Create custom exports:**
   Extract exactly what you need in any format

## Summary

DuckDB databases provide:
- ✅ Fast local queries (no server needed)
- ✅ Full FHIR resource access
- ✅ Excellent compression (~75% smaller)
- ✅ SQL and JSON query support
- ✅ Easy to share and backup

Perfect for analysis, exploration, and reporting on FHIR data!
