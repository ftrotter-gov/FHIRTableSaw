# Quick Start: SQL on FHIR with fhir4ds

## Setup (5 minutes)

### 1. Configure Database

Copy the example environment file and update with your PostgreSQL credentials:

```bash
cp env.example .env
```

Edit `.env` with your database settings:

```bash
# Your PostgreSQL connection
DATABASE_URL=postgresql+psycopg://YOUR_USER:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE

# Schema to use (default: fhir_tablesaw)
DB_SCHEMA=fhir_tablesaw
```

**Example:**
```bash
DATABASE_URL=postgresql+psycopg://postgres:mypassword@localhost:5432/ndh
DB_SCHEMA=fhir_tablesaw
```

### 2. Install Dependencies

Activate your virtual environment and install:

```bash
source venv/bin/activate
pip install -e .
```

This installs `fhir4ds` and all other dependencies.

### 3. Verify Installation

```bash
python -c "import fhir4ds; print('fhir4ds installed successfully!')"
```

### 4. Prepare Your Data

Your FHIR data must be in **NDJSON** format (newline-delimited JSON):

```bash
# Each line is a complete FHIR Practitioner resource
{"resourceType": "Practitioner", "id": "123", "name": [...], ...}
{"resourceType": "Practitioner", "id": "456", "name": [...], ...}
{"resourceType": "Practitioner", "id": "789", "name": [...], ...}
```

**NOT** a JSON array:
```json
[
  {"resourceType": "Practitioner", ...},
  {"resourceType": "Practitioner", ...}
]
```

### 5. Load Data

```bash
python scripts/load_practitioner_ndjson.py path/to/your/practitioners.ndjson
```

**Example output:**
```
Loading Practitioner data from: data/practitioners.ndjson

============================================================
PROCESSING COMPLETE
============================================================
Status: success
Total resources in file: 150
Matching Practitioners: 150
Resource type: Practitioner
Table name: practitioner
Mode: append
============================================================

✓ Data successfully loaded to PostgreSQL!
```

### 6. Verify in Database

```bash
psql -d YOUR_DATABASE -c "SELECT COUNT(*) FROM fhir_tablesaw.practitioner;"
```

Or:

```sql
SELECT
    resource_uuid,
    npi,
    first_name,
    last_name
FROM fhir_tablesaw.practitioner
LIMIT 10;
```

## Usage Patterns

### Load Practitioners

```bash
python scripts/load_practitioner_ndjson.py data/practitioners.ndjson
```

### Replace vs Append

**Append mode (default):** Adds new rows to existing table
```python
process_practitioner_ndjson(
    ndjson_path="data.ndjson",
    if_exists="append"
)
```

**Replace mode:** Drops and recreates table
```python
process_practitioner_ndjson(
    ndjson_path="data.ndjson",
    if_exists="replace"
)
```

### Programmatic Usage

```python
from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson
from fhir_tablesaw_3tier.env import load_dotenv

# Load .env file
load_dotenv()

# Process NDJSON
result = process_practitioner_ndjson(
    ndjson_path="practitioners.ndjson"
)

print(f"Loaded {result['matching_resources']} practitioners")
```

## Environment Variables

Your `.env` file should contain:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `DATABASE_URL` | ✅ Yes | PostgreSQL connection string | `postgresql+psycopg://user:pass@localhost:5432/dbname` |
| `DB_SCHEMA` | ⚠️ Optional | Schema name (default: `fhir_tablesaw`) | `fhir_tablesaw` |

## Troubleshooting

### "DATABASE_URL not set"

```bash
# Check if .env exists
ls -la .env

# If missing, copy from example
cp env.example .env

# Edit with your credentials
nano .env  # or vim, code, etc.
```

### "fhir4ds not found"

```bash
# Ensure venv is activated
source venv/bin/activate

# Reinstall
pip install -e .

# Verify
python -c "import fhir4ds; print('OK')"
```

### "Invalid JSON on line X"

Your NDJSON file has a syntax error. Each line must be valid JSON:

```bash
# Check specific line
sed -n 'Xp' your_file.ndjson | jq .

# Validate entire file
while IFS= read -r line; do echo "$line" | jq . > /dev/null || echo "Error on line"; done < your_file.ndjson
```

### "No Practitioner resources found"

Your NDJSON might contain other resource types. Check:

```bash
# See what resource types are in the file
grep -o '"resourceType":"[^"]*"' your_file.ndjson | sort | uniq -c
```

## Next Steps

- Review the ViewDefinition: `cat viewdefs/practitioner.json`
- Read full documentation: `README_SQL_ON_FHIR.md`
- Check implementation details: `IMPLEMENTATION_SUMMARY.md`

## Converting Data to NDJSON

If your data is in a different format:

### From JSON Array

```python
import json

# Load JSON array
with open('practitioners.json') as f:
    resources = json.load(f)

# Write as NDJSON
with open('practitioners.ndjson', 'w') as f:
    for resource in resources:
        f.write(json.dumps(resource) + '\n')
```

### From FHIR Bundle

```python
import json

with open('bundle.json') as f:
    bundle = json.load(f)

with open('practitioners.ndjson', 'w') as f:
    for entry in bundle.get('entry', []):
        resource = entry.get('resource', {})
        if resource.get('resourceType') == 'Practitioner':
            f.write(json.dumps(resource) + '\n')
```

### From FHIR API

```bash
# Using curl and jq
curl 'http://fhir-server/Practitioner?_count=1000' | \
  jq -c '.entry[].resource' > practitioners.ndjson
```

## Help

Still stuck? Check:
1. Environment variables: `cat .env`
2. Database connection: `psql -d YOUR_DATABASE -c '\dt fhir_tablesaw.*'`
3. ViewDefinition syntax: `cat viewdefs/practitioner.json | jq .`
4. Example script: `cat scripts/load_practitioner_ndjson.py`
