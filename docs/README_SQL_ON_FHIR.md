# SQL on FHIR Implementation Guide

## Overview

This project now supports **SQL on FHIR v2.0**, a standardized approach for flattening FHIR resources into tabular format using declarative **ViewDefinitions**.

### What Changed?

**Before (Custom Python Flattening):**
```python
# Custom 300+ line parsing function
practitioner, report = practitioner_from_fhir_json(raw_json)
save_practitioner(session, practitioner)
```

**After (SQL on FHIR with fhir4ds):**
```python
# Declarative ViewDefinition + library handles all parsing
from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson

result = process_practitioner_ndjson(ndjson_path="data/practitioners.ndjson")
```

## Installation

### 1. Activate Your Virtual Environment

```bash
source venv/bin/activate  # Or your venv activation command
```

### 2. Install Dependencies

```bash
pip install -e .
```

This will install `fhir4ds` and all other dependencies from `pyproject.toml`.

### 3. Configure Environment

Create a `.env` file with your PostgreSQL connection:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/fhir_db
DB_SCHEMA=fhir_tablesaw
```

## Usage

### Loading Practitioner Data from NDJSON

```bash
python scripts/load_practitioner_ndjson.py data/practitioners.ndjson
```

### Programmatic Usage

```python
from fhir_tablesaw_3tier.fhir4ds_integration import FHIR4DSRunner

# Initialize with ViewDefinition
runner = FHIR4DSRunner(viewdef_path="viewdefs/practitioner.json")

# Process NDJSON file
result = runner.process_ndjson(
    ndjson_path="data/practitioners.ndjson",
    if_exists="append"  # or "replace" to drop/recreate table
)

print(f"Loaded {result['matching_resources']} practitioners")
```

### Using Different ViewDefinitions

```python
from fhir_tablesaw_3tier.fhir4ds_integration import FHIR4DSRunner

# Load Endpoints
endpoint_runner = FHIR4DSRunner(viewdef_path="viewdefs/endpoint.json")
endpoint_runner.process_ndjson(ndjson_path="data/endpoints.ndjson")

# Load Locations
location_runner = FHIR4DSRunner(viewdef_path="viewdefs/location.json")
location_runner.process_ndjson(ndjson_path="data/locations.ndjson")
```

## ViewDefinitions

ViewDefinitions are JSON files that define how to flatten FHIR resources using **FHIRPath expressions**.

### Example: Practitioner ViewDefinition

```json
{
  "resourceType": "ViewDefinition",
  "name": "practitioner",
  "resource": "Practitioner",
  "select": [{
    "column": [
      {
        "name": "resource_uuid",
        "path": "id"
      },
      {
        "name": "npi",
        "path": "identifier.where(system='http://hl7.org/fhir/sid/us-npi').value.first()"
      },
      {
        "name": "first_name",
        "path": "name[0].given[0]"
      },
      {
        "name": "last_name",
        "path": "name[0].family"
      }
    ]
  }]
}
```

See `viewdefs/practitioner.json` for the complete definition.

### FHIRPath Expressions

FHIRPath is a query language for navigating FHIR resources:

| FHIRPath | Description | Example Output |
|----------|-------------|----------------|
| `id` | Resource ID | `"abc-123"` |
| `name[0].given[0]` | First given name | `"John"` |
| `name[0].family` | Family name | `"Doe"` |
| `identifier.where(system='...')` | Filter by system | Filtered identifiers |
| `.value.first()` | Get first value | `"1234567890"` |
| `extension.where(url='...').valueBoolean` | Extract extension | `true` |

Full FHIRPath documentation: https://hl7.org/fhirpath/

## Architecture

### Current Three-Layer Approach

```
┌─────────────────────────────────────────────────────────┐
│                    FHIR JSON (Input)                    │
└─────────────────────┬───────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
         ▼                         ▼
┌─────────────────┐    ┌──────────────────────┐
│  SQL on FHIR    │    │  Custom Python       │
│  (fhir4ds)      │    │  (Legacy - for       │
│                 │    │   FHIR generation)   │
│  ViewDefinition │    │                      │
│  + FHIRPath     │    │  *_to_fhir_json()    │
└────────┬────────┘    └──────────┬───────────┘
         │                        │
         ▼                        ▼
┌─────────────────────────────────────────────────────────┐
│              PostgreSQL (Relational Tables)             │
└─────────────────────────────────────────────────────────┘
```

### What We Keep vs. Replace

#### ✅ KEEP (For DB → FHIR Round-Trip)
- `src/fhir_tablesaw_3tier/domain/*.py` - Canonical models
- `*_to_fhir_json()` functions - FHIR generation
- PostgreSQL schema/tables
- SQLAlchemy models for reading
- Tests (updated)

#### ❌ REPLACE (With ViewDefinitions)
- `src/fhir_tablesaw_3tier/fhir/*_from_fhir_json()` - Parsing functions
- `src/fhir_tablesaw_3tier/db/persist_*.py` - Persistence logic
- Custom flattening code (~2000 lines)

#### ✨ NEW
- `viewdefs/*.json` - SQL on FHIR ViewDefinitions
- `src/fhir_tablesaw_3tier/fhir4ds_integration.py` - Integration wrapper
- `scripts/load_*_ndjson.py` - Loading scripts

## Benefits of SQL on FHIR

### 1. **Standardization**
- Industry-standard approach (HL7 specification)
- Portable across systems and platforms
- 100% SQL on FHIR v2.0 compliant

### 2. **Maintainability**
- Declarative FHIRPath vs. imperative Python
- Self-documenting ViewDefinitions
- Easier to understand and modify

### 3. **Reduced Code**
- **Before:** ~2000 lines of custom parsing
- **After:** ~500 lines of ViewDefinitions + ~200 lines wrapper

### 4. **Reusability**
- ViewDefinitions can be shared across organizations
- NDH could publish standard ViewDefinitions
- Community contributions

### 5. **Future-Proof**
- As FHIR evolves, update ViewDefinitions not code
- Library (fhir4ds) handles FHIR changes

## Current Limitations & Next Steps

### Junction Tables

The current ViewDefinition handles the main `practitioner` table. Repeating elements (addresses, telecoms, credentials) need additional handling:

**Options:**
1. **Nested ViewDefinitions** - fhir4ds supports nested selects
2. **Post-processing** - Extract arrays after main table load
3. **Separate ViewDefinitions** - One per junction table

### To Be Implemented

- [ ] ViewDefinitions for junction tables (addresses, telecoms, etc.)
- [ ] ViewDefinitions for other resources (Endpoint, Location, etc.)
- [ ] Update existing tests to use fhir4ds
- [ ] CLI integration
- [ ] Performance benchmarking

## Migration Guide

### For Developers

1. **Install fhir4ds:**
   ```bash
   pip install -e .
   ```

2. **Test with sample data:**
   ```bash
   python scripts/load_practitioner_ndjson.py test_data.ndjson
   ```

3. **Review ViewDefinition:**
   - Open `viewdefs/practitioner.json`
   - Compare FHIRPath expressions to old Python logic

4. **Verify database output:**
   ```sql
   SELECT * FROM practitioner LIMIT 10;
   ```

### For Existing Code

**Old approach (still works):**
```python
from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json
from fhir_tablesaw_3tier.db.persist_practitioner import save_practitioner

practitioner, report = practitioner_from_fhir_json(fhir_json)
save_practitioner(session, practitioner)
```

**New approach (recommended):**
```python
from fhir_tablesaw_3tier.fhir4ds_integration import process_practitioner_ndjson

result = process_practitioner_ndjson(ndjson_path="data.ndjson")
```

Both approaches write to the same PostgreSQL tables.

## Resources

- **SQL on FHIR v2.0 Specification:** https://sql-on-fhir.org/
- **fhir4ds Documentation:** https://github.com/joelmontavon/fhir4ds
- **FHIRPath Specification:** https://hl7.org/fhirpath/
- **ViewDefinition Examples:** `viewdefs/` directory

## Troubleshooting

### fhir4ds Not Found

```bash
# Make sure you're in the venv
source venv/bin/activate

# Reinstall with editable mode
pip install -e .
```

### DATABASE_URL Not Set

```bash
# Create .env file
echo 'DATABASE_URL=postgresql://user:pass@localhost/db' > .env
```

### NDJSON Format Issues

NDJSON requires one JSON object per line:
```json
{"resourceType": "Practitioner", "id": "1", ...}
{"resourceType": "Practitioner", "id": "2", ...}
```

NOT a JSON array:
```json
[
  {"resourceType": "Practitioner", ...},
  {"resourceType": "Practitioner", ...}
]
```

## Support

For questions or issues:
1. Check existing tests: `tests/test_practitioner_3tier.py`
2. Review ViewDefinition: `viewdefs/practitioner.json`
3. Check fhir4ds documentation
4. Open an issue with sample NDJSON data
