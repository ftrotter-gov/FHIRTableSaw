# Palantir Import Fixes

## Issues Fixed

### 1. **64-bit Unsigned Integer Problem**

**Problem:** Pandas was reading CSV files with uint64 (unsigned 64-bit integer) columns, but PostgreSQL doesn't support unsigned 64-bit integers, causing the error:
```
ERROR: Unsigned 64 bit integer datatype is not supported
```

**Solution:** Modified `src/fhir_tablesaw_3tier/csv_uploader.py` to detect uint64 columns and convert them:
- If values fit within signed int64 range (≤ 9223372036854775807): Convert to Int64 (nullable signed integer)
- If values exceed int64 range: Convert to string

This happens automatically during the CSV upload process, before staging table creation.

### 2. **Schema Configuration Problem**

**Problem:** The `.env` file has separate schema settings for different data sources:
- `TEST_FHIR_SCHEMA=fhirtablesaw_test`
- `CMS_FHIR_SCHEMA=fhirtablesaw_cms`
- `PALANTIR_FHIR_SCHEMA=fhirtablesaw_palantir`

But `go_p.py` was setting `DB_SCHEMA` environment variable, and the CSV uploader was reading the schema at initialization time (before the override took effect), resulting in all imports going to the wrong schema (`fhirtablesaw` instead of `fhirtablesaw_palantir`).

**Solution:** 
- Modified `scripts/process_ndjson_fast.py` to explicitly read `DB_SCHEMA` from environment at upload time and pass it to the CSVPostgreSQLUploader constructor
- Added debug output to show which schema is being used during upload

### 3. **Intelligent CSV Reuse**

**Problem:** The pipeline would regenerate NDJSON→DuckDB→CSV even when CSV files already existed, wasting time on a process that takes over an hour.

**Solution:** Modified `go.py` to:
- Check if CSV files already exist before processing
- If CSV exists and no PostgreSQL upload is needed: Skip processing entirely
- If CSV exists and PostgreSQL upload IS needed: Skip stages 1 & 2 (NDJSON→DuckDB→CSV) and directly upload the existing CSV to PostgreSQL
- Only regenerate CSV files if they don't exist

This allows you to run `python go_p.py` immediately with existing CSV files and it will intelligently skip to the upload stage.

## Files Modified

1. `src/fhir_tablesaw_3tier/csv_uploader.py`
   - Added uint64 column detection and conversion logic
   - Converts problematic data types before PostgreSQL upload

2. `scripts/process_ndjson_fast.py`
   - Explicitly passes schema from environment to CSVPostgreSQLUploader
   - Added debug output showing schema being used

3. `go.py`
   - Added intelligent CSV file detection and reuse
   - Directly uploads existing CSV files when they exist
   - Skips stages 1 & 2 when CSV is already available

## Usage

To import existing Palantir CSV files into PostgreSQL:

```bash
# This will:
# 1. Skip NDJSON download (files already exist)
# 2. Skip DuckDB processing (CSV files already exist)
# 3. Directly upload existing CSV files to fhirtablesaw_palantir schema
python go_p.py
```

The script will automatically:
- Detect existing CSV files in `/Users/ftrotter/delete_me_cache/palantir/ndjson/initial/`
- Convert any uint64 columns to compatible PostgreSQL types
- Upload to the correct schema (`fhirtablesaw_palantir`)
- Show progress and statistics for each resource type

## Verification

You can verify the schema is correct by checking the output:
```
Using DB_SCHEMA: fhirtablesaw_palantir
  Target: fhirtablesaw_palantir.endpoint
```

After upload, verify in PostgreSQL:
```sql
-- List tables in the Palantir schema
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'fhirtablesaw_palantir';

-- Check row counts
SELECT 'endpoint' as table_name, COUNT(*) as rows FROM fhirtablesaw_palantir.endpoint
UNION ALL
SELECT 'location', COUNT(*) FROM fhirtablesaw_palantir.location
UNION ALL
SELECT 'organization', COUNT(*) FROM fhirtablesaw_palantir.organization;
```
