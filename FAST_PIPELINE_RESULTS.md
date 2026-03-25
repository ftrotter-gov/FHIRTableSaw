# Fast Pipeline Performance Results

## New Architecture: DuckDB → CSV → PostgreSQL

The new fast pipeline uses local DuckDB processing instead of remote PostgreSQL for fhir4ds operations, dramatically improving performance.

## Architecture Comparison

### Old Approach
```
NDJSON → fhir4ds → Remote PostgreSQL (row-by-row) → ViewDefinition → PostgreSQL
```
**Bottleneck**: Remote database operations for every resource

### New Approach
```
NDJSON → fhir4ds → Local DuckDB (in-memory) → ViewDefinition → CSV → Bulk PostgreSQL
```
**Advantage**: All heavy processing done locally, then bulk upload

## Performance Results

### Benchmark Data (Production Practitioner.ndjson file)

| Rows | Old Method | New Method | Speedup |
|------|-----------|------------|---------|
| 1,000 | ~11 sec | **1.77 sec** | **6.2x** |
| 10,000 | ~90 sec | **3.23 sec** | **27.8x** |
| 50,000 | ~7.5 min | **14.38 sec** | **31.3x** |

### Throughput Comparison

| Method | Rows/Second | Notes |
|--------|-------------|-------|
| **Old (Remote PostgreSQL)** | ~111 rows/sec | Limited by network + database |
| **New (Local DuckDB)** | **~3,477 rows/sec** | Local processing, no network |
| **Improvement** | **31.3x faster** | Same fhir4ds, different backend |

## Projected Performance for Full Dataset

Based on measured 3,096 rows/second:

| Dataset | Rows | Old Time | New Time | Speedup |
|---------|------|----------|----------|---------|
| Practitioner | 7.1M | 17.8 hours | **38 minutes** | **28x** |
| Future (2x) | 14M | 35.6 hours | **75 minutes** | **28x** |

## Why This Works

### The Real Bottleneck Was Database I/O

The old approach had three slow operations:
1. **fhir4ds loading**: Inserting 7.1M resources one-by-one to remote PostgreSQL
2. **ViewDefinition execution**: SQL queries on remote database
3. **Final writes**: Row-by-row UPSERTs

### The New Solution

1. **fhir4ds loading**: Insert to local DuckDB (in-memory) - **NO NETWORK LATENCY**
2. **ViewDefinition execution**: Query local database - **INSTANT**
3. **CSV export**: Write to disk - **FAST**
4. **PostgreSQL upload**: Bulk insert via staging table - **ALREADY OPTIMIZED**

## File Locations (Configurable)

Default behavior (all files in same directory):

```
/Volumes/eBolt/palantir/ndjson/initial/
├── Practitioner.ndjson                  # Source (12 GB)
└── Practitioner_practitioner.csv        # Output (~1-2 GB)
```

## Usage Examples

### Basic Usage (CSV only)
```bash
python scripts/process_ndjson_fast.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --viewdef viewdefs/practitioner.json
```
Creates: `Practitioner_practitioner.csv` in same directory

### With PostgreSQL Upload
```bash
python scripts/process_ndjson_fast.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --viewdef viewdefs/practitioner.json \
    --upload
```
Creates CSV and uploads to PostgreSQL table `practitioner`

### Custom Paths
```bash
python scripts/process_ndjson_fast.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --viewdef viewdefs/practitioner.json \
    --csv-path /output/directory/my_practitioners.csv \
    --upload --table practitioners_table
```

### Testing with Limit
```bash
python scripts/process_ndjson_fast.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --viewdef viewdefs/practitioner.json \
    --limit 100000 \
    --upload
```

## Features

✅ **27.8x faster** than previous approach
✅ **Configurable paths** for CSV output
✅ **No DuckDB persistence needed** (in-memory only)
✅ **Keeps fhir4ds ViewDefinitions** (maintainability)
✅ **Optional PostgreSQL upload** (bulk optimized)
✅ **Testing support** with --limit flag
✅ **Flexible** - can run locally without database

## Command-Line Interface

```
python scripts/process_ndjson_fast.py <ndjson_file> --viewdef <viewdef_file> [OPTIONS]

Required:
  ndjson_file              Path to NDJSON file
  --viewdef PATH           Path to ViewDefinition JSON file

Path Configuration:
  --csv-path PATH          Output CSV path (default: same dir as NDJSON)

Processing Options:
  --batch-size INT         Resources per batch (default: 5000)
  --limit INT              Maximum resources to process (for testing)

Control Flags:
  --force-overwrite        Overwrite existing CSV
  --upload                 Upload CSV to PostgreSQL
  --table NAME             PostgreSQL table name (default: from viewdef)
  --upload-mode MODE       replace|append|fail (default: replace)
```

## Modules Created

1. **`src/fhir_tablesaw_3tier/duckdb_loader.py`**
   - Loads NDJSON to local DuckDB using fhir4ds
   - Returns datastore for immediate use

2. **`src/fhir_tablesaw_3tier/csv_exporter.py`**
   - Executes ViewDefinitions on datastore
   - Exports results to CSV files

3. **`src/fhir_tablesaw_3tier/csv_uploader.py`**
   - Bulk uploads CSV to PostgreSQL
   - Uses staging table optimization

4. **`scripts/process_ndjson_fast.py`**
   - Unified pipeline script
   - Ties all modules together

## Next Steps

### For Production Use

1. **Process all resource types:**
```bash
for resource in Practitioner Organization Location Endpoint PractitionerRole OrganizationAffiliation; do
    python scripts/process_ndjson_fast.py \
        /Volumes/eBolt/palantir/ndjson/initial/${resource}.ndjson \
        --viewdef viewdefs/$(echo $resource | tr '[:upper:]' '[:lower:]').json \
        --upload
done
```

2. **Expected total time:** ~3-4 hours for all resources (vs. days before!)

### For Further Optimization

If 38 minutes isn't fast enough, consider:
- **Parallel processing**: Process multiple files simultaneously (4-8x speedup)
- **Larger batches**: Increase --batch-size for faster loading
- **SSD storage**: Use fast local disk for temporary files

## Summary

The new fast pipeline achieves **27.8x speedup** by:
1. Using local DuckDB instead of remote PostgreSQL for fhir4ds
2. Eliminating network latency during resource loading
3. Bulk uploading final results to PostgreSQL

**Result: 7.1M rows in 38 minutes instead of 18 hours!**

This makes daily/weekly processing of large FHIR datasets practical.
