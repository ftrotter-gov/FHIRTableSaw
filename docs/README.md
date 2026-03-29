# FHIRTableSaw Documentation

This directory contains detailed documentation for various aspects of the FHIRTableSaw project.

## Quick Start Guides

- **[QUICKSTART_SQL_ON_FHIR.md](QUICKSTART_SQL_ON_FHIR.md)** - Get started with SQL-on-FHIR ViewDefinitions
- **[QUICKSTART_LOAD_RESOURCES.md](QUICKSTART_LOAD_RESOURCES.md)** - How to load FHIR resources into the system

## Pipeline & Performance

- **[GO_FAST_ENV_CONFIG.md](GO_FAST_ENV_CONFIG.md)** - Configuration guide for the fast DuckDB pipeline
- **[DUCKDB_QUERY_GUIDE.md](DUCKDB_QUERY_GUIDE.md)** - How to query DuckDB files created by the pipeline
- **[PERFORMANCE_OPTIMIZATION.md](PERFORMANCE_OPTIMIZATION.md)** - Performance tuning and optimization
- **[BENCHMARK_GUIDE.md](BENCHMARK_GUIDE.md)** - How to run benchmarks
- **[BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md)** - Benchmark results and analysis
- **[OPTIMIZATION_COMPLETE.md](OPTIMIZATION_COMPLETE.md)** - Summary of optimization work

## Multi-Source Setup

- **[MULTI_SOURCE_SETUP.md](MULTI_SOURCE_SETUP.md)** - Configure multiple data sources
- **[README_MULTI_SOURCE.md](README_MULTI_SOURCE.md)** - Multi-source architecture overview

## Data Validation & Testing

- **[DATA_VALIDATION.md](DATA_VALIDATION.md)** - Data validation approaches
- **[TEST_RESULTS.md](TEST_RESULTS.md)** - Test execution results
- **[GX_API_EXPLANATION.md](GX_API_EXPLANATION.md)** - Great Expectations API documentation

## Implementation Details

- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Overall implementation summary
- **[IMPLEMENTATION_SUMMARY_RESOURCES.md](IMPLEMENTATION_SUMMARY_RESOURCES.md)** - Resource-specific implementations
- **[README_SQL_ON_FHIR.md](README_SQL_ON_FHIR.md)** - SQL-on-FHIR implementation details
- **[README_STAGE_A.md](README_STAGE_A.md)** - Stage A implementation notes

## Reporting

- **[EVIDENCE_REPORTS.md](EVIDENCE_REPORTS.md)** - Evidence.dev reporting setup and usage

## Dependencies

- **[DEPENDENCY_MANAGEMENT.md](DEPENDENCY_MANAGEMENT.md)** - Managing project dependencies

## Understanding the Project

The FHIRTableSaw project processes FHIR resources through a multi-stage pipeline:

1. **NDJSON Source** - FHIR resources in NDJSON format
2. **DuckDB Loading** - Fast local processing using DuckDB (via `go_fast.py`)
3. **SQL-on-FHIR ViewDefinitions** - Transform FHIR resources to tabular format
4. **CSV Export** - Flattened data in CSV format
5. **PostgreSQL** - Optional upload to PostgreSQL for production use
6. **Evidence Reports** - Data visualization and reporting

### Key Scripts

- `scripts/go_fast.py` - Main pipeline script (31x faster than legacy approach)
- `scripts/go.py` - Legacy pipeline script
- `go_testserver.py`, `go_cms.py`, `go_p.py` - Environment-specific loaders

### Data Flow

```
FHIR NDJSON → DuckDB (.duckdb files) → ViewDefinitions → CSV → PostgreSQL
                ↓
              Evidence Reports (via DuckDB or PostgreSQL)
```

Each NDJSON file gets a corresponding DuckDB file in the same directory:
- `Practitioner.ndjson` → `Practitioner.duckdb`
- `Organization.ndjson` → `Organization.duckdb`
- etc.

These DuckDB files can be queried directly or used with Evidence for reporting.
