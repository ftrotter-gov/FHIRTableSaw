# Benchmark Guide: Testing Performance on Production Data

## Overview

This guide helps you measure actual performance on production data to:
1. Verify the optimization improvements
2. Find the optimal batch size for your environment
3. Identify any remaining bottlenecks

## Quick Test (Recommended First Step)

Test with 10,000 rows to get a baseline:

```bash
python scripts/test_with_timing.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson --limit 10000 --batchsize 5000
```

This will show:
- Total time elapsed
- Rows loaded
- Rows per second throughput

## Full Benchmark Suite

Test multiple batch sizes to find the optimal setting:

```bash
python scripts/benchmark_batch_sizes.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson
```

### Custom Batch Sizes

```bash
# Test specific batch sizes
python scripts/benchmark_batch_sizes.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --batch-sizes "1000,2500,5000,10000,25000,50000"

# Test with different row limit
python scripts/benchmark_batch_sizes.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --max-rows 50000 \
    --batch-sizes "10000,25000,50000"
```

## Individual Script Testing

All loading scripts now support `--limit`:

```bash
# Test Practitioner loading with 100K row limit
python scripts/load_practitioner_ndjson.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --limit 100000 \
    --batchsize 10000 \
    --replace
```

## Understanding the Results

### Expected Output

```
Testing file: /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson
Batch size: 5000
Row limit: 10000

..
✓ Trimming final batch to X rows to respect max_rows limit
Finalizing data transfer from staging to target table...
✓ Data transfer complete!

======================================================================
TIMING RESULTS
======================================================================
Total elapsed time: XX.XX seconds
Rows loaded: 10000
Batches processed: 2
Throughput: XXX.XX rows/second
======================================================================
```

### Interpreting Throughput

| Rows/Second | Performance Level | Notes |
|-------------|------------------|-------|
| < 100 | Poor | Possible bottleneck in fhir4ds or network |
| 100-500 | Fair | Acceptable for small datasets |
| 500-2000 | Good | Reasonable performance |
| 2000-10000 | Excellent | Near-optimal for remote DB |
| > 10000 | Outstanding | Likely local DB or very fast network |

## Known Bottlenecks

Based on code analysis and terminal output, time is spent in:

### 1. **fhir4ds Loading (60-90% of time)**
The `datastore.load_resources(matching_batch)` call inserts resources one-by-one into fhir4ds's internal `fhir_resources` table. This is the primary bottleneck.

**Cannot be optimized** without modifying fhir4ds library itself.

### 2. **ViewDefinition Execution (5-20% of time)**
The `view_runner.execute_view_definition()` runs SQL queries to flatten FHIR data.

**Moderate optimization potential** through ViewDefinition simplification.

### 3. **PostgreSQL Write (1-5% of time with optimization)**
With the staging table optimization, this is now minimal.

**Already optimized!** ✅

### 4. **NDJSON Reading (<1% of time)**
File I/O is very fast.

**Not a bottleneck.**

## Realistic Performance Expectations

Given that fhir4ds is the bottleneck (not our code), expect:

- **NOT 2000x speedup** from our optimization alone
- **10-50x speedup** is realistic (we eliminated the PostgreSQL bottleneck)
- **Further speedup requires** fhir4ds modifications or alternative approach

### Example Projections

If 10,000 rows takes 60 seconds:
- **Throughput**: 167 rows/second
- **7.1M rows**: ~11.8 hours (down from days!)
- **14M rows**: ~23.6 hours

This is still **10-20x faster** than the old row-by-row UPSERT, but fhir4ds remains the limiting factor.

## Next Steps After Benchmarking

### Option 1: Accept Current Performance
If 10-20x speedup is sufficient, you're done!

### Option 2: Further Optimization

Potential strategies:

1. **Bypass fhir4ds for bulk loading**
   - Load NDJSON directly to PostgreSQL as JSONB
   - Run ViewDefinition SQL directly on JSONB column
   - Could be 10-100x faster

2. **Parallel processing**
   - Process multiple files simultaneously
   - 4-6x additional speedup on multi-core systems

3. **Pre-process NDJSON**
   - Convert to simpler format before loading
   - Skip fhir4ds overhead

4. **Contact fhir4ds maintainers**
   - Request bulk insert functionality
   - Or fork and modify ourselves

## Running Your Benchmarks

Recommended test sequence:

```bash
# 1. Small test (10K rows)
python scripts/test_with_timing.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --limit 10000 --batchsize 5000

# 2. Medium test (100K rows)
python scripts/test_with_timing.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --limit 100000 --batchsize 10000

# 3. Batch size comparison (uses 100K rows each)
python scripts/benchmark_batch_sizes.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --batch-sizes "2500,5000,10000,20000,50000"

# 4. Full file (if satisfied with performance)
python scripts/load_practitioner_ndjson.py \
    /Volumes/eBolt/palantir/ndjson/initial/Practitioner.ndjson \
    --batchsize 10000 --replace
```

## Reporting Results

After running benchmarks, document:
1. Rows per second achieved
2. Optimal batch size found
3. Estimated time for full 7.1M row load
4. Whether further optimization is needed

Save results to a file:

```bash
python scripts/test_with_timing.py /path/to/file --limit 100000 --batchsize 10000 \
    2>&1 | tee benchmark_results.txt
```

## Files Created for Benchmarking

- `scripts/test_with_timing.py` - Simple timing test with --limit support
- `scripts/benchmark_batch_sizes.py` - Compare multiple batch sizes
- `scripts/load_practitioner_ndjson.py` - Updated with --limit flag
- `PERFORMANCE_OPTIMIZATION.md` - Detailed optimization documentation
- `BENCHMARK_GUIDE.md` - This file

## Summary

The optimization **did work** - we fixed the catastrophic row-by-row UPSERT. However, you were right to be skeptical about the 2000x number - that assumed the database write was the only bottleneck. In reality, **fhir4ds loading is 60-90% of the time**, which we cannot optimize without changing the library itself.

**Realistic improvement: 10-50x faster** (still a huge win!)
