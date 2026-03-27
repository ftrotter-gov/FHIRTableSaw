# go_fast.py Environment Configuration Update

## Summary

Updated `go_fast.py` to use `.env` configuration similar to `go.py`, making it easier to configure default settings without requiring CLI arguments every time.

## Changes Made

### 1. Updated `scripts/go_fast.py`

- **Added environment variable support**: The script now reads default configuration values from `.env` file
- **CLI arguments override defaults**: Command-line arguments take precedence over `.env` values
- **Added `--no-upload` flag**: Allows explicitly disabling PostgreSQL upload even if enabled in `.env`
- **Enhanced help text**: Shows current default values from `.env` in help output

### 2. Updated `env.example`

Added documentation for new `go_fast.py` configuration variables:

- `BATCH_SIZE` - Number of resources to process per batch (default: 5000)
- `UPLOAD_TO_POSTGRESQL` - Whether to upload to PostgreSQL automatically (default: false)
- `UPLOAD_MODE` - PostgreSQL upload mode: replace/append/fail (default: replace)
- `TEMP_DIR` - Directory for DuckDB temp files (falls back to FHIR_API_CACHE_FOLDER)

## New Environment Variables

```bash
# go_fast.py Configuration (FAST Pipeline: DuckDB → CSV → PostgreSQL)

# Number of resources to process per batch (default: 5000)
BATCH_SIZE=5000

# Whether to upload CSV files to PostgreSQL automatically (default: false)
# Set to "true", "1", or "yes" to enable automatic uploads
UPLOAD_TO_POSTGRESQL=false

# PostgreSQL upload mode (default: replace)
# Options: replace, append, fail
UPLOAD_MODE=replace

# Directory for DuckDB temporary files (optional)
# If not set, falls back to FHIR_API_CACHE_FOLDER, then to NDJSON directory
TEMP_DIR=
```

## Usage Examples

### Using .env Defaults

If your `.env` has:
```bash
BATCH_SIZE=10000
UPLOAD_TO_POSTGRESQL=true
UPLOAD_MODE=append
TEMP_DIR=/fast/ssd/cache
```

Then you can simply run:
```bash
python scripts/go_fast.py /path/to/ndjson/directory --test
```

And it will use the settings from `.env`.

### Overriding with CLI Arguments

CLI arguments always take precedence:

```bash
# Override batch size
python scripts/go_fast.py /path/to/ndjson --batch-size 20000

# Disable upload even if enabled in .env
python scripts/go_fast.py /path/to/ndjson --no-upload

# Override upload mode
python scripts/go_fast.py /path/to/ndjson --upload --upload-mode replace
```

### Backward Compatibility

All existing CLI usage patterns still work exactly as before:

```bash
# Old usage still works
python scripts/go_fast.py /path/to/ndjson --test --upload --batch-size 5000

# New usage with .env defaults
python scripts/go_fast.py /path/to/ndjson --test
```

## Benefits

1. **Less typing**: Set common options once in `.env` instead of specifying them every time
2. **Consistent defaults**: Team members share the same configuration through `.env`
3. **Flexible overrides**: CLI arguments override `.env` when needed
4. **Better documentation**: Help text shows current defaults from `.env`
5. **Parallel to go.py**: Both scripts now use the same configuration approach

## Testing

The configuration loading has been tested and verified:

```bash
python test_go_fast_config.py
```

Output shows:
- ✓ Configuration values loaded from `.env`
- ✓ Default values used by `go_fast.py`
- ✓ Fallback behavior (TEMP_DIR → FHIR_API_CACHE_FOLDER)

## Migration Guide

### For Users

1. Copy `env.example` to `.env` if you haven't already
2. Add the new `go_fast.py` configuration variables to your `.env`:
   ```bash
   BATCH_SIZE=5000
   UPLOAD_TO_POSTGRESQL=false
   UPLOAD_MODE=replace
   TEMP_DIR=/your/preferred/temp/directory
   ```
3. Run `go_fast.py` as before - it will now use your `.env` defaults

### For Developers

- The configuration loading happens at the start of `main()`
- Uses `os.environ.get()` for reading with fallback defaults
- Boolean parsing: "true", "1", "yes" (case-insensitive) = True
- Integer parsing: `int(os.environ.get("BATCH_SIZE", "5000"))`

## Files Modified

1. `scripts/go_fast.py` - Added environment variable configuration loading
2. `env.example` - Documented new configuration variables
3. `test_go_fast_config.py` - Test script to verify configuration loading (can be deleted)
4. `GO_FAST_ENV_CONFIG.md` - This documentation file
