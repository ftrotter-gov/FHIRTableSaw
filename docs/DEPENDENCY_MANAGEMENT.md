# Dependency Management

This document describes the dependency management system for FHIRTableSaw to prevent mid-execution crashes due to missing dependencies.

## Overview

The project now includes:
1. **requirements.txt** - Complete list of all required dependencies
2. **check_dependencies.py** - Utility to verify all dependencies are installed
3. **Automatic checks** in all go.py scripts to fail fast if dependencies are missing

## Quick Start

### Installing Dependencies

To install all required dependencies, run one of the following:

```bash
# Option 1: Install from requirements.txt
pip install -r requirements.txt

# Option 2: Install the package in development mode (recommended)
pip install -e .

# Option 3: Install the package in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Checking Dependencies

To verify all dependencies are installed:

```bash
python check_dependencies.py
```

This will:
- ✓ Exit with code 0 if all dependencies are installed
- ✗ Exit with code 1 and show missing packages if any are missing

## Files

### requirements.txt

Contains all required dependencies with minimum version requirements:

- **Core dependencies** (from pyproject.toml):
  - httpx, PyYAML, SQLAlchemy, pydantic, fhir-core, psycopg, fhir4ds, great-expectations, pandas

- **Additional dependencies**:
  - duckdb, python-dotenv

- **Development dependencies** (optional):
  - pytest, ruff

### check_dependencies.py

A standalone Python script that can be:
- **Run directly**: `python check_dependencies.py` to check all dependencies
- **Imported**: Used by other scripts to check dependencies programmatically

Functions:
- `check_dependencies()` - Returns dict with status and missing packages
- `require_dependencies()` - Checks dependencies and exits if any are missing

### Scripts with Dependency Checks

The following scripts now check dependencies before running:
- `go.py` - Main runner script
- `create_ndjson_from_api.py` - FHIR API downloader
- `scripts/go.py` - Batch loader (old method)
- `scripts/go_fast.py` - Fast batch loader (DuckDB method)
- `scripts/process_ndjson_fast.py` - Fast processing pipeline

## Error Handling

If dependencies are missing, scripts will:

1. Display a clear error message listing missing packages
2. Show installation commands
3. Exit immediately with code 1

Example output:
```
======================================================================
ERROR: Missing required dependencies!
======================================================================

The following packages are required but not installed:
  - fhir4ds
  - duckdb

To install all required dependencies, run:
  pip install -r requirements.txt

Or install missing packages individually:
  pip install fhir4ds duckdb

Or install the package in development mode:
  pip install -e .
======================================================================
```

## Integration with Virtual Environments

### Best Practice

Always use a virtual environment:

```bash
# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

### CI/CD Integration

For CI/CD pipelines, check dependencies as a separate step:

```yaml
- name: Check dependencies
  run: python check_dependencies.py
```

## Troubleshooting

### "check_dependencies.py not found"

This error means the script couldn't find the dependency checker. Solutions:
1. Ensure you're running from the repository root
2. Check that `check_dependencies.py` exists in the repo root

### Import errors despite passing check

If `check_dependencies.py` passes but imports still fail:
1. Verify you're using the correct Python environment: `which python`
2. Check Python version: `python --version` (requires Python 3.11+)
3. Try reinstalling: `pip install --force-reinstall -r requirements.txt`

### Conflicting dependencies

If you encounter dependency conflicts:
1. Start with a fresh virtual environment
2. Install from requirements.txt first
3. Report the issue if conflicts persist

## Development

### Adding New Dependencies

When adding new dependencies:

1. Add to `pyproject.toml` under `dependencies`
2. Update `requirements.txt` with the same version constraints
3. Add to `check_dependencies.py` in the `required_deps` dict
4. Test with `python check_dependencies.py`

Example:
```python
required_deps = {
    "httpx": "httpx",
    "new_package": "new-package-name",  # Add here
    # ...
}
```

### Testing Dependency Checks

To test the dependency checker with a missing package:

```bash
# Create a test environment without a package
python -m venv test_env
source test_env/bin/activate
pip install httpx PyYAML  # Install only some dependencies

# This should fail and show missing packages
python check_dependencies.py
```

## Background

This system was implemented to prevent the following scenario:
1. User starts a long-running script (e.g., downloading FHIR data)
2. Script runs for a long time
3. Script crashes halfway through due to missing dependency (e.g., fhir4ds)
4. User has to reinstall and restart from scratch

With the new system:
- Scripts check all dependencies immediately on startup
- Failures are immediate and clear
- No time is wasted on partial processing
