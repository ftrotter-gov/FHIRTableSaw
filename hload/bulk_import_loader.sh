#!/bin/bash
# Wrapper script for bulk_import_loader.py
# Provides convenient shell interface to the Python bulk import tool

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="${SCRIPT_DIR}/bulk_import_loader.py"

# Check if Python script exists
if [[ ! -f "$PYTHON_SCRIPT" ]]; then
    echo "Error: bulk_import_loader.py not found at: $PYTHON_SCRIPT"
    exit 1
fi

# Execute the Python script with all arguments passed through
exec python3 "$PYTHON_SCRIPT" "$@"
