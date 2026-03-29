#!/usr/bin/env bash
# =============================================================================
# build_reports.sh - Simple wrapper to build Evidence reports
# =============================================================================
#
# This script builds the Evidence static site by calling the build script
# in the evidence directory.
#
# Usage:
#   ./build_reports.sh
#
# The script will:
#   1. Install npm dependencies if needed
#   2. Extract data from configured sources (PostgreSQL)
#   3. Build the static site
#   4. Output the results to evidence/static_site_content/
#
# Prerequisites:
#   - Node.js >= 18 installed
#   - PostgreSQL connection configured in evidence/sources/postgres/connection.yaml
#
# =============================================================================

set -euo pipefail

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Building Evidence Reports ==="
echo ""

# Call the actual build script in the evidence directory
bash "${SCRIPT_DIR}/evidence/scripts/build_static.sh"

echo ""
echo "=== Build Complete ==="
echo "Static site is ready in: ${SCRIPT_DIR}/evidence/static_site_content/"
echo ""
echo "To view locally, run:"
echo "  cd evidence/static_site_content && python3 -m http.server 8000"
echo "Then open: http://localhost:8000/"
