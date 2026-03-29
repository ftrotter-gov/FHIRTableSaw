#!/usr/bin/env bash
# =============================================================================
# build_static.sh - Build Evidence static site for GitHub Pages
# =============================================================================
#
# This script builds the Evidence reports as a static site suitable for
# deployment to GitHub Pages. It can run either inside Docker or natively
# with Node.js installed.
#
# Usage:
#   Via Docker (recommended):
#     cd evidence && docker compose run evidence-build
#
#   Natively (requires Node.js >= 18):
#     cd evidence && bash scripts/build_static.sh
#
# Environment variables (set in .env or export before running):
#   EVIDENCE_SOURCE__postgres__host     - PostgreSQL host
#   EVIDENCE_SOURCE__postgres__port     - PostgreSQL port
#   EVIDENCE_SOURCE__postgres__database - PostgreSQL database name
#   EVIDENCE_SOURCE__postgres__user     - PostgreSQL username
#   EVIDENCE_SOURCE__postgres__password - PostgreSQL password
#   EVIDENCE_SOURCE__postgres__ssl      - SSL mode (false, true, no-verify)
#   BASE_PATH                           - GitHub Pages base path (default: /FHIRTableSaw)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVIDENCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${EVIDENCE_DIR}"

# Default base path for GitHub Pages
BASE_PATH="${BASE_PATH:-/FHIRTableSaw}"

echo "=== FHIRTableSaw Evidence Static Build ==="
echo "Evidence directory: ${EVIDENCE_DIR}"
echo "Base path: ${BASE_PATH}"
echo ""

# Install dependencies if node_modules is missing
if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
fi

# Extract data from configured sources
echo "Running source extraction..."
npm run sources

# Build the static site
echo "Building static site..."
EVIDENCE_BUILD_DIR="./build${BASE_PATH}" npm run build

echo ""
echo "=== Build complete ==="
echo "Static files are in: ${EVIDENCE_DIR}/build${BASE_PATH}"
echo "Ready for GitHub Pages deployment."
