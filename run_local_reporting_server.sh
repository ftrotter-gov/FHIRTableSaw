#!/bin/bash

# Quick script to run local Evidence reporting server
# This script extracts data sources, builds the static site, and serves it locally

set -e

echo "=== FHIRTableSaw Local Reporting Server ==="
echo ""

# Change to evidence directory
cd "$(dirname "$0")/evidence"

echo "Step 1: Extracting data from sources..."
npm run sources

echo ""
echo "Step 2: Building static site..."
npm run build

echo ""
echo "Step 3: Starting local server..."
echo ""
echo "📊 Reports will be available at:"
echo "   http://localhost:8000/"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Change to static site directory and start server
cd static_site_content
python3 -m http.server 8000
