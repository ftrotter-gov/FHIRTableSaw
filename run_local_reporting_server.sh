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

# Ensure static_site_content directory exists
if [ ! -d "static_site_content" ]; then
    echo "Creating static_site_content directory..."
    mkdir -p static_site_content
    
    # Check if Evidence built to build/ directory instead
    if [ -d "build/FHIRTableSaw" ]; then
        echo "Copying from build directory..."
        cp -r build/FHIRTableSaw/* static_site_content/
    fi
fi

# Verify the directory has content
if [ ! -f "static_site_content/index.html" ]; then
    echo "❌ Error: Static site build failed or incomplete"
    echo "   No index.html found in static_site_content/"
    exit 1
fi

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
