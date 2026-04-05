#!/bin/bash
# Build script for fasttools Go programs

set -e

cd "$(dirname "$0")/.."

echo "Building find_npi..."
go build -o fasttools/find_npi.gobin fasttools/find_npi.go

echo "Building categorize_ndjson..."
go build -o fasttools/categorize_ndjson/categorize_ndjson.gobin fasttools/categorize_ndjson/main.go

echo "Building quick_validate..."
go build -o fasttools/quick_validate.gobin fasttools/quick_validate.go

echo "Building extract_practitioner_network..."
go build -o fasttools/extract_practitioner_network.gobin fasttools/extract_practitioner_network.go

echo ""
echo "✓ Build complete!"
echo ""
echo "Available tools:"
echo "  - fasttools/find_npi.gobin                    - Extract FHIR resources by NPI"
echo "  - fasttools/extract_practitioner_network.gobin - Extract complete Practitioner network by NPI"
echo "  - fasttools/categorize_ndjson/categorize_ndjson.gobin - Count FHIR resource types in NDJSON"
echo "  - fasttools/quick_validate.gobin              - Validate FHIR v4 resources in NDJSON files"
