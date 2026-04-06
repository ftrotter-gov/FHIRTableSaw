#!/bin/bash
# Build script for fasttools Go programs

set -e

cd "$(dirname "$0")/.."

echo "Building find_npi..."
go build -o fasttools/find_npi.gobin ./cmd/find_npi

echo "Building categorize_ndjson..."
go build -o fasttools/categorize_ndjson.gobin ./cmd/categorize_ndjson

echo "Building quick_validate..."
go build -o fasttools/quick_validate.gobin ./cmd/quick_validate

echo "Building extract_practitioner_network..."
go build -o fasttools/extract_practitioner_network.gobin ./cmd/extract_practitioner_network

echo "Building wyomingizer..."
go build -o fasttools/wyomingizer.gobin ./cmd/wyomingizer

echo ""
echo "✓ Build complete!"
echo ""
echo "Available tools:"
echo "  - fasttools/find_npi.gobin                    - Extract FHIR resources by NPI"
echo "  - fasttools/extract_practitioner_network.gobin - Extract complete Practitioner network by NPI"
echo "  - fasttools/wyomingizer.gobin                 - Extract state-bounded FHIR resource subset"
echo "  - fasttools/categorize_ndjson/categorize_ndjson.gobin - Count FHIR resource types in NDJSON"
echo "  - fasttools/quick_validate.gobin              - Validate FHIR v4 resources in NDJSON files"
