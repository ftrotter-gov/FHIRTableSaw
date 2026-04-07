#!/bin/bash
# Load FHIR resources from a directory to a HAPI FHIR server
# Uses HAPI's transaction bundle API to upload NDJSON data

set -e

# Default values
HAPI_URL="${HAPI_URL:-http://localhost:8080/fhir}"
DIRECTORY=""

# Loading order to satisfy referential integrity
RESOURCE_ORDER=(
    "Organization"
    "Location"
    "Endpoint"
    "Practitioner"
    "OrganizationAffiliation"
    "PractitionerRole"
)

# Usage
usage() {
    cat <<EOF
Usage: $(basename "$0") <directory> [options]

Load FHIR resources from NDJSON files to a HAPI server

Arguments:
  directory             Directory containing NDJSON files

Options:
  --hapi-url URL       HAPI server URL (default: http://localhost:8080/fhir)
  -h, --help           Show this help message

Loading order:
  1. Organization
  2. Location
  3. Endpoint
  4. Practitioner
  5. OrganizationAffiliation
  6. PractitionerRole

File naming convention (from NamingConventions.md):
  - ResourceType.ndjson
  - ResourceType.descriptor.ndjson

Examples:
  $(basename "$0") /path/to/ndjson/files
  $(basename "$0") ./data --hapi-url http://localhost:8080/fhir
  HAPI_URL=http://localhost:8080/fhir $(basename "$0") ./data
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        --hapi-url)
            HAPI_URL="$2"
            shift 2
            ;;
        *)
            if [[ -z "$DIRECTORY" ]]; then
                DIRECTORY="$1"
            else
                echo "Error: Unknown argument: $1"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [[ -z "$DIRECTORY" ]]; then
    echo "Error: Directory argument is required"
    echo ""
    usage
fi

if [[ ! -d "$DIRECTORY" ]]; then
    echo "Error: Directory not found: $DIRECTORY"
    exit 1
fi

# Strip trailing slash from HAPI URL
HAPI_URL="${HAPI_URL%/}"

echo "================================================================================"
echo "HAPI FHIR Loader"
echo "================================================================================"
echo "Directory:    $DIRECTORY"
echo "HAPI Server:  $HAPI_URL"
echo "Load Order:   ${RESOURCE_ORDER[*]}"
echo "================================================================================"
echo ""

# Function to find NDJSON file for a resource type
find_ndjson_file() {
    local resource_type="$1"
    local dir="$2"
    
    # Look for exact match first
    if [[ -f "$dir/${resource_type}.ndjson" ]]; then
        echo "$dir/${resource_type}.ndjson"
        return 0
    fi
    
    # Look for files with descriptors
    local files=("$dir/${resource_type}".*.ndjson)
    if [[ -f "${files[0]}" ]]; then
        echo "${files[0]}"
        return 0
    fi
    
    return 1
}

# Function to upload a single resource from NDJSON
upload_resource() {
    local resource_json="$1"
    
    curl -s -X POST \
        -H "Content-Type: application/fhir+json" \
        -d "$resource_json" \
        "${HAPI_URL}" \
        -w "\n%{http_code}" | tail -1
}

# Function to load an NDJSON file
load_ndjson_file() {
    local file="$1"
    local resource_type="$2"
    
    echo "📂 Loading $resource_type from: $(basename "$file")"
    
    local total=0
    local success=0
    local failed=0
    
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        
        ((total++))
        
        # Upload resource
        http_code=$(upload_resource "$line")
        
        if [[ "$http_code" =~ ^(200|201)$ ]]; then
            ((success++))
        else
            ((failed++))
            echo "  ✗ Failed: HTTP $http_code"
        fi
        
        # Progress report every 100 resources
        if (( total % 100 == 0 )); then
            echo "  Progress: $total processed ($success success, $failed failed)"
        fi
    done < "$file"
    
    echo "  ✓ Completed: $total total, $success success, $failed failed"
    echo ""
    
    return $failed
}

# Main loading loop
total_success=0
total_failed=0
found_count=0

echo "🔍 Discovering NDJSON files..."
echo ""

for resource_type in "${RESOURCE_ORDER[@]}"; do
    file=$(find_ndjson_file "$resource_type" "$DIRECTORY") || true
    
    if [[ -n "$file" ]]; then
        ((found_count++))
        echo "  Found: $resource_type -> $(basename "$file")"
    fi
done

if [[ $found_count -eq 0 ]]; then
    echo "  ⚠ No matching NDJSON files found"
    exit 1
fi

echo ""
echo "🚀 Starting load process..."
echo ""

for resource_type in "${RESOURCE_ORDER[@]}"; do
    file=$(find_ndjson_file "$resource_type" "$DIRECTORY") || true
    
    if [[ -z "$file" ]]; then
        echo "⏭  Skipping $resource_type (no file found)"
        echo ""
        continue
    fi
    
    load_ndjson_file "$file" "$resource_type"
    result=$?
    
    # Count successes/failures (approximate from return code)
    if [[ $result -eq 0 ]]; then
        line_count=$(wc -l < "$file" | tr -d ' ')
        ((total_success += line_count))
    else
        ((total_failed += result))
    fi
done

echo "================================================================================"
echo "LOADING SUMMARY"
echo "================================================================================"
echo "Approximate totals:"
echo "  Success: $total_success"
echo "  Failed:  $total_failed"
echo "================================================================================"

if [[ $total_failed -gt 0 ]]; then
    echo ""
    echo "⚠  Some resources failed to load. Check the logs above for details."
    exit 1
else
    echo ""
    echo "✓ Successfully loaded resources to HAPI server."
fi
