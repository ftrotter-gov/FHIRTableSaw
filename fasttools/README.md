# FastTools

Fast command-line tools for working with FHIR NDJSON data, written in Go for performance.

## Tools

### find_npi

Extract FHIR resources containing a specific NPI (National Provider Identifier) from NDJSON files.

**Purpose:**
Scans FHIR Bulk Export style NDJSON files and finds resources that contain a specific NPI in their `identifier[]` array (where `system == "http://hl7.org/fhir/sid/us-npi"`), outputting matching resources as pretty-printed JSON.

**Usage:**

```bash
fasttools/find_npi.gobin <npi> <input.ndjson|input.ndjson.gz|-> <output.json>
```

**Examples:**

```bash
# Search plain NDJSON file
fasttools/find_npi.gobin 1234567890 practitioners.ndjson matches.json

# Search gzip-compressed NDJSON file
fasttools/find_npi.gobin 1234567890 practitioners.ndjson.gz matches.json

# Search from stdin
cat practitioners.ndjson | fasttools/find_npi.gobin 1234567890 - matches.json
```

**Features:**

- Supports plain NDJSON and gzip-compressed files (*.gz)
- Handles large files efficiently with streaming processing
- Outputs pretty-printed JSON arrays for easy inspection
- Skips invalid JSON lines with warnings to stderr
- Can process up to 50MB per line

### categorize_ndjson

Count FHIR resource types in NDJSON files for quick inspection and validation.

**Purpose:**
Streams NDJSON files and counts how many resources of each `resourceType` are present, providing a summary report.

**Usage:**

```bash
go run fasttools/categorize_ndjson/main.go [flags] <file|dir|->...
```

**Features:**

- Counts resources by type
- Supports directories (walks recursively)
- Handles gzip-compressed files
- Progress reporting with progress bars
- MD5 checksums and file size reporting
- Detects invalid JSON lines

### quick_validate

Fast FHIR v4 validator that validates resources in NDJSON files and reports statistics.

**Purpose:**
Validates FHIR v4 resources in NDJSON files for structural compliance with FHIR R4 specifications. Provides detailed validation statistics including percentage of valid records, throughput metrics, and resource type breakdown.

**Usage:**

```bash
fasttools/quick_validate.gobin <input.ndjson|input.ndjson.gz|->
```

**Examples:**

```bash
# Validate plain NDJSON file
fasttools/quick_validate.gobin practitioners.ndjson

# Validate gzip-compressed NDJSON file
fasttools/quick_validate.gobin practitioners.ndjson.gz

# Validate from stdin
cat practitioners.ndjson | fasttools/quick_validate.gobin -

# Validate a sample
head -1000 practitioners.ndjson | fasttools/quick_validate.gobin -
```

**Features:**

- Fast validation using Go's native JSON parsing
- Validates FHIR R4 resource structure and required fields
- Supports resource types: Patient, Practitioner, PractitionerRole, Organization, Location, Endpoint, OrganizationAffiliation
- Handles plain NDJSON and gzip-compressed files
- Real-time progress reporting
- Detailed statistics: validation rate, throughput, resource type breakdown
- Can process millions of records efficiently (>100k records/second)

**Validation Checks:**

- JSON structure validity
- Required `resourceType` field
- Resource-specific required fields per FHIR R4 spec
- Valid data types for common fields
- Status code validation for Endpoints

## Building

To build all tools:

```bash
./fasttools/build.sh
```

Or build individually:

```bash
# Build find_npi
go build -o fasttools/find_npi.gobin fasttools/find_npi.go

# Build categorize_ndjson
go build -o fasttools/categorize_ndjson/categorize_ndjson.gobin fasttools/categorize_ndjson/main.go

# Build quick_validate
go build -o fasttools/quick_validate.gobin fasttools/quick_validate.go
```

## Requirements

- Go 1.18 or later

All tools use only Go standard library - no external dependencies required.

## Notes

- Binaries are OS/arch-specific and should not be committed to git
- The `.gitignore` is configured to exclude built binaries
- Invalid JSON lines are skipped with warnings to help handle real-world data
