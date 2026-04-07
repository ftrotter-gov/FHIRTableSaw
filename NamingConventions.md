# NDJSON File Naming Conventions

This document describes the naming conventions used for FHIR NDJSON files throughout the FHIRTableSaw project. These conventions are enforced by tools in both `analysis_neo4j/` and `fasttools/` directories.

## Overview

The project uses a consistent naming pattern for "directories of NDJSON files" that contain FHIR resources. These files follow a specific structure to enable automated discovery and processing.

## Basic Pattern

```
ResourceType.ndjson
ResourceType.descriptor.ndjson
ResourceType.descriptor1.descriptor2.ndjson
```

### Components

1. **ResourceType** (required): The FHIR resource type name
   - Must match exactly the FHIR resource type (case-sensitive in filename, but matched case-insensitively by tools)
   - Examples: `Practitioner`, `Organization`, `Location`, `Endpoint`, `PractitionerRole`, `OrganizationAffiliation`

2. **Descriptors** (optional): One or more dot-separated descriptors
   - Used to identify subsets, filters, or sources of data
   - Can be state codes, data sources, filtering criteria, etc.
   - Examples: `Wyoming`, `WY`, `Active`, `Hospitals`, `1234567890` (NPI)

3. **Extension** (required): Must be `.ndjson`
   - ONLY files ending in `.ndjson` are processed
   - Files with additional extensions like `.ndjson.gz` are **explicitly excluded**
   - This is enforced to avoid processing compressed files

## Valid Examples

### Simple Resource Files

```
Practitioner.ndjson
Organization.ndjson
Location.ndjson
Endpoint.ndjson
PractitionerRole.ndjson
OrganizationAffiliation.ndjson
```

### With Single Descriptor

```
Practitioner.Wyoming.ndjson
Organization.WY.ndjson
Location.Active.ndjson
Endpoint.Production.ndjson
PractitionerRole.Primary.ndjson
```

### With Multiple Descriptors

```
Practitioner.Wyoming.Active.ndjson
Organization.WY.Hospitals.ndjson
Location.WY-RI.Combined.ndjson
Practitioner.1234567890.ndjson
```

### State-Bounded Subsets (Wyomingizer Output)

The `wyomingizer` tool generates files with state code descriptors:

```
Location.WY.ndjson
Organization.WY.ndjson
Practitioner.WY.ndjson
PractitionerRole.WY.ndjson
OrganizationAffiliation.WY.ndjson
Endpoint.WY.ndjson
```

For multiple states:

```
Location.WY-RI.ndjson
Organization.WY-RI.ndjson
```

### NPI Network Extraction Output

The `extract_practitioner_network` tool generates files with NPI descriptors:

```
Practitioner.1234567890.ndjson
PractitionerRole.1234567890.ndjson
Organization.1234567890.ndjson
Location.1234567890.ndjson
Endpoint.1234567890.ndjson
```

## Invalid Patterns (Will Cause Errors)

### Ambiguous Multi-Resource Type Names

These patterns are **explicitly rejected** because they create ambiguity about which resource type is being referenced:

❌ **INVALID**: `Practitioner.Role.ndjson`
- **Why**: Ambiguous - could be interpreted as:
  - A Practitioner file with descriptor "Role"
  - A PractitionerRole file with missing type
- ✅ **USE INSTEAD**: `PractitionerRole.ndjson` or `PractitionerRole.descriptor.ndjson`

❌ **INVALID**: `Organization.Affiliation.ndjson`
- **Why**: Ambiguous - could be interpreted as:
  - An Organization file with descriptor "Affiliation"
  - An OrganizationAffiliation file with missing type
- ✅ **USE INSTEAD**: `OrganizationAffiliation.ndjson` or `OrganizationAffiliation.descriptor.ndjson`

### Wrong Extensions

❌ **INVALID**: `Practitioner.ndjson.gz`
- **Why**: Compressed files are not processed
- Tools explicitly filter for files ending in `.ndjson` only

❌ **INVALID**: `Practitioner.json`
- **Why**: Must be NDJSON format, not regular JSON

❌ **INVALID**: `Practitioner.txt`
- **Why**: Wrong extension

### Missing Resource Type

❌ **INVALID**: `Wyoming.ndjson`
- **Why**: No resource type specified

❌ **INVALID**: `data.ndjson`
- **Why**: No resource type specified

## Discovery Logic

### Python Implementation (analysis_neo4j)

From `analysis_neo4j/scripts/import_ndjson.py`:

```python
def discover_ndjson_files(*, directory: Path) -> Dict[str, Path]:
    """
    Discovers FHIR NDJSON files using exact resource type matching.
    
    Matches patterns:
    - ResourceType.ndjson (exact match)
    - ResourceType.*.ndjson (wildcard with descriptors)
    
    Rejects confusing patterns:
    - Practitioner.Role.*.ndjson
    - Organization.Affiliation.*.ndjson
    """
```

**Key Features**:

1. **Exact Prefix Matching**: Files must start with the exact resource type followed by `.`
2. **False Match Prevention**: `Organization.*` will NOT match `OrganizationAffiliation.*`
3. **Confusing Pattern Detection**: Actively checks for and errors on ambiguous patterns
4. **First Match Priority**: If multiple files match, the exact match (`ResourceType.ndjson`) is preferred

### Go Implementation (fasttools)

From `fasttools/wyomingizer.go` and `fasttools/extract_practitioner_network.go`:

```go
func findShardFiles(dir, prefix string) ([]string, error) {
    // Case-insensitive matching
    // ONLY process files ending in .ndjson (not .ndjson.gz)
    // Must start with the resource type prefix
}
```

**Key Features**:

1. **Case-Insensitive Matching**: Both prefix and suffix matching are case-insensitive
2. **Strict Extension Filtering**: ONLY `.ndjson` files are accepted
3. **Glob Pattern**: Uses `prefix*` to find all matching files
4. **No Compressed Files**: Explicitly excludes `.ndjson.gz` and other extensions

## Resource Type Differentiation

### How the System Distinguishes Similar Types

The naming convention handles these potentially confusing resource type pairs:

| Resource Type | Valid Patterns | Invalid Patterns |
|--------------|----------------|------------------|
| `Practitioner` | `Practitioner.ndjson`<br>`Practitioner.Wyoming.ndjson`<br>`Practitioner.WY.Active.ndjson` | `Practitioner.Role.ndjson`<br>`Practitioner.Role.WY.ndjson` |
| `PractitionerRole` | `PractitionerRole.ndjson`<br>`PractitionerRole.Wyoming.ndjson`<br>`PractitionerRole.WY.Active.ndjson` | (Must use full compound name) |
| `Organization` | `Organization.ndjson`<br>`Organization.Wyoming.ndjson`<br>`Organization.WY.Hospitals.ndjson` | `Organization.Affiliation.ndjson`<br>`Organization.Affiliation.WY.ndjson` |
| `OrganizationAffiliation` | `OrganizationAffiliation.ndjson`<br>`OrganizationAffiliation.Wyoming.ndjson`<br>`OrganizationAffiliation.WY.ndjson` | (Must use full compound name) |

### Matching Algorithm

1. **Exact resource type match required**: The filename must start with the complete resource type name
2. **Boundary checking**: After the resource type, the next character must be `.` (for descriptors) or end of name (for `.ndjson`)
3. **Prefix length validation**: `Organization.*` will not match `OrganizationAffiliation.*` because:
   - After matching `Organization`, the next character in `OrganizationAffiliation.ndjson` is `A` (not `.`)
   - This fails the boundary check

### Example Differentiation

Given these files in a directory:

```
Practitioner.ndjson              ← Matches: Practitioner
Practitioner.Wyoming.ndjson      ← Matches: Practitioner  
PractitionerRole.ndjson          ← Matches: PractitionerRole
PractitionerRole.Wyoming.ndjson  ← Matches: PractitionerRole
Organization.ndjson              ← Matches: Organization
Organization.Hospitals.ndjson    ← Matches: Organization
OrganizationAffiliation.ndjson   ← Matches: OrganizationAffiliation
```

When searching for `Practitioner`:
- ✅ Matches: `Practitioner.ndjson`, `Practitioner.Wyoming.ndjson`
- ❌ Does NOT match: `PractitionerRole.ndjson`, `PractitionerRole.Wyoming.ndjson`

When searching for `PractitionerRole`:
- ✅ Matches: `PractitionerRole.ndjson`, `PractitionerRole.Wyoming.ndjson`
- ❌ Does NOT match: `Practitioner.ndjson`, `Practitioner.Wyoming.ndjson`

When searching for `Organization`:
- ✅ Matches: `Organization.ndjson`, `Organization.Hospitals.ndjson`
- ❌ Does NOT match: `OrganizationAffiliation.ndjson`

## Error Handling

### Python Error Messages (analysis_neo4j)

When confusing patterns are detected, the Python importer exits with a clear error:

```
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ERROR: Confusing filename detected!
File: Practitioner.Role.Wyoming.ndjson
This pattern is ambiguous. Please rename to:
  PractitionerRole.*.ndjson
Example: PractitionerRole.Wyoming.ndjson
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
```

This happens BEFORE any processing begins, ensuring data is not misclassified.

### Go Silent Filtering (fasttools)

The Go tools don't error on confusing patterns but may silently skip them:
- Files not ending in `.ndjson` are ignored
- Files not matching the expected prefix pattern are ignored

## Best Practices

### DO

✅ Use full compound resource type names: `PractitionerRole`, `OrganizationAffiliation`

✅ Use descriptive middle segments: `Organization.Wyoming.ndjson`, `Practitioner.Active.ndjson`

✅ Use state codes for geographic subsets: `Location.WY.ndjson`, `Organization.WY-RI.ndjson`

✅ Use NPIs for practitioner networks: `Practitioner.1234567890.ndjson`

✅ Keep descriptors meaningful and readable: `Organization.Hospitals.Active.ndjson`

### DON'T

❌ Split compound resource types: `Practitioner.Role.ndjson`

❌ Use compressed extensions: `.ndjson.gz`

❌ Use generic names without resource type: `data.ndjson`, `output.ndjson`

❌ Mix resource types in descriptors: `Practitioner.Organization.ndjson` (confusing)

## Supported Resource Types

The following FHIR resource types are currently supported:

- `Practitioner`
- `PractitionerRole`
- `Organization`
- `OrganizationAffiliation`
- `Location`
- `Endpoint`

Additional resource types can be added by updating the importer configuration in:
- `analysis_neo4j/scripts/import_ndjson.py` (`IMPORTER_MAP`)
- `fasttools/wyomingizer.go` (`inputFiles` struct)

## References

### Implementation Files

- **Python Discovery Logic**: `analysis_neo4j/scripts/import_ndjson.py` (lines 46-139)
- **Go File Finding (wyomingizer)**: `fasttools/wyomingizer.go` (lines 362-385)
- **Go File Finding (network extraction)**: `fasttools/extract_practitioner_network.go` (lines 511-526)

### Key Code Sections

**Python Confusing Pattern Detection**:

```python
confusing_patterns = {
    'Practitioner.Role': 'PractitionerRole',
    'Organization.Affiliation': 'OrganizationAffiliation',
}
```

**Go Extension Filtering**:

```go
// ONLY process files ending in .ndjson (not .ndjson.gz or any other extension)
if !strings.HasSuffix(lower, ".ndjson") {
    continue
}
```

## Version History

- Initial documentation: 2026-04-07
- Consolidated from analysis_neo4j and fasttools implementations
