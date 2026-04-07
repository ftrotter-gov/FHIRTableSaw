# Neo4j Analysis Subproject

A self-contained Neo4j graph database analysis environment for FHIR NDJSON resources.

## Overview

This subproject provides:

- Dockerized Neo4j instance for graph analysis
- Python import scripts for FHIR NDJSON resources
- Schema definitions with indexes on FHIR identifiers and NPIs
- Idempotent import process (can be rerun safely)

## Supported FHIR Resources

- Practitioner
- PractitionerRole
- Organization
- OrganizationAffiliation
- Endpoint
- Location

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ with the parent FHIRTableSaw virtual environment activated
- FHIR NDJSON files to import

## Quick Start

### 1. Initial Setup

```bash
# Navigate to the analysis_neo4j directory
cd analysis_neo4j

# Copy environment template and set your password
cp .env.example .env
# Edit .env and set a secure password

# Start Neo4j
docker-compose up -d

# Wait for Neo4j to be ready (check logs)
docker-compose logs -f neo4j
```

Neo4j will be available at:

- Browser UI: <http://localhost:7474>
- Bolt protocol: `bolt://localhost:7687`

### 2. Install Python Dependencies

```bash
# Navigate back to parent directory and activate the main venv
cd ..
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install Neo4j driver if not already installed
pip install neo4j>=5.14.0

# Navigate back to analysis_neo4j
cd analysis_neo4j
```

**Note:** This subproject uses the parent FHIRTableSaw Python environment. The neo4j driver has been added to the parent `requirements.txt`.

### 3. Create Indexes

```bash
# Apply schema and create indexes
cat schema/indexes.cypher | docker exec -i fhir_neo4j_analysis cypher-shell -u neo4j -p your_password_here
```

### 4. Import FHIR Data

```bash
# Import all supported resources from a directory
python scripts/import_ndjson.py /path/to/ndjson/directory

# Or specify batch size
python scripts/import_ndjson.py /path/to/ndjson/directory --batch-size 1000
```

### 5. Verify Import

Open Neo4j Browser at <http://localhost:7474> and run:

```cypher
// Count nodes by type
MATCH (n) RETURN labels(n) AS type, count(n) AS count
ORDER BY count DESC;

// Find practitioners with NPIs
MATCH (p:Practitioner)
WHERE p.npi IS NOT NULL
RETURN p.fhir_id, p.npi, p.name
LIMIT 10;

// Verify relationship structure
MATCH (p:Practitioner)-[r]->(other)
RETURN type(r) AS relationship_type, labels(other) AS target_type, count(*) AS count
ORDER BY count DESC;
```

## Import Script Details

### Filename Matching

The import script supports **wildcard filenames** while preventing misclassification:

**Supported patterns:**
- `Organization.ndjson` or `Organization.*.ndjson` → Organization nodes
- `OrganizationAffiliation.ndjson` or `OrganizationAffiliation.*.ndjson` → OrganizationAffiliation nodes
- `Practitioner.ndjson` or `Practitioner.*.ndjson` → Practitioner nodes
- `PractitionerRole.ndjson` or `PractitionerRole.*.ndjson` → PractitionerRole nodes

**Examples:**
- `Practitioner.Wyoming.ndjson` → Practitioner
- `Organization.Hospitals.ndjson` → Organization
- `PractitionerRole.Active.ndjson` → PractitionerRole

**ERRORS - Confusing patterns:**
- `Practitioner.Role.ndjson` → ERROR (ambiguous - use `PractitionerRole.*.ndjson`)
- `Organization.Affiliation.ndjson` → ERROR (ambiguous - use `OrganizationAffiliation.*.ndjson`)

### Idempotent Imports

Imports use `MERGE` operations based on FHIR resource IDs, so they can be rerun safely without creating duplicates.

### Data Inspection

Before importing, the script inspects the first 10 lines of each NDJSON file to understand the actual field structure and adapt the import logic accordingly.

## Schema Design

### Node Types

Each FHIR resource becomes a node with:

- Label matching the resource type
- `fhir_id` property (the FHIR resource.id)
- `resource_type` property
- Key FHIR fields as properties
- Extracted identifiers (including NPIs)

### Indexes

All indexes are defined in `schema/indexes.cypher`:

- FHIR resource IDs
- Identifier values and systems
- NPI values (special index for quick lookup)

### Relationships

Relationships are created from FHIR references:

- `HAS_ROLE`: Practitioner → PractitionerRole
- `WORKS_AT`: PractitionerRole → Organization
- `AFFILIATED_WITH`: Organization → OrganizationAffiliation
- `HAS_ENDPOINT`: Organization/PractitionerRole → Endpoint
- `AT_LOCATION`: Organization/PractitionerRole → Location

## Maintenance

### Stop Neo4j

```bash
docker-compose down
```

### Reset Database

```bash
# WARNING: This deletes all data
docker-compose down -v
rm -rf data/ logs/
docker-compose up -d
```

### View Logs

```bash
docker-compose logs -f neo4j
```

### Backup Data

```bash
# Stop Neo4j first
docker-compose down

# Backup data directory
tar -czf neo4j_backup_$(date +%Y%m%d).tar.gz data/

# Restart
docker-compose up -d
```

## Example Queries

See `docs/CYPHER_QUERIES.md` for comprehensive query examples.

## Troubleshooting

### "Authentication failed"

Check that your `.env` file has the correct password and matches what you set in Docker.

### "Cannot connect to Neo4j"

Ensure Docker is running and Neo4j container is up:

```bash
docker-compose ps
docker-compose logs neo4j
```

### Import errors

Check that NDJSON files are valid (one JSON object per line) and match expected resource types.

## Project Isolation

This subproject is completely isolated from the main FHIRTableSaw project:

- Separate Docker environment
- Separate Python dependencies
- No impact on PostgreSQL database
- Can be removed without affecting main project

## License

Follows the license of the parent FHIRTableSaw project.
