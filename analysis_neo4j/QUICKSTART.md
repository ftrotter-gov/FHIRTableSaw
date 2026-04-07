# Quick Start Guide

Get up and running with Neo4j FHIR analysis in 5 minutes.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+
- FHIR NDJSON files (Practitioner, Organization, etc.)

## Step-by-Step Setup

### 1. Configure Environment

```bash
cd analysis_neo4j
cp .env.example .env
```

Edit `.env` and set a secure password:

```bash
NEO4J_PASSWORD=your_secure_password_here
```

### 2. Start Neo4j

```bash
docker-compose up -d
```

Wait 30 seconds for Neo4j to start, then verify:

```bash
docker-compose logs neo4j
```

You should see "Started" in the logs.

### 3. Create Indexes

```bash
cat schema/indexes.cypher | docker exec -i fhir_neo4j_analysis cypher-shell -u neo4j -p your_secure_password_here
```

### 4. Install Python Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Import Your Data

```bash
# Set password for import script
export NEO4J_PASSWORD=your_secure_password_here

# Import from your NDJSON directory
python scripts/import_ndjson.py /path/to/your/ndjson/files
```

### 6. Explore in Neo4j Browser

Open <http://localhost:7474> in your browser.

Login with:
- Username: `neo4j`
- Password: (the password you set)

Try this query:

```cypher
MATCH (n) RETURN labels(n) AS type, count(n) AS count ORDER BY count DESC;
```

## Common Commands

### Stop Neo4j

```bash
docker-compose down
```

### View Logs

```bash
docker-compose logs -f neo4j
```

### Reset Database (WARNING: Deletes all data)

```bash
docker-compose down -v
rm -rf data/ logs/
docker-compose up -d
# Re-run schema creation (step 3 above)
```

### Inspect NDJSON Before Import

```bash
python scripts/inspect_ndjson.py /path/to/Practitioner.ndjson 10
```

## Troubleshooting

### "Authentication failed"

- Check that `.env` password matches what you're using
- Try: `docker-compose restart neo4j`

### "Cannot connect to Neo4j"

- Verify Docker is running: `docker ps`
- Check Neo4j status: `docker-compose ps`
- Wait longer - Neo4j can take 30-60 seconds to start

### "No NDJSON files found"

Files must be named exactly:
- `Practitioner.ndjson`
- `PractitionerRole.ndjson`
- `Organization.ndjson`
- `OrganizationAffiliation.ndjson`
- `Endpoint.ndjson`
- `Location.ndjson`

## Next Steps

- See [README.md](README.md) for detailed documentation
- See [docs/CYPHER_QUERIES.md](docs/CYPHER_QUERIES.md) for query examples
- Explore the Neo4j Browser at <http://localhost:7474>

## Key Features

✓ **Exact Filename Matching** - OrganizationAffiliation won't be confused with Organization

✓ **Idempotent Imports** - Safe to rerun without creating duplicates

✓ **NPI Indexing** - Fast lookups by NPI for practitioners and organizations

✓ **Relationship Mapping** - Automatic creation of practitioner-organization networks

✓ **Isolated Subproject** - Won't affect your main PostgreSQL database
