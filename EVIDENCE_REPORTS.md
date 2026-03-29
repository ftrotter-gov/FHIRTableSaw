# Evidence Reports for FHIRTableSaw

This project uses [Evidence](https://evidence.dev/) to generate data-driven
reports from the FHIRTableSaw PostgreSQL and DuckDB data sources. Reports are
built as static pages and published to GitHub Pages.

## Directory Layout

```text
evidence/
├── Dockerfile                 # Multi-stage Docker build
├── docker-compose.yml         # Dev server + static build services
├── package.json               # Node dependencies (Evidence, PG, DuckDB)
├── package-lock.json          # Lockfile for reproducible installs
├── evidence.config.yaml       # Evidence theme and plugin config
├── .npmrc                     # npm configuration (legacy-peer-deps)
├── .gitignore                 # Evidence-specific ignores
├── .evidence/
│   └── customization/
│       └── custom-formatting.json
├── pages/
│   └── index.md               # Main report page (endpoint validation)
├── sources/
│   ├── postgres/
│   │   ├── connection.yaml    # PG source definition (creds via env vars)
│   │   └── endpoint_summary.sql
│   └── duckdb_local/
│       └── connection.yaml    # DuckDB source (file-based)
└── scripts/
    └── build_static.sh        # Helper script for native static builds
```

## Prerequisites

- **Docker** and **Docker Compose** (recommended), OR
- **Node.js >= 18** and **npm >= 7** (for native usage)

## Quick Start with Docker

### 1. Configure PostgreSQL Credentials

Evidence reads PostgreSQL connection details from environment variables.
The `docker-compose.yml` maps from the project's existing `.env` variables
(`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_DATABASE`, `DB_PORT`) to
Evidence's `EVIDENCE_SOURCE__postgres__*` format. **The `.env` file is
never modified by this setup.**

If your `.env` already contains these variables (which it should for the
FHIRTableSaw project), no additional configuration is needed:

```bash
# These variables in .env are used automatically:
DB_HOST=your-pg-host
DB_PORT=5432
DB_DATABASE=postgres
DB_USER=postgres
DB_PASSWORD=your-password
```

Alternatively, you can export Evidence environment variables directly:

```bash
export EVIDENCE_SOURCE__postgres__host=your-pg-host
export EVIDENCE_SOURCE__postgres__port=5432
export EVIDENCE_SOURCE__postgres__database=postgres
export EVIDENCE_SOURCE__postgres__user=postgres
export EVIDENCE_SOURCE__postgres__password=your-password
export EVIDENCE_SOURCE__postgres__ssl=no-verify
```

### 2. Start the Development Server

```bash
cd evidence
docker compose up evidence-dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

The dev server supports hot-reload: edit markdown files in `evidence/pages/`
and see changes immediately.

### 3. Build Static Site for GitHub Pages

```bash
cd evidence
docker compose run evidence-build
```

The built static files will be in `evidence/build/FHIRTableSaw/`.

## Quick Start Without Docker

```bash
cd evidence
npm install

# Set PostgreSQL credentials
export EVIDENCE_SOURCE__postgres__host=your-pg-host
export EVIDENCE_SOURCE__postgres__port=5432
export EVIDENCE_SOURCE__postgres__database=postgres
export EVIDENCE_SOURCE__postgres__user=postgres
export EVIDENCE_SOURCE__postgres__password=your-password
export EVIDENCE_SOURCE__postgres__ssl=no-verify

# Run the dev server
npm run dev

# Or build the static site
bash scripts/build_static.sh
```

## DuckDB Data Source

The DuckDB source is configured to read a file named `fhirtablesaw.duckdb`
from the `sources/duckdb_local/` directory.

**To use DuckDB data:**

1. Copy or symlink your `.duckdb` file into `evidence/sources/duckdb_local/`:

   ```bash
   ln -s /path/to/your/data.duckdb evidence/sources/duckdb_local/fhirtablesaw.duckdb
   ```

2. When using Docker, mount the file as a volume in `docker-compose.yml`:

   ```yaml
   volumes:
     - /path/to/your/data.duckdb:/app/sources/duckdb_local/fhirtablesaw.duckdb:ro
   ```

3. Add `.sql` query files to `evidence/sources/duckdb_local/` to extract data.
   Each file creates a queryable table named `duckdb_local.<filename>`.

## Adding New Report Pages

Create new `.md` files in `evidence/pages/`. Evidence uses markdown with
embedded SQL and components:

````markdown
---
title: My New Report
---

# My Report Title

```sql my_query
SELECT column_a, column_b
FROM postgres.my_source_query
```

<DataTable data={my_query} />
````

Source queries (`.sql` files in `sources/<source_name>/`) use the
native SQL dialect of that database. Page queries reference extracted data
using `<source_name>.<query_file_name>` syntax.

## GitHub Pages Deployment

### Automated (GitHub Actions)

The workflow at `.github/workflows/evidence-gh-pages.yml` automatically
builds and deploys when changes are pushed to `main` in the `evidence/`
directory.

**Setup steps:**

1. Go to your GitHub repo → **Settings** → **Pages**
2. Under **Source**, select **GitHub Actions**
3. Go to **Settings** → **Secrets and variables** → **Actions**
4. Add these repository secrets:

   | Secret Name | Value |
   |---|---|
   | `EVIDENCE_SOURCE__postgres__host` | Your PostgreSQL host |
   | `EVIDENCE_SOURCE__postgres__port` | `5432` |
   | `EVIDENCE_SOURCE__postgres__database` | `postgres` |
   | `EVIDENCE_SOURCE__postgres__user` | `postgres` |
   | `EVIDENCE_SOURCE__postgres__password` | Your password |
   | `EVIDENCE_SOURCE__postgres__ssl` | `no-verify` (optional) |

5. Push changes to `main` or trigger the workflow manually

Reports will be available at:
`https://<username>.github.io/FHIRTableSaw/`

### Manual

```bash
cd evidence
bash scripts/build_static.sh
# Upload evidence/build/FHIRTableSaw/ to your web server
```

## Validation Query

The installation includes a validation query that confirms the PostgreSQL
connection is working. The query runs against `fhirtablesaw.endpoint` and
returns one row with three columns:

```sql
SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM fhirtablesaw.endpoint
```

This is displayed on the main report page at `/` with a `DataTable` and
`BigValue` components.

## Troubleshooting

### PostgreSQL connection fails

- Verify your credentials are set correctly in environment variables
- Ensure the PostgreSQL server is accessible from the Docker container
  (use `network_mode: host` in docker-compose.yml for local PG servers)
- Check that the `fhirtablesaw` schema and `endpoint` table exist

### DuckDB file not found

- Ensure the `.duckdb` file is placed in `evidence/sources/duckdb_local/`
- When using Docker, verify the volume mount path is correct
- The file must be named `fhirtablesaw.duckdb` (or update `connection.yaml`)

### Build fails with memory error

```bash
NODE_OPTIONS="--max-old-space-size=4096" npm run sources
```

### Port 3000 already in use

```bash
docker compose up evidence-dev --build -d
# Or change the port mapping in docker-compose.yml
```

### npm peer dependency errors

The `.npmrc` includes `legacy-peer-deps=true` to resolve TypeScript
version conflicts with Evidence's Svelte dependencies. This is set
automatically and should not need manual intervention.
