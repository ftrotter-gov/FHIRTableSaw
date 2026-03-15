# NDH Slurper (FHIRTableSaw 3-tier)

This repo now includes a **simple ingestion (“slurp”) pipeline** that can pull data from an **unauthenticated NDH-style FHIR R4 server** and write it into a relational database using the 3-tier architecture.

The ingestion path is:

```
FHIR server (search Bundles) -> canonical domain models -> SQLAlchemy tables
```

## What resources are currently supported

The slurper only ingests the slices that are implemented in `src/fhir_tablesaw_3tier`:

1. **Practitioner**
2. **Organization** (only those that successfully parse as **ClinicalOrganization**, i.e. type `prov` + has NPI)
3. **PractitionerRole**
4. **OrganizationAffiliation**

Notes:
- The code is designed to be extended as additional profiles are implemented.
- **Non-clinical Organization types** will typically be skipped for now.

## How paging works

FHIR servers return search results as a **Bundle**. The slurper:

1. Calls `GET /<ResourceType>?_count=<N>`
2. Iterates `Bundle.entry[].resource`
3. Follows `Bundle.link[]` where `relation == "next"` until there is no `next` link

Implementation is in:

- `src/fhir_tablesaw_3tier/ndh_slurp.py`

## How database persistence works

The slurper uses:

- `Base.metadata.create_all(engine)` to create tables
- per-resource persistence functions, e.g. `save_practitioner(...)`, `save_practitioner_role(...)`

### Will it automatically create missing tables?

Yes **as long as you do not pass** `--no-create-schema`.

By default, the `slurp-ndh` command calls:

```python
Base.metadata.create_all(engine)
```

That will **create any missing tables** that are defined in `src/fhir_tablesaw_3tier/db/models.py`.

Important limitation:
- `create_all()` is **not a migration system**. If you already have tables with an older schema, it will **not** automatically alter them to match new columns/indexes.

## Dropped repeats report

FHIR often contains repeating arrays that we intentionally flatten in CMS practice.

When ingestion encounters repeats beyond the “kept” shape, it:

- **drops** the extra values
- prints **warnings** during parsing
- accumulates a **summary counter** and prints it after slurp completes

Example paths that might be counted:

- `PractitionerRole.telecom`
- `PractitionerRole.location`
- `Organization.contact`

## Quick start

### 1) Install

From repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2) Configure environment

Copy the example file and edit:

```bash
cp env.example .env
```

At minimum you must set:

- `DATABASE_URL`

Optional:

- `DB_SCHEMA` (defaults to `fhir_tablesaw`)

### 3) Run slurp

```bash
fhir-tablesaw-3tier slurp-ndh --fhir-server-url "$FHIR_SERVER_URL"
```

`slurp-ndh` reads DB configuration from `.env`:

- `DATABASE_URL`
- `DB_SCHEMA` (optional; defaults to `fhir_tablesaw`)

Optional debugging flags:

```bash
fhir-tablesaw-3tier slurp-ndh --fhir-server-url "$FHIR_SERVER_URL" --hard-limit 100
```

If you want to manage schema yourself:

```bash
fhir-tablesaw-3tier slurp-ndh --fhir-server-url "$FHIR_SERVER_URL" --no-create-schema
```

## Wipe and rebuild the database schema (no migrations)

Because we are not using Alembic migrations, the supported workflow is to
**wipe and rebuild** your schema.

### Option A: Reset via CLI (drop_all + create_all)

This will **delete all ingested data** and recreate all tables defined in
`src/fhir_tablesaw_3tier/db/models.py`:

```bash
fhir-tablesaw-3tier reset-db
```

Then re-ingest:

```bash
fhir-tablesaw-3tier slurp-ndh --fhir-server-url "$FHIR_SERVER_URL"
```

### Option B: Reset at the Postgres level (drop schema)

If you prefer using Postgres tooling, you can also drop the whole schema/database
and recreate it, then run `slurp-ndh`.

Example (DANGEROUS; wipes everything in the schema):

```sql
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
```

After that, run `slurp-ndh` (which will call `create_all()` by default).

## Operational notes / current limitations

- PractitionerRole persistence currently **requires the Practitioner** to exist in the DB (it will skip roles whose practitioner isn’t present).
- OrganizationAffiliation will create base `organization` registry rows for referenced organizations if missing (so the bridge can be represented), but subtype data may still be missing.
- Organization ingestion currently attempts to parse as `ClinicalOrganization` and will skip organizations that don’t meet that profile-in-practice.

## Code pointers

- Slurp runner: `src/fhir_tablesaw_3tier/ndh_slurp.py`
- CLI command: `src/fhir_tablesaw_3tier/cli.py` (`slurp-ndh`)
- DB models: `src/fhir_tablesaw_3tier/db/models.py`
- Persistence modules: `src/fhir_tablesaw_3tier/db/persist_*.py`
