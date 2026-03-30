# Evidence Reports - Index of Indexes Structure

This directory contains Evidence.dev reports following an **Index of Indexes** pattern.

## Directory Structure

```
evidence/
├── sources/
│   └── postgres/
│       └── {section}_{report_name}.sql    # SQL source files
├── pages/
│   ├── index.md                           # Main index (lists sections)
│   └── {section}/
│       ├── index.md                       # Section index (lists reports)
│       └── {report_name}.md               # Individual report pages
└── static_site_content/                   # Built static site
```

## Naming Conventions

### SQL Files
- **Pattern:** `{section}_{report_name}.sql`
- **Location:** `evidence/sources/postgres/`
- **Example:** `endpoint_summary_stats.sql`

### Page Files
- **Section Index:** `evidence/pages/{section}/index.md`
- **Report Page:** `evidence/pages/{section}/{report_name}.md`
- **Example:** `evidence/pages/endpoint/summary_stats.md`

### Query References
- **In SQL blocks:** `{report_name}_data`
- **Table reference:** `postgres.{section}_{report_name}`

## Current Structure

### Sections
- **endpoint** - FHIR Endpoint resource reports

### Reports
- **endpoint/summary_stats** - Overview of endpoint data volume and quality

## Creating New Reports

See detailed instructions in: `/AI_Instructions/MakeNewEvidenceReport.md`

### Quick Start

1. **Create SQL file:**
   ```bash
   evidence/sources/postgres/{section}_{report_name}.sql
   ```

2. **Create report page:**
   ```bash
   evidence/pages/{section}/{report_name}.md
   ```

3. **Update section index:**
   ```bash
   evidence/pages/{section}/index.md
   ```

4. **Build and test:**
   ```bash
   cd evidence
   npm run sources  # Extract data
   npm run dev      # Test locally
   npm run build    # Build for production
   ```

## Building Reports

### Development
```bash
cd evidence
npm run dev
# Visit http://localhost:3000
```

### Production Build
```bash
./build_reports.sh
# Or manually:
cd evidence
npm run sources
npm run build
```

### View Static Site
```bash
cd evidence/static_site_content
python3 -m http.server 8000
# Visit http://localhost:8000
```

## Key Features

- **SQL Display:** Every report includes a collapsible `<Details>` section showing the source SQL
- **Standardized Comments:** All SQL files have standard headers (Section, Report, Description, Returns)
- **BigValue Metrics:** Used for standalone summary cards
- **DataTable:** Always included for full data view
- **Hierarchical Navigation:** Easy navigation from reports → section → main index

## Documentation

- **Creating Reports:** `/AI_Instructions/MakeNewEvidenceReport.md`
- **Evidence.dev Docs:** https://docs.evidence.dev/
- **Project Docs:** `/docs/EVIDENCE_REPORTS.md`
