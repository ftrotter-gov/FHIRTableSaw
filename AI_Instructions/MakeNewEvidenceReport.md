# Make a New Evidence Report

Complete guide for creating Evidence.dev reports in the FHIRTableSaw project.

## Overview

Evidence reports follow an **Index of Indexes** pattern:

- **Main Index** (`evidence/pages/index.md`) - Lists all report sections
- **Section Indexes** (`evidence/pages/{section}/index.md`) - Lists reports in each section
- **Report Pages** (`evidence/pages/{section}/{report_name}.md`) - Individual reports

## Quick Start Checklist

1. Create SQL source file in `evidence/sources/postgres/`
2. Create report page in `evidence/pages/{section}/`
3. Update section index at `evidence/pages/{section}/index.md`
4. Update main index at `evidence/pages/index.md` (if new section)

---

## Naming Conventions

### SQL Source Files

**Location:** `evidence/sources/postgres/`

**Naming Pattern:** `{section}_{report_name}.sql`

**Examples:**
- `endpoint_summary_stats.sql`
- `endpoint_address_distribution.sql`
- `practitioner_credential_coverage.sql`
- `location_geographic_analysis.sql`

**Rules:**
- Use lowercase with underscores
- First part = section name
- Second part = descriptive report name
- Must end with `.sql`

### Page Files

**Location:** `evidence/pages/{section}/`

**Naming Pattern:** `{report_name}.md`

**Examples:**
- `evidence/pages/endpoint/summary_stats.md`
- `evidence/pages/endpoint/address_distribution.md`
- `evidence/pages/practitioner/credential_coverage.md`
- `evidence/pages/location/geographic_analysis.md`

**Rules:**
- Folder name = section name (lowercase, underscores)
- File name = report name only (lowercase, underscores)
- Must end with `.md`

### Query Names (in Page Files)

**Pattern:** `{report_name}_data`

**Examples:**
```sql summary_stats_data
SELECT * FROM postgres.endpoint_summary_stats
```

```sql address_distribution_data
SELECT * FROM postgres.endpoint_address_distribution
```

**Rules:**
- Match the report file name
- Add `_data` suffix for clarity
- Use in SQL code blocks and component references

### Table References

**Pattern:** `{source_name}.{sql_filename_without_extension}`

**Examples:**
- `postgres.endpoint_summary_stats` → refers to `endpoint_summary_stats.sql`
- `postgres.practitioner_credential_coverage` → refers to `practitioner_credential_coverage.sql`

**Key Points:**
- `postgres` = source name (defined in `evidence/sources/postgres/connection.yaml`)
- SQL filename (minus `.sql`) becomes the table name
- Evidence automatically materializes these as queryable tables

---

## SQL Source File Structure

### Required Comment Header

Every SQL file must start with a standard comment block:

```sql
-- Section: {section_name}
-- Report: {report_name}
-- Description: {Brief description of what this query returns}
-- Returns: {Number of rows} row(s) with {column count} columns: {column names}
```

**Example:**

```sql
-- Section: endpoint
-- Report: summary_stats
-- Description: Summary statistics for the endpoint table
-- Returns: 1 row with 3 columns: uuid_count, address_count, row_count

SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM fhirtablesaw.endpoint
ORDER BY row_count
```

### SQL Best Practices

**Core Rules:**
- Each file contains ONE SELECT statement
- No DDL (CREATE, ALTER, DROP)
- No DML (INSERT, UPDATE, DELETE)
- Pure SELECT queries only
- Output is automatically materialized as a table

**Database Schema:**
- Use `fhirtablesaw` schema: `FROM fhirtablesaw.{table_name}`
- For FHIR resources: `fhirtablesaw.endpoint`, `fhirtablesaw.practitioner`, etc.

**Advanced SQL Features Supported:**
- CTEs (WITH clauses)
- Window functions
- Joins (including complex multi-table joins)
- Subqueries
- Aggregate functions
- PostgreSQL-specific functions

**Performance Tips:**
- Filter early in CTEs
- Project only needed columns
- Aggregate before joining when possible
- Add ORDER BY for better compression
- Avoid SELECT * unless truly needed

### SQL Template

```sql
-- Section: {section}
-- Report: {report_name}
-- Description: {what this does}
-- Returns: {row/column description}

WITH base AS (
    SELECT
        column1,
        column2,
        column3
    FROM fhirtablesaw.{table_name}
    WHERE {filter_conditions}
),
aggregated AS (
    SELECT
        column1,
        COUNT(*) AS count_col,
        SUM(column2) AS sum_col
    FROM base
    GROUP BY column1
)
SELECT *
FROM aggregated
ORDER BY count_col DESC
```

---

## Report Page Structure

### Page Template

```markdown
---
title: {Report Title}
---

# {Report Title}

{Brief description of what this report shows}

```sql {report_name}_data
SELECT *
FROM postgres.{section}_{report_name}
```

<Details title="Show SQL Source">

\`\`\`sql
-- Section: {section}
-- Report: {report_name}
-- Description: {description}
-- Returns: {return description}

{Paste the complete SQL from the source file here}
\`\`\`

</Details>

## Summary Metrics

<BigValue
    data={{report_name}_data}
    value=metric_column_1
    title="Metric 1 Title"
/>

<BigValue
    data={{report_name}_data}
    value=metric_column_2
    title="Metric 2 Title"
/>

## Detailed Data

<DataTable data={{report_name}_data} />

## Additional Visualizations

{Add charts as needed - see Component Reference below}
```

### Real Example

```markdown
---
title: Endpoint Summary Statistics
---

# Endpoint Summary Statistics

Overview of endpoint data quality and volume metrics.

```sql summary_stats_data
SELECT *
FROM postgres.endpoint_summary_stats
```

<Details title="Show SQL Source">

\`\`\`sql
-- Section: endpoint
-- Report: summary_stats
-- Description: Summary statistics for the endpoint table
-- Returns: 1 row with 3 columns: uuid_count, address_count, row_count

SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM fhirtablesaw.endpoint
ORDER BY row_count
\`\`\`

</Details>

## Summary Metrics

<BigValue
    data={summary_stats_data}
    value=uuid_count
    title="Distinct UUIDs"
/>

<BigValue
    data={summary_stats_data}
    value=address_count
    title="Distinct Addresses"
/>

<BigValue
    data={summary_stats_data}
    value=row_count
    title="Total Rows"
/>

## Detailed Data

<DataTable data={summary_stats_data} />
```

---

## Component Reference

### DataTable (Always Include)

Display full tabular data with sorting and filtering.

**Basic Usage:**

```markdown
<DataTable data={query_name} />
```

**With Custom Columns:**

```markdown
<DataTable data={query_name}>
  <Column id=column1 />
  <Column id=column2 title="Custom Title" />
  <Column id=count_col fmt=num0 />
  <Column id=percent_col fmt=pct1 />
</DataTable>
```

**Format Options:**
- `num0` - Integer (no decimals)
- `num1` - One decimal place
- `num2` - Two decimal places
- `pct0` - Percentage, no decimals
- `pct1` - Percentage, one decimal
- `usd` - US Dollar format

### BigValue (For Key Metrics)

Display standalone metric cards on dashboards.

**Usage:**

```markdown
<BigValue
    data={query_name}
    value=column_name
    title="Display Title"
/>
```

**When to Use:**
- Dashboard summary cards
- Key performance indicators
- Standalone metrics at top of page

### Value (For Inline Metrics)

Insert metric values within text paragraphs.

**Usage:**

```markdown
The total count is <Value data={query_name} column=count_col />.
```

**When to Use:**
- Inline text references
- Narrative reports
- Contextual metrics within paragraphs

### LineChart

Display trends over time or continuous data.

**Basic Usage:**

```markdown
```sql monthly_data
SELECT
    service_month,
    claim_count
FROM postgres.claims_monthly
ORDER BY service_month
```

<LineChart
    data={monthly_data}
    x=service_month
    y=claim_count
/>
```

**Multiple Series:**

```markdown
```sql monthly_by_type
SELECT
    service_month,
    taxonomy_code,
    claim_count
FROM postgres.claims_monthly_by_type
ORDER BY service_month
```

<LineChart
    data={monthly_by_type}
    x=service_month
    y=claim_count
    series=taxonomy_code
/>
```

### BarChart

Display comparisons across categories.

**Basic Usage:**

```markdown
```sql top_states
SELECT
    state,
    provider_count
FROM postgres.provider_counts_by_state
ORDER BY provider_count DESC
LIMIT 10
```

<BarChart
    data={top_states}
    x=state
    y=provider_count
/>
```

**Multiple Series (Grouped Bars):**

```markdown
<BarChart
    data={category_data}
    x=category
    y=value
    series=type
/>
```

### ScatterPlot

Show relationships between two numeric variables.

**Usage:**

```markdown
```sql scatter_data
SELECT
    provider_count,
    total_allowed,
    state
FROM postgres.state_summary
```

<ScatterPlot
    data={scatter_data}
    x=provider_count
    y=total_allowed
    series=state
/>
```

### Details (Show/Hide SQL)

Collapsible section for SQL source code display.

**Usage:**

```markdown
<Details title="Show SQL Source">

\`\`\`sql
-- Your SQL query here
SELECT * FROM fhirtablesaw.example
\`\`\`

</Details>
```

**Standard Pattern:**
- Always include on report pages
- Title should be "Show SQL Source"
- Include the complete SQL with comments
- Use triple backticks with `sql` language marker

---

## Section Index Structure

Each section needs an index page listing its reports.

**Location:** `evidence/pages/{section}/index.md`

**Template:**

```markdown
---
title: {Section Name} Reports
---

# {Section Name} Reports

{Brief description of this section}

## Available Reports

- [Report 1 Title](./{report_name_1}) - Brief description
- [Report 2 Title](./{report_name_2}) - Brief description
- [Report 3 Title](./{report_name_3}) - Brief description

---

[← Back to Main Index](../)
```

**Example:**

```markdown
---
title: Endpoint Reports
---

# Endpoint Reports

Data quality and analysis reports for FHIR Endpoint resources.

## Available Reports

- [Summary Statistics](./summary_stats) - Overview of endpoint data volume and quality
- [Address Distribution](./address_distribution) - Geographic distribution of endpoint addresses
- [Connection Type Analysis](./connection_types) - Breakdown of endpoint connection types

---

[← Back to Main Index](../)
```

---

## Main Index Structure

The main index lists all report sections.

**Location:** `evidence/pages/index.md`

**Template:**

```markdown
---
title: FHIRTableSaw Reports
---

# FHIRTableSaw Data Reports

Comprehensive data quality and analysis reports for FHIRTableSaw project.

Data is sourced from PostgreSQL (primary relational store) and optionally from local DuckDB files.

## Report Sections

### [{Section 1 Name}](./{section_1}/)

{Brief description of section 1}

### [{Section 2 Name}](./{section_2}/)

{Brief description of section 2}

### [{Section 3 Name}](./{section_3}/)

{Brief description of section 3}

---

## About This Dashboard

- **PostgreSQL source**: Connects to the FHIRTableSaw PostgreSQL database
- **DuckDB source**: Available for local DuckDB `.db` files when mounted
- **Deployment**: Static pages published to GitHub Pages

See [EVIDENCE_REPORTS.md](https://github.com/ftrotter-gov/FHIRTableSaw/blob/main/docs/EVIDENCE_REPORTS.md)
for setup and usage instructions.
```

---

## Step-by-Step Workflow

### Creating a New Report in Existing Section

1. **Create SQL file**
   - Location: `evidence/sources/postgres/{section}_{report_name}.sql`
   - Add standard comment header
   - Write SELECT query

2. **Create page file**
   - Location: `evidence/pages/{section}/{report_name}.md`
   - Use page template
   - Add query referencing `postgres.{section}_{report_name}`
   - Include Details block with SQL source

3. **Update section index**
   - Edit `evidence/pages/{section}/index.md`
   - Add link to new report

4. **Test**
   - Run `npm run dev` in evidence directory
   - Verify report displays correctly

### Creating a New Section

1. **Create section directory**
   - Create `evidence/pages/{section}/`

2. **Create section index**
   - Create `evidence/pages/{section}/index.md`
   - Use section index template

3. **Create first report** (follow steps above)

4. **Update main index**
   - Edit `evidence/pages/index.md`
   - Add section link and description

---

## Common Patterns

### Summary Report (1 Row)

Use BigValue components for metrics:

```markdown
<BigValue data={query_name} value=metric1 title="Metric 1" />
<BigValue data={query_name} value=metric2 title="Metric 2" />
```

### Distribution Report (Multiple Rows)

Use DataTable + BarChart:

```markdown
<DataTable data={query_name} />

<BarChart data={query_name} x=category y=count />
```

### Time Series Report

Use LineChart:

```markdown
<LineChart data={query_name} x=date y=value />
```

### Comparison Report (Multiple Series)

Use grouped BarChart or multi-series LineChart:

```markdown
<BarChart data={query_name} x=category y=value series=type />
```

---

## Troubleshooting

### Query Returns No Data

- Check SQL file has valid SELECT
- Verify table name in `FROM postgres.{section}_{report_name}`
- Ensure SQL file is in `evidence/sources/postgres/`

### Chart Not Displaying

- Verify data reference matches query name
- Check column names match SQL output
- Ensure data has rows (charts need data to render)

### SQL Source Not Showing

- Verify Details component syntax
- Check triple backticks are properly escaped
- Ensure SQL is inside Details block

---

## Quick Reference Card

| Element | Pattern |
|---------|---------|
| **SQL File** | `evidence/sources/postgres/{section}_{report}.sql` |
| **Page File** | `evidence/pages/{section}/{report}.md` |
| **Section Index** | `evidence/pages/{section}/index.md` |
| **Query Name** | `{report}_data` |
| **Table Reference** | `postgres.{section}_{report}` |
| **SQL Comment** | `-- Section: / Report: / Description: / Returns:` |
| **Show SQL** | `<Details title="Show SQL Source">` |

---

## Additional Resources

- [Evidence.dev Documentation](https://docs.evidence.dev/)
- [Evidence Components Reference](https://docs.evidence.dev/components/all-components)
- [SQL Styling Guide](https://docs.evidence.dev/core-concepts/syntax)
- [FHIRTableSaw Evidence Reports](../docs/EVIDENCE_REPORTS.md)
