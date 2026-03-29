---
title: FHIRTableSaw Reports
---

# FHIRTableSaw Data Reports

This dashboard provides data quality and summary reports for the FHIRTableSaw project.
Data is sourced from PostgreSQL (the primary relational store) and optionally from
local DuckDB files.

## Endpoint Summary

The following table shows summary statistics for the `endpoint` table in PostgreSQL.

```sql endpoint_summary
SELECT
    uuid_count,
    address_count,
    row_count
FROM postgres.endpoint_summary
```

<DataTable data={endpoint_summary} />

<BigValue
    data={endpoint_summary}
    value=uuid_count
    title="Distinct UUIDs"
/>

<BigValue
    data={endpoint_summary}
    value=address_count
    title="Distinct Addresses"
/>

<BigValue
    data={endpoint_summary}
    value=row_count
    title="Total Rows"
/>

---

## About This Dashboard

- **PostgreSQL source**: Connects to the FHIRTableSaw PostgreSQL database
- **DuckDB source**: Available for local DuckDB `.db` files when mounted
- **Deployment**: Static pages published to GitHub Pages

See [EVIDENCE_REPORTS.md](https://github.com/ftrotter-gov/FHIRTableSaw/blob/main/EVIDENCE_REPORTS.md)
for setup and usage instructions.
