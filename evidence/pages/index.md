---
title: FHIRTableSaw Reports
---

# FHIRTableSaw Data Reports

Comprehensive data quality and analysis reports for FHIRTableSaw project.

Data is sourced from PostgreSQL (primary relational store) and optionally from local DuckDB files.

## Report Sections

### [Endpoint Reports](./endpoint/)

Data quality and analysis reports for FHIR Endpoint resources.

### [NPPES Taxonomy Analysis](./nppes_taxonomy_analysis/)

Statistical analysis of healthcare provider taxonomies from the National Plan and Provider Enumeration System (NPPES) database.

---

## About This Dashboard

- **PostgreSQL source**: Connects to the FHIRTableSaw PostgreSQL database
- **DuckDB source**: Available for local DuckDB `.db` files when mounted
- **Deployment**: Static pages published to GitHub Pages

See [EVIDENCE_REPORTS.md](https://github.com/ftrotter-gov/FHIRTableSaw/blob/main/docs/EVIDENCE_REPORTS.md)
for setup and usage instructions.
