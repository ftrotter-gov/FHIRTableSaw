# FHIR Table Saw

## About

This project intelligently flattens an existing FHIR instance into a lossy flat relational table structure based on some rules about how to normalize. When there is data that is not covered in the flattening map, it is ignored.

FHIRTableSaw uses SQL-on-FHIR ViewDefinitions to transform FHIR resources into tabular formats, processing data through a high-performance DuckDB pipeline that's 31x faster than traditional approaches.

## Quick Start

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure environment**: Copy `env.example` to `.env` and configure your settings
3. **Load FHIR data**: `python scripts/go_fast.py /path/to/ndjson/directory`
4. **Query results**: Use DuckDB files directly or upload to PostgreSQL

## Documentation

Detailed documentation is available in the [docs/](docs/) directory:

- **[Quick Start Guides](docs/)** - Get started quickly with SQL-on-FHIR and data loading
- **[Pipeline Guide](docs/GO_FAST_ENV_CONFIG.md)** - Configure and run the fast DuckDB pipeline
- **[DuckDB Queries](docs/DUCKDB_QUERY_GUIDE.md)** - Query DuckDB files created by the pipeline
- **[Evidence Reports](docs/EVIDENCE_REPORTS.md)** - Generate data reports and dashboards
- **[Multi-Source Setup](docs/MULTI_SOURCE_SETUP.md)** - Work with multiple data sources

See the [docs/README.md](docs/README.md) for a complete documentation index.

## Policies

### Open Source Policy

We adhere to the [CMS Open Source Policy](https://github.com/CMSGov/cms-open-source-policy). If you have any questions, just [shoot us an email](mailto:opensource@cms.hhs.gov).

### Security and Responsible Disclosure Policy

_Submit a vulnerability:_ Vulnerability reports can be submitted through [Bugcrowd](https://bugcrowd.com/cms-vdp). Reports may be submitted anonymously. If you share contact information, we will acknowledge receipt of your report within 3 business days.



## Public domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/) as indicated in [LICENSE](LICENSE).

All contributions to this project will be released under the CC0 dedication. By submitting a pull request or issue, you are agreeing to comply with this waiver of copyright interest.
