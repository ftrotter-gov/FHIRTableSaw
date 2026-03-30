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
```

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
