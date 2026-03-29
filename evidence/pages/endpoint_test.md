---
title: Endpoint Test Query
---

# Endpoint Test Query

Testing the query format with `postgres.fhirtablesaw.endpoint` reference.

## Query Being Tested

```sql
SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM postgres.fhirtablesaw.endpoint
```

## Results

```sql endpoint_test
SELECT
    uuid_count,
    address_count,
    row_count
FROM postgres.endpoint_test
```

<DataTable data={endpoint_test} />

### Key Metrics

<BigValue
    data={endpoint_test}
    value=uuid_count
    title="Distinct Resource UUIDs"
    fmt='#,##0'
/>

<BigValue
    data={endpoint_test}
    value=address_count
    title="Distinct Addresses"
    fmt='#,##0'
/>

<BigValue
    data={endpoint_test}
    value=row_count
    title="Total Row Count"
    fmt='#,##0'
/>

## Data Quality Insights

This query helps validate:
- **UUID uniqueness**: How many unique resources exist
- **Address uniqueness**: How many distinct endpoint addresses are configured
- **Total rows**: The overall size of the endpoint table

---

[Back to Main Dashboard](/)
