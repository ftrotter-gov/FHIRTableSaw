-- Test query for endpoint table validation
-- Testing the exact query format: postgres.fhirtablesaw.endpoint
SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM postgres.fhirtablesaw.endpoint
