-- Validation query: endpoint summary statistics
-- Returns one row with three columns: uuid_count, address_count, row_count
SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM fhirtablesaw.endpoint
