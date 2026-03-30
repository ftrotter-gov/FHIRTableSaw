-- Section: endpoint
-- Report: summary_stats
-- Description: Summary statistics for the endpoint table
-- Returns: 1 row with 3 columns: uuid_count, address_count, row_count

SELECT
    COUNT(DISTINCT(resource_uuid)) AS uuid_count,
    COUNT(DISTINCT(address)) AS address_count,
    COUNT(*) AS row_count
FROM fhirtablesaw.endpoint
