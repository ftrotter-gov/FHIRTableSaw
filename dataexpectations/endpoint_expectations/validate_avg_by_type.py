"""
Test #17: Average Endpoints by Endpoint Type

Calculates average endpoints by type (FHIR and DirectTrust).
Detects one-sided ingestion failures for specific endpoint classes.
"""
from src.utils.inlaw import InLaw


class ValidateAverageEndpointsByType(InLaw):
    """Validate average endpoints by type within expected ranges."""
    
    title = "Average endpoints by type (FHIR/DirectTrust) should be within expected ranges"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate average endpoints by type.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_avg_fhir_endpoints: Minimum average for FHIR endpoints
                - min_avg_direct_endpoints: Minimum average for DirectTrust endpoints
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for endpoint counts by connection type
        sql = """
            SELECT 
                CASE 
                    WHEN json_extract_string(conn_type, '$.system') = 'http://terminology.hl7.org/CodeSystem/endpoint-connection-type'
                         AND json_extract_string(conn_type, '$.code') LIKE '%fhir%' THEN 'FHIR'
                    WHEN json_extract_string(conn_type, '$.system') = 'http://terminology.hl7.org/CodeSystem/endpoint-connection-type'
                         AND json_extract_string(conn_type, '$.code') LIKE '%direct%' THEN 'DirectTrust'
                    ELSE 'Other'
                END AS endpoint_type,
                COUNT(*) AS endpoint_count
            FROM fhir_resources,
                 unnest(json_extract(resource, '$.connectionType.coding')) AS conn_type
            WHERE json_extract_string(resource, '$.resourceType') = 'Endpoint'
            GROUP BY endpoint_type
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for analysis
        df = gx_df.active_batch.data.dataframe
        
        if len(df) == 0:
            return "No endpoint type data found"
        
        # Calculate averages (in this simplified case, just counts)
        fhir_count = df[df['endpoint_type'] == 'FHIR']['endpoint_count'].sum() if 'FHIR' in df['endpoint_type'].values else 0
        direct_count = df[df['endpoint_type'] == 'DirectTrust']['endpoint_count'].sum() if 'DirectTrust' in df['endpoint_type'].values else 0
        
        min_fhir = config.get('min_avg_fhir_endpoints', 1)
        min_direct = config.get('min_avg_direct_endpoints', 1)
        
        # Check both types meet minimums
        if fhir_count < min_fhir:
            return f"FHIR endpoint count {fhir_count} below minimum {min_fhir}"
        
        if direct_count < min_direct:
            return f"DirectTrust endpoint count {direct_count} below minimum {min_direct}"
        
        return True
