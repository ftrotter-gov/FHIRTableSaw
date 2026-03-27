"""
Test #9: Physician Type Ranking by NUCC Code

Counts physicians by NUCC taxonomy code and verifies top 10 match expectations.
Guards against taxonomy drift and broken grouping logic.
"""
from src.utils.inlaw import InLaw


class ValidatePhysicianTypeRanking(InLaw):
    """Validate top 10 physician types by NUCC code match expected rankings."""
    
    title = "Top 10 physician types should approximately match expected rankings"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate physician type distribution.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - expected_top_10_nucc: List of expected top 10 NUCC codes
                - min_top_10_match: Minimum number that should match (default 7)
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for physician counts by NUCC taxonomy code
        sql = """
            SELECT 
                json_extract_string(coding, '$.code') AS nucc_code,
                json_extract_string(coding, '$.display') AS nucc_display,
                COUNT(DISTINCT json_extract_string(pract.resource, '$.id')) AS physician_count
            FROM fhir_resources AS pract,
                 fhir_resources AS role,
                 unnest(json_extract(role.resource, '$.specialty')) AS specialty,
                 unnest(json_extract(specialty, '$.coding')) AS coding
            WHERE json_extract_string(pract.resource, '$.resourceType') = 'Practitioner'
              AND json_extract_string(role.resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(role.resource, '$.practitioner.reference') LIKE '%' || json_extract_string(pract.resource, '$.id')
              AND json_extract_string(coding, '$.system') = 'http://nucc.org/provider-taxonomy'
              AND json_extract_string(coding, '$.code') IS NOT NULL
            GROUP BY json_extract_string(coding, '$.code'), json_extract_string(coding, '$.display')
            ORDER BY physician_count DESC
            LIMIT 10
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for ranking analysis
        df = gx_df.active_batch.data.dataframe
        
        if len(df) == 0:
            return "No physician taxonomy codes found"
        
        # Get actual top 10 NUCC codes
        actual_top_10 = df['nucc_code'].tolist()
        
        # Get expected top 10 from config
        expected_top_10 = config.get('expected_top_10_nucc', [])
        min_match = config.get('min_top_10_match', 7)
        
        if not expected_top_10:
            # No expected list provided, just verify we have data
            return True
        
        # Count how many of actual top 10 are in expected list
        matches = sum(1 for code in actual_top_10 if code in expected_top_10)
        
        if matches >= min_match:
            return True
        
        return f"Only {matches}/{min_match} top NUCC codes match expected. Actual: {actual_top_10[:5]}..."
