"""
Test #8: Psychiatrist Count

Validates number of psychiatrists in the dataset.
Provides specialty-specific benchmark for important provider group.
"""
from src.utils.inlaw import InLaw


class ValidatePsychiatristCount(InLaw):
    """Validate psychiatrist count is within expected range."""
    
    title = "Psychiatrist count should be within expected range"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate psychiatrist count.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_psychiatrist_count: Minimum expected psychiatrist count
                - max_psychiatrist_count: Maximum expected psychiatrist count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for psychiatrists
        # Psychiatry NUCC codes: 2084P0800X (Psychiatry), 2084P0804X, etc.
        sql = """
            SELECT COUNT(DISTINCT json_extract_string(pract.resource, '$.id')) AS psychiatrist_count
            FROM fhir_resources AS pract
            WHERE json_extract_string(pract.resource, '$.resourceType') = 'Practitioner'
              AND EXISTS (
                  SELECT 1 
                  FROM fhir_resources AS role,
                       unnest(json_extract(role.resource, '$.specialty')) AS specialty,
                       unnest(json_extract(specialty, '$.coding')) AS coding
                  WHERE json_extract_string(role.resource, '$.resourceType') = 'PractitionerRole'
                    AND json_extract_string(role.resource, '$.practitioner.reference') LIKE '%' || json_extract_string(pract.resource, '$.id')
                    AND json_extract_string(coding, '$.system') = 'http://nucc.org/provider-taxonomy'
                    AND json_extract_string(coding, '$.code') LIKE '2084P%'
              )
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_count = config.get('min_psychiatrist_count', 10)
        max_count = config.get('max_psychiatrist_count', 1000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="psychiatrist_count",
            min_value=min_count,
            max_value=max_count
        )
        
        if result.success:
            return True
        
        # Get actual count for error message
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['psychiatrist_count'].iloc[0])
        
        return f"Psychiatrist count {actual_count} outside expected range [{min_count}, {max_count}]"
