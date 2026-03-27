"""
Test #16: Total Endpoint Count

Validates total number of endpoint resources.
Strategic for electronic connectivity layer monitoring.
"""
from src.utils.inlaw import InLaw


class ValidateTotalEndpointCount(InLaw):
    """Validate total endpoint count is within expected range."""
    
    title = "Total Endpoint count should be within expected range"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate total endpoint count.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_total_endpoints: Minimum expected endpoint count
                - max_total_endpoints: Maximum expected endpoint count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for endpoint count
        sql = "SELECT COUNT(*) AS endpoint_count FROM fhir_resources"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_eps = config.get('min_total_endpoints', 1)
        max_eps = config.get('max_total_endpoints', 10000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="endpoint_count",
            min_value=min_eps,
            max_value=max_eps
        )
        
        if result.success:
            return True
        
        # Get actual count for error message
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['endpoint_count'].iloc[0])
        
        return f"Endpoint count {actual_count} outside expected range [{min_eps}, {max_eps}]"
