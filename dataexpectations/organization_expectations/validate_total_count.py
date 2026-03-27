"""
Test #2: Total Organization Count

Validates that the total number of organization resources is within expected range.
Serves as a census-level expectation to ensure organizations are being loaded consistently.
"""
from src.utils.inlaw import InLaw


class ValidateTotalOrganizationCount(InLaw):
    """Validate total organization count is within expected range."""
    
    title = "Total Organization count should be within expected range"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate total organization count.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_total_organizations: Minimum expected organization count
                - max_total_organizations: Maximum expected organization count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for organization count
        sql = "SELECT COUNT(*) AS organization_count FROM fhir_resources"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_orgs = config.get('min_total_organizations', 1)
        max_orgs = config.get('max_total_organizations', 10000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="organization_count",
            min_value=min_orgs,
            max_value=max_orgs
        )
        
        if result.success:
            return True
        
        # Get actual count for error message using pandas DataFrame
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['organization_count'].iloc[0])
        
        return f"Organization count {actual_count} outside expected range [{min_orgs}, {max_orgs}]"
