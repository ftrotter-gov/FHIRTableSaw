"""
Validate Practitioner table row count is within expected range.
"""
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable


class ValidateRowCount(InLaw):
    """Validate that the Practitioner table has an expected number of rows."""
    
    title = "Practitioner table should have expected number of rows"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Run row count validation test.
        
        Args:
            engine: SQLAlchemy engine for database connection
            config: Configuration dictionary containing:
                - schema: Database schema name
                - practitioner_table: Practitioner table name
                - min_expected_rows: Minimum expected row count
                - max_expected_rows: Maximum expected row count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Build table reference
        practitioner_DBTable = DBTable(
            schema=config.get('schema', 'public'),
            table=config.get('practitioner_table', 'practitioners')
        )
        
        # Query row count
        sql = f"SELECT COUNT(*) AS row_count FROM {practitioner_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_rows = config.get('min_expected_rows', 1)
        max_rows = config.get('max_expected_rows', 10000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="row_count",
            min_value=min_rows,
            max_value=max_rows
        )
        
        if result.success:
            return True
        return f"Row count validation failed: expected {min_rows}-{max_rows} rows"
