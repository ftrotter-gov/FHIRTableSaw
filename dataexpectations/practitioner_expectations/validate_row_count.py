"""
Validate Practitioner table row count is within expected range.

Works with both PostgreSQL (normalized tables) and DuckDB (JSON resources).
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
                For PostgreSQL:
                    - schema: Database schema name
                    - practitioner_table: Practitioner table name
                    - min_expected_rows: Minimum expected row count
                    - max_expected_rows: Maximum expected row count
                For DuckDB:
                    - duckdb_path: Path to DuckDB file
                    - min_total_practitioners: Minimum expected row count
                    - max_total_practitioners: Maximum expected row count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Check if using DuckDB or PostgreSQL
        if 'duckdb_path' in config:
            # DuckDB: Query fhir_resources table
            sql = "SELECT COUNT(*) AS row_count FROM fhir_resources"
            min_rows = config.get('min_total_practitioners', 1)
            max_rows = config.get('max_total_practitioners', 10000000)
        else:
            # PostgreSQL: Query normalized practitioner table
            practitioner_DBTable = DBTable(
                schema=config.get('schema', 'public'),
                table=config.get('practitioner_table', 'practitioners')
            )
            sql = f"SELECT COUNT(*) AS row_count FROM {practitioner_DBTable}"
            min_rows = config.get('min_expected_rows', 1)
            max_rows = config.get('max_expected_rows', 10000000)
        
        # Execute query and get GX validator
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="row_count",
            min_value=min_rows,
            max_value=max_rows
        )
        
        if result.success:
            return True
        
        # Get actual count for error message using pandas DataFrame access
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['row_count'].iloc[0])
        
        return f"Row count {actual_count} outside expected range [{min_rows}, {max_rows}]"
