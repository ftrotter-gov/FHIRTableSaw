"""
Validate required fields in Practitioner table.
"""
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable


class ValidateResourceUuid(InLaw):
    """Validate that all practitioners have a resource_uuid."""
    
    title = "All practitioners should have a resource_uuid"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Run resource_uuid validation test.
        
        Args:
            engine: SQLAlchemy engine for database connection
            config: Configuration dictionary containing schema and table names
        
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
        
        # Query for null resource_uuids
        sql = f"""
            SELECT COUNT(*) AS null_uuid_count
            FROM {practitioner_DBTable}
            WHERE resource_uuid IS NULL
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Expect exactly 0 null resource_uuids
        result = gx_df.expect_column_values_to_be_between(
            column="null_uuid_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found practitioners with NULL resource_uuid"


class ValidateLastName(InLaw):
    """Validate that practitioners have last names."""
    
    title = "Practitioners should have last names"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Run last_name validation test.
        
        Args:
            engine: SQLAlchemy engine for database connection
            config: Configuration dictionary containing schema and table names
        
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
        
        # Query for practitioners without last names
        sql = f"""
            SELECT COUNT(*) AS missing_last_name_count
            FROM {practitioner_DBTable}
            WHERE last_name IS NULL OR TRIM(last_name) = ''
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Allow some flexibility - let's say max 5% can be missing
        # Get total count first to calculate threshold
        total_sql = f"SELECT COUNT(*) AS total_count FROM {practitioner_DBTable}"
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        
        # For now, expect exactly 0 missing last names (can adjust threshold later)
        result = gx_df.expect_column_values_to_be_between(
            column="missing_last_name_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found practitioners without last names"
