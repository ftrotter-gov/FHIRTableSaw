"""
Validate NPI fields in Practitioner table.
"""
from src.utils.inlaw import InLaw
from src.utils.dbtable import DBTable


class ValidateNpiFormat(InLaw):
    """Validate that NPI values are properly formatted (10 digits)."""
    
    title = "NPI values should be 10 digits when present"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Run NPI format validation test.
        
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
        
        # Query for invalid NPIs (not 10 digits when non-null)
        sql = f"""
            SELECT COUNT(*) AS invalid_npi_count
            FROM {practitioner_DBTable}
            WHERE npi IS NOT NULL
              AND LENGTH(npi) != 10
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Expect exactly 0 invalid NPIs
        result = gx_df.expect_column_values_to_be_between(
            column="invalid_npi_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found NPIs with invalid length (expected 10 digits)"


class ValidateNpiUniqueness(InLaw):
    """Validate that NPI values are unique when present."""
    
    title = "NPI values should be unique (no duplicates)"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Run NPI uniqueness validation test.
        
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
        
        # Query for duplicate NPIs
        sql = f"""
            SELECT COUNT(*) AS duplicate_npi_count
            FROM (
                SELECT npi, COUNT(*) AS npi_count
                FROM {practitioner_DBTable}
                WHERE npi IS NOT NULL
                GROUP BY npi
                HAVING COUNT(*) > 1
            ) AS duplicates
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Expect exactly 0 duplicate NPIs
        result = gx_df.expect_column_values_to_be_between(
            column="duplicate_npi_count",
            min_value=0,
            max_value=0
        )
        
        if result.success:
            return True
        return "Found duplicate NPI values in Practitioner table"
