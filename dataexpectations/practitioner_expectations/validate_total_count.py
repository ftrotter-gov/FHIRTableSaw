"""
Validate total Practitioner count in DuckDB.
"""
from src.utils.duckdb_helper import DuckDBHelper
import great_expectations as gx


class ValidateTotalPractitionerCount:
    """Validate that the Practitioner DuckDB has an expected number of resources."""
    
    title = "Total Practitioner count should be within expected range"
    
    @staticmethod
    def run(engine=None, config: dict | None = None):
        """
        Run total practitioner count validation test.
        
        Args:
            engine: Ignored for DuckDB tests
            config: Configuration dictionary containing:
                - min_total_practitioners: Minimum expected count
                - max_total_practitioners: Maximum expected count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for total count
        sql = "SELECT COUNT(*) AS practitioner_count FROM fhir_resources"
        
        try:
            df = DuckDBHelper.query_to_dataframe(
                resource_type='practitioner',
                sql=sql,
                cache_dir=config.get('cache_dir', '../saw_cache')
            )
            
            actual_count = int(df['practitioner_count'].iloc[0])
            min_count = config.get('min_total_practitioners', 1)
            max_count = config.get('max_total_practitioners', 10000000)
            
            if min_count <= actual_count <= max_count:
                return True
            
            return f"Practitioner count {actual_count} outside expected range [{min_count}, {max_count}]"
            
        except Exception as e:
            return f"Error running test: {str(e)}"
