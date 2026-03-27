"""
Test #3: Provider Organization Type Coverage

Validates the number of organizations with type 'provider'.
Ensures organization typing logic is working correctly.
"""
from src.utils.inlaw import InLaw


class ValidateProviderTypeOrganizations(InLaw):
    """Validate count of organizations with 'provider' type."""
    
    title = "Provider organization type count should be within expected range"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate provider organization count.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_provider_orgs: Minimum expected provider organization count
                - max_provider_orgs: Maximum expected provider organization count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for organizations with type='provider'
        # DuckDB JSON syntax: json_extract_string() or resource->>'$.path'
        sql = """
            SELECT COUNT(*) AS provider_org_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_array_length(json_extract(resource, '$.type')) > 0
              AND list_contains(
                  list_transform(
                      json_extract(resource, '$.type[*].coding[*].code'),
                      x -> json_extract_string(x, '$')
                  ),
                  'prov'
              )
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_count = config.get('min_provider_orgs', 1)
        max_count = config.get('max_provider_orgs', 10000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="provider_org_count",
            min_value=min_count,
            max_value=max_count
        )
        
        if result.success:
            return True
        
        # Get actual count for error message
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['provider_org_count'].iloc[0])
        
        return f"Provider org count {actual_count} outside expected range [{min_count}, {max_count}]"
