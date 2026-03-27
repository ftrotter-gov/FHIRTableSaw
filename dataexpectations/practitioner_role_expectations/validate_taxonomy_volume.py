"""
Test #4: PractitionerRole Taxonomy Volume

Validates total taxonomy codings through practitioner roles.
Ensures specialty and role coding is being carried through the dataset.
"""
from src.utils.inlaw import InLaw


class ValidatePractitionerRoleTaxonomyVolume(InLaw):
    """Validate total taxonomy coding volume in practitioner roles."""
    
    title = "PractitionerRole taxonomy volume should be within expected range"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate taxonomy volume in practitioner roles.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_taxonomy_codes: Minimum expected taxonomy code count
                - max_taxonomy_codes: Maximum expected taxonomy code count
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for taxonomy codes in PractitionerRole resources
        # Count all specialty.coding entries
        sql = """
            SELECT COUNT(*) AS taxonomy_count
            FROM fhir_resources,
                 unnest(json_extract(resource, '$.specialty')) AS specialty_entry,
                 unnest(json_extract(specialty_entry, '$.coding')) AS coding_entry
            WHERE json_extract_string(resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(coding_entry, '$.system') = 'http://nucc.org/provider-taxonomy'
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Get expected range from config
        min_count = config.get('min_taxonomy_codes', 100)
        max_count = config.get('max_taxonomy_codes', 10000000)
        
        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="taxonomy_count",
            min_value=min_count,
            max_value=max_count
        )
        
        if result.success:
            return True
        
        # Get actual count for error message
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['taxonomy_count'].iloc[0])
        
        return f"Taxonomy code count {actual_count} outside expected range [{min_count}, {max_count}]"
