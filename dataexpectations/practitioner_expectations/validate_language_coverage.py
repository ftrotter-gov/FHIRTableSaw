"""
Test #11: Practitioner Language Coverage

Measures percentage of practitioners with language information.
Assesses accessibility metadata completeness.
"""
from src.utils.inlaw import InLaw


class ValidatePractitionerLanguageCoverage(InLaw):
    """Validate percentage of practitioners with language data."""
    
    title = "Practitioner language coverage should meet minimum threshold"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate language coverage in practitioner data.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_language_coverage_pct: Minimum expected percentage with language (default 10)
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for practitioners with/without language data
        sql = """
            SELECT 
                CASE 
                    WHEN json_array_length(json_extract(resource, '$.communication')) > 0 THEN 'has_language'
                    ELSE 'no_language'
                END AS language_status,
                COUNT(*) AS practitioner_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
            GROUP BY language_status
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for percentage calculation
        df = gx_df.active_batch.data.dataframe
        
        total = df['practitioner_count'].sum()
        has_language = df[df['language_status'] == 'has_language']['practitioner_count'].sum() if 'has_language' in df['language_status'].values else 0
        
        coverage_pct = (has_language / total) * 100 if total > 0 else 0
        
        min_coverage = config.get('min_language_coverage_pct', 10)
        
        if coverage_pct >= min_coverage:
            return True
        
        return f"Language coverage {coverage_pct:.1f}% below minimum {min_coverage}%"
