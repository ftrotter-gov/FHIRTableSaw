"""
Test #7: Pediatrician Gender Distribution

Validates gender distribution among pediatricians.
Expects higher proportion of women than men.
"""
from src.utils.inlaw import InLaw


class ValidatePediatricianGenderDistribution(InLaw):
    """Validate pediatrician gender distribution shows expected pattern."""
    
    title = "Pediatrician gender distribution should show higher female proportion"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate gender distribution among pediatricians.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_female_pediatrician_pct: Minimum expected female percentage (default 55)
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for pediatricians
        # Pediatrician NUCC codes: 208000000X (Pediatrics), 2080P0216X, etc.
        sql = """
            SELECT 
                json_extract_string(pract.resource, '$.gender') AS gender,
                COUNT(*) AS count
            FROM fhir_resources AS pract
            WHERE json_extract_string(pract.resource, '$.resourceType') = 'Practitioner'
              AND json_extract_string(pract.resource, '$.gender') IS NOT NULL
              AND EXISTS (
                  SELECT 1 
                  FROM fhir_resources AS role,
                       unnest(json_extract(role.resource, '$.specialty')) AS specialty,
                       unnest(json_extract(specialty, '$.coding')) AS coding
                  WHERE json_extract_string(role.resource, '$.resourceType') = 'PractitionerRole'
                    AND json_extract_string(role.resource, '$.practitioner.reference') LIKE '%' || json_extract_string(pract.resource, '$.id')
                    AND json_extract_string(coding, '$.system') = 'http://nucc.org/provider-taxonomy'
                    AND json_extract_string(coding, '$.code') LIKE '2080%'
              )
            GROUP BY json_extract_string(pract.resource, '$.gender')
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for analysis
        df = gx_df.active_batch.data.dataframe
        
        if len(df) == 0:
            return "No pediatricians with gender information found"
        
        total = df['count'].sum()
        female_count = df[df['gender'] == 'female']['count'].sum() if 'female' in df['gender'].values else 0
        female_pct = (female_count / total) * 100 if total > 0 else 0
        
        min_female_pct = config.get('min_female_pediatrician_pct', 55)
        
        if female_pct >= min_female_pct:
            return True
        
        return f"Female pediatrician percentage {female_pct:.1f}% below expected minimum {min_female_pct}%"
