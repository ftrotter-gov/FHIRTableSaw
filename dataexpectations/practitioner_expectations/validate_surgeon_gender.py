"""
Test #6: Surgeon Gender Distribution

Validates gender distribution among surgeons.
Expects higher proportion of men than women.
"""
from src.utils.inlaw import InLaw


class ValidateSurgeonGenderDistribution(InLaw):
    """Validate surgeon gender distribution shows expected pattern."""
    
    title = "Surgeon gender distribution should show higher male proportion"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate gender distribution among surgeons.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_male_surgeon_pct: Minimum expected male percentage (default 60)
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query for surgeons by joining Practitioner and PractitionerRole on id
        # Surgeon taxonomy codes start with "208" in NUCC
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
                    AND json_extract_string(coding, '$.code') LIKE '208%'
              )
            GROUP BY json_extract_string(pract.resource, '$.gender')
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for analysis
        df = gx_df.active_batch.data.dataframe
        
        if len(df) == 0:
            return "No surgeons with gender information found"
        
        total = df['count'].sum()
        male_count = df[df['gender'] == 'male']['count'].sum() if 'male' in df['gender'].values else 0
        male_pct = (male_count / total) * 100 if total > 0 else 0
        
        min_male_pct = config.get('min_male_surgeon_pct', 60)
        
        if male_pct >= min_male_pct:
            return True
        
        return f"Male surgeon percentage {male_pct:.1f}% below expected minimum {min_male_pct}%"
