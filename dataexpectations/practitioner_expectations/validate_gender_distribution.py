"""
Test #5: Overall Gender Distribution

Measures gender split across practitioner population.
Checks for roughly balanced distribution with tolerance.
"""
from src.utils.inlaw import InLaw


class ValidateOverallGenderDistribution(InLaw):
    """Validate overall gender distribution is approximately balanced."""
    
    title = "Overall gender distribution should be roughly balanced"
    
    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate gender distribution across all practitioners.
        
        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_gender_balance_pct: Minimum percentage for either gender (default 30)
                - max_gender_balance_pct: Maximum percentage for either gender (default 70)
        
        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"
        
        # Query DuckDB for gender counts from Practitioner resources
        sql = """
            SELECT 
                json_extract_string(resource, '$.gender') AS gender,
                COUNT(*) AS gender_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND json_extract_string(resource, '$.gender') IS NOT NULL
            GROUP BY json_extract_string(resource, '$.gender')
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        
        # Access pandas DataFrame for distribution analysis
        df = gx_df.active_batch.data.dataframe
        
        # Calculate percentages
        total_with_gender = df['gender_count'].sum()
        
        if total_with_gender == 0:
            return "No practitioners with gender information found"
        
        # Calculate percentages for each gender
        gender_percentages = {}
        for _, row in df.iterrows():
            gender = row['gender']
            count = row['gender_count']
            pct = (count / total_with_gender) * 100
            gender_percentages[gender] = pct
        
        # Get thresholds from config
        min_pct = config.get('min_gender_balance_pct', 30)
        max_pct = config.get('max_gender_balance_pct', 70)
        
        # Check if any single gender dominates too much
        for gender, pct in gender_percentages.items():
            if pct > max_pct:
                return f"Gender '{gender}' at {pct:.1f}% exceeds maximum {max_pct}%"
            if pct < min_pct and pct > 5:  # Only check minimums for significant groups
                return f"Gender '{gender}' at {pct:.1f}% below minimum {min_pct}%"
        
        return True
