-- Section: nppes_taxonomy_analysis
-- Report: gender_preference
-- Description: Gender distribution by taxonomy with statistical significance (z-scores)
-- Returns: Multiple rows with 8 columns: provider_type, male_count, male_percentage, female_count, female_percentage, delta, z_score, total_count

SELECT
    provider_type,
    male_count,
    male_percentage,
    female_count,
    female_percentage,
    delta,
    z_score,
    total_count
FROM z_reports.gender_provider_type_analysis
ORDER BY ABS(z_score) DESC NULLS LAST
