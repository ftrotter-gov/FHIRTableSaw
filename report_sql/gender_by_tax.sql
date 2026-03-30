-- Gender distribution by taxonomy with statistical significance
-- Calculates counts, percentages, delta, and z-score for each provider type
-- Orders by z-score (most significant gender differences first)

CREATE TABLE z_reports.gender_provider_type_analysis AS
WITH gender_counts AS (
    SELECT
        taxonomy_code_id,
        short_description AS provider_type,
        COUNT(DISTINCT CASE WHEN provider_sex_code = 'M' THEN npi_taxonomy.npi END) AS male_count,
        COUNT(DISTINCT CASE WHEN provider_sex_code = 'F' THEN npi_taxonomy.npi END) AS female_count,
        COUNT(DISTINCT npi_taxonomy.npi) AS total_count
    FROM nppes_silver.npi_taxonomy
    JOIN dctnry_gold.nucc_taxonomycode ON
        nucc_taxonomycode.id = npi_taxonomy.taxonomy_code_id
    JOIN bronze_nppes.main_file ON main_file.npi = npi_taxonomy.npi
    WHERE entity_type_code = '1'
        AND provider_sex_code IN ('M', 'F')
    GROUP BY taxonomy_code_id, short_description
),
stats AS (
    SELECT
        provider_type,
        male_count,
        female_count,
        total_count,
        -- Calculate percentages
        ROUND(100.0 * male_count / NULLIF(total_count, 0), 2) AS male_percentage,
        ROUND(100.0 * female_count / NULLIF(total_count, 0), 2) AS female_percentage,
        -- Calculate delta (male % - female %)
        ROUND(100.0 * male_count / NULLIF(total_count, 0) -
              100.0 * female_count / NULLIF(total_count, 0), 2) AS delta,
        -- Calculate pooled proportion for z-score
        (male_count + female_count) / NULLIF((total_count + total_count)::NUMERIC, 0) AS pooled_prop
    FROM gender_counts
    WHERE total_count >= 30  -- Minimum sample size for z-score validity
),
with_z_scores AS (
    SELECT
        provider_type,
        male_count,
        male_percentage,
        female_count,
        female_percentage,
        delta,
        -- Calculate z-score for difference in proportions
        ROUND(
            CASE
                WHEN male_count > 0 AND female_count > 0 THEN
                    (male_percentage/100.0 - female_percentage/100.0) /
                    NULLIF(
                        SQRT(
                            pooled_prop * (1 - pooled_prop) *
                            (1.0/NULLIF(male_count, 0) + 1.0/NULLIF(female_count, 0))
                        ),
                        0
                    )
                ELSE NULL
            END,
            2
        ) AS z_score,
        total_count
    FROM stats
    WHERE male_count > 0 AND female_count > 0  -- Only include taxonomies with both genders
)
SELECT
    provider_type,
    male_count,
    male_percentage,
    female_count,
    female_percentage,
    delta,
    z_score,
    total_count
FROM with_z_scores
ORDER BY ABS(z_score) DESC NULLS LAST
