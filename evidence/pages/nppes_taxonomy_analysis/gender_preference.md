---
title: Gender Preference in Clinical Specialty
---

# Gender Preference in Clinical Specialty

Statistical analysis of gender distribution across healthcare provider specialties. This report calculates z-scores to identify clinical specialties with statistically significant gender preferences. Higher absolute z-scores indicate stronger gender associations within a specialty.

**Z-Score Interpretation:**
- Absolute z-score above 3.0: Extremely significant gender difference (p less than 0.001)
- Absolute z-score above 2.0: Highly significant gender difference (p less than 0.05)
- Absolute z-score above 1.0: Notable gender difference
- Positive delta/z-score = Male-dominated specialty
- Negative delta/z-score = Female-dominated specialty

```sql gender_preference_data
SELECT *
FROM postgres.nppes_gender_preference
```

<Details title="Show SQL Source">

```sql
-- Section: nppes_taxonomy_analysis
-- Report: gender_preference
-- Description: Gender distribution by taxonomy with statistical significance (z-scores)
-- Returns: Multiple rows with 8 columns: provider_type, male_count, male_percentage, female_count, female_percentage, delta, z_score, total_count

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
```

</Details>

## Most Extreme Gender Preferences

These specialties show the highest statistical significance for gender preference (top 20 by absolute z-score).

```sql top_extreme_data
SELECT *
FROM postgres.nppes_gender_preference
LIMIT 20
```

<BarChart
    data={top_extreme_data}
    x=provider_type
    y=z_score
    swapXY=true
    title="Top 20 Specialties by Z-Score (Gender Preference Strength)"
/>

## Most Male-Dominated Specialties

```sql most_male_data
SELECT *
FROM postgres.nppes_gender_preference
WHERE z_score > 0
ORDER BY z_score DESC
LIMIT 15
```

<DataTable data={most_male_data}>
  <Column id=provider_type title="Provider Type" />
  <Column id=male_count title="Male Count" fmt=num0 />
  <Column id=male_percentage title="Male %" fmt=num1 />
  <Column id=female_count title="Female Count" fmt=num0 />
  <Column id=female_percentage title="Female %" fmt=num1 />
  <Column id=delta title="Delta (M-F)" fmt=num1 />
  <Column id=z_score title="Z-Score" fmt=num2 />
  <Column id=total_count title="Total" fmt=num0 />
</DataTable>

## Most Female-Dominated Specialties

```sql most_female_data
SELECT *
FROM postgres.nppes_gender_preference
WHERE z_score < 0
ORDER BY z_score ASC
LIMIT 15
```

<DataTable data={most_female_data}>
  <Column id=provider_type title="Provider Type" />
  <Column id=male_count title="Male Count" fmt=num0 />
  <Column id=male_percentage title="Male %" fmt=num1 />
  <Column id=female_count title="Female Count" fmt=num0 />
  <Column id=female_percentage title="Female %" fmt=num1 />
  <Column id=delta title="Delta (M-F)" fmt=num1 />
  <Column id=z_score title="Z-Score" fmt=num2 />
  <Column id=total_count title="Total" fmt=num0 />
</DataTable>

## Complete Data

Full dataset for all provider specialties with both male and female practitioners (minimum 30 total providers).

<DataTable data={gender_preference_data} search=true rows=25>
  <Column id=provider_type title="Provider Type" />
  <Column id=male_count title="Male Count" fmt=num0 />
  <Column id=male_percentage title="Male %" fmt=num1 />
  <Column id=female_count title="Female Count" fmt=num0 />
  <Column id=female_percentage title="Female %" fmt=num1 />
  <Column id=delta title="Delta (M-F)" fmt=num1 />
  <Column id=z_score title="Z-Score" fmt=num2 />
  <Column id=total_count title="Total" fmt=num0 />
</DataTable>
