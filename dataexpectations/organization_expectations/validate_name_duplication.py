"""
Organization Name Duplication Detection

Identifies patterns of suspicious organization name duplication that might indicate
data quality problems, organizational restructuring not properly handled, or potentially
fraudulent entities.

While some name overlap is legitimate (multiple "St. Mary's Hospital" facilities in
different locations), excessive exact-match duplication suggests problems with
deduplication logic, entity resolution failures, or data entry errors.

This test performs three checks:
1. Exact duplicate name percentage: What fraction of all organization names appear
   more than once? Some duplication is expected for multi-site organizations and
   franchise operations, but a high rate indicates systemic data quality issues.
2. Maximum single name occurrence count: No single organization name should appear
   an unreasonable number of times. Even large franchise operations have a bounded
   number of locations. Extremely high counts suggest data loading errors.
3. Near-duplicate name percentage: After normalizing names by lowering case, stripping
   punctuation, and collapsing whitespace, how many additional duplicates appear that
   were NOT exact matches? These near-duplicates often represent the same organization
   entered multiple times with inconsistent formatting.
"""
import re

from src.utils.inlaw import InLaw


class ValidateOrganizationNameDuplication(InLaw):
    """Detect suspicious patterns of organization name duplication."""

    title = "Organization name duplication should be within acceptable thresholds"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate organization name duplication patterns.

        Checks three aspects of name duplication:
        - Percentage of names that are exact duplicates
        - Maximum occurrence count for any single name
        - Percentage of names that are near-duplicates (differ only in
          punctuation, capitalization, or minor whitespace)

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (currently no config keys are
                    required; all thresholds are defined as internal variables
                    below with documented defaults)

        Returns:
            True if all checks pass, error message string if any check fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ──────────────────────────────────────────────────────────────
        # Threshold variables – adjust these to tune sensitivity
        # ──────────────────────────────────────────────────────────────

        # Maximum acceptable fraction of organization names that are exact
        # duplicates of at least one other organization name.  A value of
        # 0.10 means up to 10% of organizations may share their exact name
        # with another org before this test fails.  Some duplication is
        # expected for multi-site health systems and franchise pharmacies.
        the_maximum_percent_exact_duplicate_names = 0.10

        # Maximum number of times any single organization name may appear.
        # Even the largest franchise chains rarely exceed a few hundred
        # locations in a single directory.  A count above this threshold
        # strongly suggests a data loading or entity resolution bug.
        the_maximum_single_name_occurrence_count = 50

        # Maximum acceptable fraction of organization names that become
        # duplicates only after normalizing punctuation, capitalization,
        # and whitespace.  These "near-duplicates" (e.g. "St. Mary's
        # Hospital" vs "St Marys Hospital") are almost always the same
        # entity entered inconsistently.
        the_maximum_percent_near_duplicate_names = 0.15

        # Step 1: Retrieve all organization names
        all_names_sql = """
            SELECT
                json_extract_string(resource, '$.id') AS org_id,
                json_extract_string(resource, '$.name') AS org_name
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract_string(resource, '$.name') IS NOT NULL
        """
        gx_df_all_names = InLaw.to_gx_dataframe(all_names_sql, engine)
        all_names_df = gx_df_all_names.active_batch.data.dataframe

        total_organization_count = len(all_names_df)

        if total_organization_count == 0:
            return ("validate_name_duplication.py Error: "
                    "No organizations with names found in fhir_resources")

        failures = []

        # Step 2 – exact duplicates, Step 3 – max single count, Step 4 – near-dupes
        ValidateOrganizationNameDuplication._check_exact_duplicates(
            all_names_df=all_names_df,
            total_organization_count=total_organization_count,
            the_maximum_percent_exact_duplicate_names=the_maximum_percent_exact_duplicate_names,
            the_maximum_single_name_occurrence_count=the_maximum_single_name_occurrence_count,
            failures=failures,
        )

        ValidateOrganizationNameDuplication._check_near_duplicates(
            all_names_df=all_names_df,
            total_organization_count=total_organization_count,
            the_maximum_percent_near_duplicate_names=the_maximum_percent_near_duplicate_names,
            failures=failures,
        )

        if not failures:
            return True

        return "; ".join(failures)

    @staticmethod
    def _check_exact_duplicates(
        *,
        all_names_df,
        total_organization_count,
        the_maximum_percent_exact_duplicate_names,
        the_maximum_single_name_occurrence_count,
        failures,
    ):
        """
        Check exact duplicate name percentage and max single name count.

        Appends failure messages to the failures list if thresholds are
        exceeded.

        Args:
            all_names_df: pandas DataFrame with 'org_id' and 'org_name'
            total_organization_count: total number of orgs in the dataset
            the_maximum_percent_exact_duplicate_names: threshold fraction
            the_maximum_single_name_occurrence_count: threshold count
            failures: list to append failure message strings into
        """
        exact_dup_counts = (
            all_names_df
            .groupby('org_name')
            .size()
            .reset_index(name='occurrence_count')
        )
        exact_dup_counts.columns = ['org_name', 'occurrence_count']

        # Names that appear more than once
        duplicated_names_df = exact_dup_counts[
            exact_dup_counts['occurrence_count'] > 1
        ]

        # Count of individual org records whose name is duplicated
        orgs_with_exact_dup_name = all_names_df[
            all_names_df['org_name'].isin(duplicated_names_df['org_name'])
        ].shape[0]

        exact_dup_pct = orgs_with_exact_dup_name / total_organization_count

        if exact_dup_pct > the_maximum_percent_exact_duplicate_names:
            top_dup = duplicated_names_df.sort_values(
                'occurrence_count', ascending=False
            ).iloc[0]
            failures.append(
                f"Exact duplicate name rate {exact_dup_pct:.2%} exceeds "
                f"threshold {the_maximum_percent_exact_duplicate_names:.2%} "
                f"({orgs_with_exact_dup_name} of {total_organization_count} "
                f"orgs). Most common: '{top_dup['org_name']}' appears "
                f"{int(top_dup['occurrence_count'])} times"
            )

        # Check max single name occurrence
        if len(exact_dup_counts) > 0:
            max_row = exact_dup_counts.sort_values(
                'occurrence_count', ascending=False
            ).iloc[0]
            highest_count = int(max_row['occurrence_count'])
            highest_name = max_row['org_name']

            if highest_count > the_maximum_single_name_occurrence_count:
                failures.append(
                    f"Organization name '{highest_name}' appears "
                    f"{highest_count} times, exceeding threshold of "
                    f"{the_maximum_single_name_occurrence_count}"
                )

    @staticmethod
    def _check_near_duplicates(
        *,
        all_names_df,
        total_organization_count,
        the_maximum_percent_near_duplicate_names,
        failures,
    ):
        """
        Check near-duplicate name percentage after normalization.

        Near-duplicates are names that become identical after lowering
        case, removing punctuation, and collapsing whitespace.  For
        example "St. Mary's Hospital" and "St Marys Hospital".

        Appends failure messages to the failures list if threshold is
        exceeded.

        Args:
            all_names_df: pandas DataFrame with 'org_id' and 'org_name'
            total_organization_count: total number of orgs in the dataset
            the_maximum_percent_near_duplicate_names: threshold fraction
            failures: list to append failure message strings into
        """
        normalized_df = all_names_df.copy()
        normalized_df['normalized_name'] = normalized_df['org_name'].apply(
            ValidateOrganizationNameDuplication._normalize_organization_name
        )

        norm_counts = (
            normalized_df
            .groupby('normalized_name')
            .size()
            .reset_index(name='norm_count')
        )
        norm_counts.columns = ['normalized_name', 'norm_count']

        # Normalized names appearing more than once
        norm_dups = norm_counts[norm_counts['norm_count'] > 1]

        orgs_with_norm_dup = normalized_df[
            normalized_df['normalized_name'].isin(
                norm_dups['normalized_name']
            )
        ].shape[0]

        near_dup_pct = orgs_with_norm_dup / total_organization_count

        if near_dup_pct > the_maximum_percent_near_duplicate_names:
            example = (
                ValidateOrganizationNameDuplication
                ._find_near_duplicate_example(
                    normalized_names=normalized_df,
                    normalized_duplicated_df=norm_dups,
                )
            )
            example_msg = ""
            if example:
                example_msg = f" Example near-duplicate group: {example}"

            failures.append(
                f"Near-duplicate name rate {near_dup_pct:.2%} exceeds "
                f"threshold "
                f"{the_maximum_percent_near_duplicate_names:.2%} "
                f"({orgs_with_norm_dup} of {total_organization_count} "
                f"orgs).{example_msg}"
            )

    @staticmethod
    def _normalize_organization_name(name):
        """
        Normalize an organization name for near-duplicate comparison.

        Converts to lowercase, removes all punctuation, and collapses
        multiple whitespace characters into a single space.  This allows
        detection of near-duplicates like:
          "St. Mary's Hospital" vs "St Marys Hospital"

        Args:
            name: The original organization name string

        Returns:
            Normalized lowercase string with punctuation removed and
            whitespace collapsed
        """
        if not isinstance(name, str):
            return ""
        normalized = name.lower()
        # Replace hyphens and dashes with spaces (so "Kaiser-Permanente"
        # matches "Kaiser Permanente")
        normalized = re.sub(r'[-–—]', ' ', normalized)
        # Remove all remaining punctuation characters
        normalized = re.sub(r'[^\w\s]', '', normalized)
        # Collapse multiple whitespace into a single space and strip
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    @staticmethod
    def _find_near_duplicate_example(
        *,
        normalized_names,
        normalized_duplicated_df,
    ):
        """
        Find an example near-duplicate group where names differ only in
        punctuation or capitalization (not exact matches).

        Provides a concrete, actionable example in the failure message.

        Args:
            normalized_names: DataFrame with 'org_name' and
                'normalized_name' columns
            normalized_duplicated_df: DataFrame of normalized names
                appearing more than once

        Returns:
            A string showing example variant spellings, or None if no
            illustrative example is found
        """
        for _, row in normalized_duplicated_df.iterrows():
            norm_name = row['normalized_name']
            matching_orgs = normalized_names[
                normalized_names['normalized_name'] == norm_name
            ]
            distinct_originals = matching_orgs['org_name'].unique()
            # Only interesting if multiple *different* original names
            # normalize to the same thing
            if len(distinct_originals) > 1:
                example_variants = list(distinct_originals[:3])
                return str(example_variants)
        return None
