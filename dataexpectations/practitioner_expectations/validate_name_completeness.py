"""
Practitioner Name Component Completeness

Validates that practitioners have complete name information including both
family (last) name and given (first) name components. Detects placeholder
values, suspiciously short names, and patterns indicating test/junk data.

Complete names are essential for:
- Human-readable directories
- Disambiguation between similar providers
- Integration with credentialing and verification systems that rely on name matching

Practitioners without complete, real names cannot be properly verified against
external credential sources and may represent test data, data entry errors,
or potentially fraudulent entries. A high rate of incomplete names indicates
serious data quality issues that undermine directory reliability.
"""
from src.utils.inlaw import InLaw


class ValidateNameBothFamilyAndGiven(InLaw):
    """Validate that practitioners have both family (last) and given (first) name components.

    Single-word names (only family or only given) make it impossible to
    disambiguate providers and break integrations with credentialing systems
    that require full name matching.
    """

    title = "Practitioners should have both family and given name components"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check that at least a minimum percentage of practitioners have both
        a family name and at least one given name present and non-empty.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (currently unused for thresholds,
                    but accepted for InLaw runner compatibility)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Thresholds ──────────────────────────────────────────────────
        # Minimum percentage of practitioners that must have BOTH a family
        # name and at least one given name populated with non-empty text.
        # 95% allows for a small number of culturally single-name practitioners
        # or legitimately incomplete records, while still catching systemic
        # data quality failures.
        the_minimum_percent_with_both_family_and_given_name = 0.95

        # ── Query: count practitioners with both name parts ─────────────
        sql = """
            SELECT
                COUNT(*) AS total_practitioners,
                COUNT(CASE
                    WHEN json_extract_string(resource, '$.name[0].family') IS NOT NULL
                     AND TRIM(json_extract_string(resource, '$.name[0].family')) != ''
                     AND json_extract_string(resource, '$.name[0].given[0]') IS NOT NULL
                     AND TRIM(json_extract_string(resource, '$.name[0].given[0]')) != ''
                    THEN 1
                END) AS practitioners_with_both_names
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        df = gx_df.active_batch.data.dataframe

        total_practitioners = int(df['total_practitioners'].iloc[0])
        practitioners_with_both_names = int(df['practitioners_with_both_names'].iloc[0])

        if total_practitioners == 0:
            return "validate_name_completeness.py Error: No Practitioner resources found in fhir_resources table"

        actual_percent_with_both = practitioners_with_both_names / total_practitioners
        missing_count = total_practitioners - practitioners_with_both_names

        if actual_percent_with_both >= the_minimum_percent_with_both_family_and_given_name:
            return True

        return (
            f"Only {actual_percent_with_both:.1%} of practitioners have both family and given names "
            f"(threshold: {the_minimum_percent_with_both_family_and_given_name:.0%}). "
            f"{missing_count} of {total_practitioners} practitioners are missing one or both name components."
        )


class ValidateNoPlaceholderNames(InLaw):
    """Validate that placeholder / test names are rare in practitioner data.

    Names like 'Test', 'Unknown', 'Pending', 'N/A', 'None', or 'Null'
    indicate test data, data entry shortcuts, or upstream system defaults
    that were never replaced with real values. These must be caught because
    they pollute provider directories and will fail credential verification.
    """

    title = "Practitioners should not have placeholder names (Test, Unknown, Pending, etc.)"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check that the percentage of practitioners whose family or given name
        matches a known placeholder pattern does not exceed a threshold.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (currently unused for thresholds,
                    but accepted for InLaw runner compatibility)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Thresholds ──────────────────────────────────────────────────
        # Maximum percentage of practitioners allowed to have a placeholder
        # name in either the family or given field. 1% is generous — in a
        # production directory you would ideally want this at 0%.
        the_maximum_percent_with_placeholder_names = 0.01

        # Known placeholder name patterns. These are compared case-insensitively
        # against both family and given name fields after trimming whitespace.
        the_placeholder_name_patterns = ['test', 'unknown', 'pending', 'n/a', 'none', 'null']

        # ── Build the LOWER() CASE expressions for each placeholder ─────
        # We check if the lowered+trimmed family or given name exactly equals
        # any of the placeholder strings.
        family_conditions = " OR ".join(
            [
                f"LOWER(TRIM(json_extract_string(resource, '$.name[0].family'))) = '{pattern}'"
                for pattern in the_placeholder_name_patterns
            ]
        )
        given_conditions = " OR ".join(
            [
                f"LOWER(TRIM(json_extract_string(resource, '$.name[0].given[0]'))) = '{pattern}'"
                for pattern in the_placeholder_name_patterns
            ]
        )
        placeholder_conditions = f"({family_conditions}) OR ({given_conditions})"

        # ── Query: count total practitioners ────────────────────────────
        sql_total = """
            SELECT COUNT(*) AS total_practitioners
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """

        # ── Query: count practitioners with placeholder names ───────────
        sql_placeholder = f"""
            SELECT COUNT(*) AS placeholder_name_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND ({placeholder_conditions})
        """

        gx_total_df = InLaw.to_gx_dataframe(sql_total, engine)
        total_practitioners = int(
            gx_total_df.active_batch.data.dataframe['total_practitioners'].iloc[0]
        )

        if total_practitioners == 0:
            return "validate_name_completeness.py Error: No Practitioner resources found in fhir_resources table"

        gx_placeholder_df = InLaw.to_gx_dataframe(sql_placeholder, engine)
        placeholder_name_count = int(
            gx_placeholder_df.active_batch.data.dataframe['placeholder_name_count'].iloc[0]
        )

        actual_placeholder_percent = placeholder_name_count / total_practitioners

        if actual_placeholder_percent <= the_maximum_percent_with_placeholder_names:
            return True

        return (
            f"{actual_placeholder_percent:.2%} of practitioners have placeholder names "
            f"(threshold: {the_maximum_percent_with_placeholder_names:.0%}). "
            f"{placeholder_name_count} of {total_practitioners} practitioners matched patterns: "
            f"{the_placeholder_name_patterns}"
        )


class ValidateMinimumNameLength(InLaw):
    """Validate that practitioner names meet a minimum character length.

    Single-character or empty names (after trimming) typically represent
    initials-only entries or data entry errors. While initials can be
    legitimate middle names, a family or given name consisting of a single
    character is almost always a data quality issue.
    """

    title = "Practitioner names should meet minimum length (not initials-only)"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check that the percentage of practitioners whose family or given name
        is shorter than a minimum length does not exceed a threshold.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (currently unused for thresholds,
                    but accepted for InLaw runner compatibility)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Thresholds ──────────────────────────────────────────────────
        # Minimum number of characters for a family or given name to be
        # considered a real name rather than an initial or placeholder.
        # A value of 2 catches single-letter entries like "A" or "X" while
        # still allowing short but legitimate names like "Li", "Bo", "Al".
        the_minimum_name_length_characters = 2

        # Maximum percentage of practitioners allowed to have a name
        # (family or given) that is shorter than the minimum length.
        # 2% provides tolerance for legitimate edge cases.
        the_maximum_percent_single_character_names = 0.02

        # ── Query: count total practitioners ────────────────────────────
        sql_total = """
            SELECT COUNT(*) AS total_practitioners
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """

        # ── Query: count practitioners with too-short names ─────────────
        # A practitioner is flagged if EITHER their family or given name
        # is present, non-empty, but shorter than the minimum length.
        sql_short_names = f"""
            SELECT COUNT(*) AS short_name_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND (
                  (
                      json_extract_string(resource, '$.name[0].family') IS NOT NULL
                      AND TRIM(json_extract_string(resource, '$.name[0].family')) != ''
                      AND LENGTH(TRIM(json_extract_string(resource, '$.name[0].family')))
                          < {the_minimum_name_length_characters}
                  )
                  OR
                  (
                      json_extract_string(resource, '$.name[0].given[0]') IS NOT NULL
                      AND TRIM(json_extract_string(resource, '$.name[0].given[0]')) != ''
                      AND LENGTH(TRIM(json_extract_string(resource, '$.name[0].given[0]')))
                          < {the_minimum_name_length_characters}
                  )
              )
        """

        gx_total_df = InLaw.to_gx_dataframe(sql_total, engine)
        total_practitioners = int(
            gx_total_df.active_batch.data.dataframe['total_practitioners'].iloc[0]
        )

        if total_practitioners == 0:
            return "validate_name_completeness.py Error: No Practitioner resources found in fhir_resources table"

        gx_short_df = InLaw.to_gx_dataframe(sql_short_names, engine)
        short_name_count = int(
            gx_short_df.active_batch.data.dataframe['short_name_count'].iloc[0]
        )

        actual_short_percent = short_name_count / total_practitioners

        if actual_short_percent <= the_maximum_percent_single_character_names:
            return True

        return (
            f"{actual_short_percent:.2%} of practitioners have names shorter than "
            f"{the_minimum_name_length_characters} characters "
            f"(threshold: {the_maximum_percent_single_character_names:.0%}). "
            f"{short_name_count} of {total_practitioners} practitioners have "
            f"initials-only or single-character names."
        )


class ValidateNameSuspiciousPatterns(InLaw):
    """Validate that practitioner names do not contain suspicious patterns.

    Names that are entirely uppercase (e.g. "SMITH" instead of "Smith") may
    indicate legacy system imports that never normalized casing. Names
    containing digits or special characters (other than hyphens, apostrophes,
    and periods which are legitimate in names like "O'Brien" or "St. James")
    suggest data corruption or system-generated entries.
    """

    title = "Practitioner names should not have suspicious patterns (all-caps, special characters)"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check that the percentage of practitioners with suspicious name
        patterns (all uppercase or containing non-name characters) does
        not exceed a threshold.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (currently unused for thresholds,
                    but accepted for InLaw runner compatibility)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Thresholds ──────────────────────────────────────────────────
        # Maximum percentage of practitioners allowed to have family names
        # that are entirely uppercase AND longer than a minimum length.
        # 10% is a lenient default since some legacy systems store names in
        # all-caps. Tighten this as data normalization improves.
        the_maximum_percent_all_caps_family_names = 0.10

        # Maximum percentage of practitioners allowed to have names containing
        # digits or special characters that are not typical in human names.
        # Allowed special characters: hyphen (-), apostrophe ('), period (.),
        # and space ( ) — as used in "O'Brien", "St. James", "Mary-Jane".
        the_maximum_percent_names_with_special_characters = 0.01

        # Minimum length of family name to be considered for all-caps check.
        # Short names like "LI" or "BO" may legitimately be all caps.
        the_minimum_length_for_all_caps_check = 4

        # ── Query: count total practitioners ────────────────────────────
        sql_total = """
            SELECT COUNT(*) AS total_practitioners
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """

        gx_total_df = InLaw.to_gx_dataframe(sql_total, engine)
        total_practitioners = int(
            gx_total_df.active_batch.data.dataframe['total_practitioners'].iloc[0]
        )

        if total_practitioners == 0:
            return ("validate_name_completeness.py Error: "
                    "No Practitioner resources found in fhir_resources table")

        failures = []

        # ── Check all-caps family names ─────────────────────────────────
        # A name is "all caps" if it equals its own UPPER() and is long
        # enough to not be a legitimate short name.
        sql_all_caps = f"""
            SELECT COUNT(*) AS all_caps_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND json_extract_string(resource, '$.name[0].family') IS NOT NULL
              AND TRIM(json_extract_string(resource, '$.name[0].family')) != ''
              AND LENGTH(TRIM(json_extract_string(resource, '$.name[0].family')))
                  >= {the_minimum_length_for_all_caps_check}
              AND TRIM(json_extract_string(resource, '$.name[0].family'))
                  = UPPER(TRIM(json_extract_string(resource, '$.name[0].family')))
        """

        gx_caps_df = InLaw.to_gx_dataframe(sql_all_caps, engine)
        all_caps_count = int(
            gx_caps_df.active_batch.data.dataframe['all_caps_count'].iloc[0]
        )
        actual_all_caps_percent = all_caps_count / total_practitioners

        if actual_all_caps_percent > the_maximum_percent_all_caps_family_names:
            failures.append(
                f"{actual_all_caps_percent:.2%} of practitioners have all-caps family names "
                f"(>= {the_minimum_length_for_all_caps_check} chars) "
                f"(threshold: {the_maximum_percent_all_caps_family_names:.0%}). "
                f"{all_caps_count} of {total_practitioners} affected"
            )

        # ── Check special characters in names ───────────────────────────
        # Uses regexp_matches to find names with characters other than
        # letters, spaces, hyphens, apostrophes, and periods.
        sql_special_chars = r"""
            SELECT COUNT(*) AS special_char_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND (
                  (
                      json_extract_string(resource, '$.name[0].family') IS NOT NULL
                      AND regexp_matches(
                          json_extract_string(resource, '$.name[0].family'),
                          '[^a-zA-Z \-''\.]'
                      )
                  )
                  OR
                  (
                      json_extract_string(resource, '$.name[0].given[0]') IS NOT NULL
                      AND regexp_matches(
                          json_extract_string(resource, '$.name[0].given[0]'),
                          '[^a-zA-Z \-''\.]'
                      )
                  )
              )
        """

        gx_special_df = InLaw.to_gx_dataframe(sql_special_chars, engine)
        special_char_count = int(
            gx_special_df.active_batch.data.dataframe['special_char_count'].iloc[0]
        )
        actual_special_percent = special_char_count / total_practitioners

        if actual_special_percent > the_maximum_percent_names_with_special_characters:
            failures.append(
                f"{actual_special_percent:.2%} of practitioners have names with "
                f"special characters "
                f"(threshold: {the_maximum_percent_names_with_special_characters:.0%}). "
                f"{special_char_count} of {total_practitioners} affected"
            )

        if not failures:
            return True

        return "; ".join(failures)
