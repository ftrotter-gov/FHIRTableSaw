"""
Identifier Diversity and Coverage Validation for Practitioner Resources.

This module analyzes the diversity and completeness of identifiers across
practitioners, checking for appropriate identifier types such as NPI
(National Provider Identifier), state license numbers, DEA numbers,
Tax IDs, and other jurisdiction-specific identifiers.

Resources with multiple, cross-verifiable identifiers are inherently more
trustworthy because they can be validated against multiple authoritative
sources. Entities with no identifiers or only a single identifier type are
harder to verify and may represent incomplete or suspect data.

The tests also check for identifier format validity to ensure identifiers
are well-formed and not placeholder values. In the US context, virtually
all legitimate practitioners should have an NPI.

Tests in this file:
    1. ValidatePractitionerNpiCoverage
       - Checks that a minimum percentage of practitioners have an NPI.
    2. ValidatePractitionerMultipleIdentifiers
       - Checks that a minimum percentage of practitioners have more than
         one distinct identifier type (system), enabling cross-verification.
    3. ValidateIdentifierTypeDiversity
       - Checks that the dataset contains at least a minimum number of
         distinct identifier systems.
    4. ValidateNoPlaceholderIdentifiers
       - Checks that the percentage of identifiers with placeholder-like
         values is below a maximum threshold.
    5. ValidateNpiFormatInIdentifiers
       - Checks that NPI values in the identifier array are well-formed
         10-digit strings.
"""
from src.utils.inlaw import InLaw


# Well-known FHIR identifier system URI for NPI
NPI_SYSTEM_URI = "http://hl7.org/fhir/sid/us-npi"


class ValidatePractitionerNpiCoverage(InLaw):
    """Validate that a sufficient percentage of practitioners carry an NPI.

    In the US healthcare context, virtually all legitimate practitioners
    should have a National Provider Identifier (NPI). A low NPI coverage
    rate suggests incomplete data ingestion or a non-US data source that
    may need different validation rules.
    """

    title = "Sufficient percentage of practitioners should have an NPI identifier"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check what percentage of Practitioner resources have at least one
        identifier whose system matches the NPI URI.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (thresholds are defined as
                    internal variables below)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # -----------------------------------------------------------------
        # Threshold variables
        # -----------------------------------------------------------------

        # The minimum fraction (0.0-1.0) of practitioners that must have
        # an NPI identifier. In the US National Directory context, nearly
        # all practitioners should have one. Set conservatively at 85%.
        the_minimum_percent_practitioners_with_npi = 0.85

        # -----------------------------------------------------------------
        # Step 1: Count total practitioners
        # -----------------------------------------------------------------
        total_sql = """
            SELECT COUNT(*) AS total_practitioners
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_practitioners = int(total_df['total_practitioners'].iloc[0])

        if total_practitioners == 0:
            return (
                "validate_identifier_diversity.py Error: "
                "No Practitioner resources found in fhir_resources"
            )

        # -----------------------------------------------------------------
        # Step 2: Count practitioners with at least one NPI identifier
        # -----------------------------------------------------------------
        npi_sql = f"""
            SELECT COUNT(DISTINCT json_extract_string(resource, '$.id'))
                   AS practitioners_with_npi
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND json_extract_string(
                      ident.identifier_element, '$.system'
                  ) = '{NPI_SYSTEM_URI}'
        """
        npi_gx_df = InLaw.to_gx_dataframe(npi_sql, engine)
        npi_df = npi_gx_df.active_batch.data.dataframe
        practitioners_with_npi = int(npi_df['practitioners_with_npi'].iloc[0])

        # -----------------------------------------------------------------
        # Step 3: Evaluate
        # -----------------------------------------------------------------
        actual_npi_fraction = practitioners_with_npi / total_practitioners

        if actual_npi_fraction >= the_minimum_percent_practitioners_with_npi:
            return True

        actual_pct = actual_npi_fraction * 100
        threshold_pct = the_minimum_percent_practitioners_with_npi * 100
        return (
            f"NPI coverage {actual_pct:.1f}% is below the minimum "
            f"threshold of {threshold_pct:.1f}% "
            f"({practitioners_with_npi} of {total_practitioners} practitioners)"
        )


class ValidatePractitionerMultipleIdentifiers(InLaw):
    """Validate that a sufficient percentage of practitioners have multiple
    distinct identifier types (systems).

    Practitioners with multiple cross-verifiable identifiers (e.g. NPI plus
    a state license number) are inherently more trustworthy because they
    can be validated against multiple authoritative sources.
    """

    title = "Sufficient percentage of practitioners should have multiple identifier types"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check what percentage of Practitioner resources have identifiers
        from two or more distinct systems.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # -----------------------------------------------------------------
        # Threshold variables
        # -----------------------------------------------------------------

        # The minimum fraction of practitioners that should have identifiers
        # from at least two distinct systems. Set at 40% as a reasonable
        # baseline; many directories include NPI plus at least one other
        # identifier type for a significant portion of their practitioners.
        the_minimum_percent_practitioners_with_multiple_identifiers = 0.40

        # -----------------------------------------------------------------
        # Step 1: Count total practitioners
        # -----------------------------------------------------------------
        total_sql = """
            SELECT COUNT(*) AS total_practitioners
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_practitioners = int(total_df['total_practitioners'].iloc[0])

        if total_practitioners == 0:
            return (
                "validate_identifier_diversity.py Error: "
                "No Practitioner resources found in fhir_resources"
            )

        # -----------------------------------------------------------------
        # Step 2: Count practitioners with >= 2 distinct identifier systems
        # -----------------------------------------------------------------
        multi_id_sql = """
            SELECT COUNT(*) AS practitioners_with_multiple_id_types
            FROM (
                SELECT
                    json_extract_string(resource, '$.id') AS practitioner_id,
                    COUNT(DISTINCT json_extract_string(
                        ident.identifier_element, '$.system'
                    )) AS distinct_systems
                FROM fhir_resources,
                     LATERAL UNNEST(
                         CAST(json_extract(resource, '$.identifier') AS JSON[])
                     ) AS ident(identifier_element)
                WHERE json_extract_string(resource, '$.resourceType')
                      = 'Practitioner'
                GROUP BY json_extract_string(resource, '$.id')
                HAVING COUNT(DISTINCT json_extract_string(
                    ident.identifier_element, '$.system'
                )) >= 2
            ) AS practitioners_with_diverse_identifiers
        """
        multi_gx_df = InLaw.to_gx_dataframe(multi_id_sql, engine)
        multi_df = multi_gx_df.active_batch.data.dataframe
        practitioners_with_multiple = int(
            multi_df['practitioners_with_multiple_id_types'].iloc[0]
        )

        # -----------------------------------------------------------------
        # Step 3: Evaluate
        # -----------------------------------------------------------------
        actual_fraction = practitioners_with_multiple / total_practitioners

        if actual_fraction >= the_minimum_percent_practitioners_with_multiple_identifiers:
            return True

        actual_pct = actual_fraction * 100
        threshold_pct = (
            the_minimum_percent_practitioners_with_multiple_identifiers * 100
        )
        return (
            f"Multiple-identifier coverage {actual_pct:.1f}% is below the "
            f"minimum threshold of {threshold_pct:.1f}% "
            f"({practitioners_with_multiple} of "
            f"{total_practitioners} practitioners)"
        )


class ValidateIdentifierTypeDiversity(InLaw):
    """Validate that the dataset contains a minimum number of distinct
    identifier systems across all practitioner resources.

    A healthy directory should reference multiple authoritative identifier
    systems (e.g. NPI, state license boards, DEA). Having very few distinct
    systems suggests the data source is narrow or incomplete.
    """

    title = "Dataset should contain multiple distinct identifier systems"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Count the total number of distinct identifier systems present
        across all Practitioner resources in the dataset.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # -----------------------------------------------------------------
        # Threshold variables
        # -----------------------------------------------------------------

        # The minimum number of distinct identifier system URIs that should
        # appear across all Practitioner resources. A value of 2 is very
        # conservative; most US directories will have at least NPI plus
        # one or more state license systems.
        the_minimum_distinct_identifier_types = 2

        # -----------------------------------------------------------------
        # Step 1: Count distinct identifier systems
        # -----------------------------------------------------------------
        diversity_sql = """
            SELECT COUNT(DISTINCT json_extract_string(
                       ident.identifier_element, '$.system'
                   )) AS distinct_identifier_systems
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
        """
        diversity_gx_df = InLaw.to_gx_dataframe(diversity_sql, engine)
        diversity_df = diversity_gx_df.active_batch.data.dataframe
        distinct_systems_count = int(
            diversity_df['distinct_identifier_systems'].iloc[0]
        )

        # -----------------------------------------------------------------
        # Step 2: Evaluate
        # -----------------------------------------------------------------
        if distinct_systems_count >= the_minimum_distinct_identifier_types:
            return True

        return (
            f"Only {distinct_systems_count} distinct identifier system(s) "
            f"found across all practitioners; minimum required is "
            f"{the_minimum_distinct_identifier_types}"
        )


class ValidateNoPlaceholderIdentifiers(InLaw):
    """Validate that the percentage of placeholder or obviously invalid
    identifier values is below an acceptable threshold.

    Placeholder identifiers such as "000", "UNKNOWN", "TEST", "N/A",
    all-zeros patterns, or very short strings indicate data quality
    problems and reduce trust in the directory.
    """

    title = "Placeholder/invalid identifier values should be rare"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check the fraction of identifier values that look like
        placeholders or obviously invalid data.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # -----------------------------------------------------------------
        # Threshold variables
        # -----------------------------------------------------------------

        # The maximum fraction (0.0-1.0) of identifier values that may be
        # placeholder-like. 2% is a tight but realistic limit; real-world
        # directories occasionally have a small number of stubs.
        the_maximum_percent_placeholder_identifiers = 0.02

        # -----------------------------------------------------------------
        # Step 1: Count total identifier values across all practitioners
        # -----------------------------------------------------------------
        total_identifiers_sql = """
            SELECT COUNT(*) AS total_identifier_values
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
              AND json_extract_string(
                      ident.identifier_element, '$.value'
                  ) IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_identifiers_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_identifier_values = int(
            total_df['total_identifier_values'].iloc[0]
        )

        if total_identifier_values == 0:
            return (
                "validate_identifier_diversity.py Error: "
                "No identifier values found across Practitioner resources"
            )

        # -----------------------------------------------------------------
        # Step 2: Count identifier values that look like placeholders
        #
        # Placeholder patterns detected:
        #   - Value is all zeros (e.g. "0000000000", "000")
        #   - Value matches common placeholder text (case-insensitive):
        #     UNKNOWN, TEST, N/A, NA, NONE, PENDING, TBD, TEMP, etc.
        #   - Value is very short (1-2 characters), which is too short
        #     for any real healthcare identifier
        # -----------------------------------------------------------------
        placeholder_sql = """
            SELECT COUNT(*) AS placeholder_identifier_count
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
              AND json_extract_string(
                      ident.identifier_element, '$.value'
                  ) IS NOT NULL
              AND (
                  REGEXP_MATCHES(
                      json_extract_string(
                          ident.identifier_element, '$.value'
                      ),
                      '^0+$'
                  )
                  OR UPPER(TRIM(json_extract_string(
                      ident.identifier_element, '$.value'
                  ))) IN (
                      'UNKNOWN', 'TEST', 'N/A', 'NA', 'NONE',
                      'PENDING', 'TBD', 'TEMP', 'PLACEHOLDER',
                      'NULL', 'INVALID', 'MISSING'
                  )
                  OR LENGTH(TRIM(json_extract_string(
                      ident.identifier_element, '$.value'
                  ))) <= 2
              )
        """
        placeholder_gx_df = InLaw.to_gx_dataframe(placeholder_sql, engine)
        placeholder_df = placeholder_gx_df.active_batch.data.dataframe
        placeholder_count = int(
            placeholder_df['placeholder_identifier_count'].iloc[0]
        )

        # -----------------------------------------------------------------
        # Step 3: Evaluate
        # -----------------------------------------------------------------
        actual_placeholder_fraction = (
            placeholder_count / total_identifier_values
        )

        if actual_placeholder_fraction <= the_maximum_percent_placeholder_identifiers:
            return True

        actual_pct = actual_placeholder_fraction * 100
        threshold_pct = the_maximum_percent_placeholder_identifiers * 100
        return (
            f"Placeholder identifier rate {actual_pct:.2f}% exceeds "
            f"maximum threshold of {threshold_pct:.1f}% "
            f"({placeholder_count} of "
            f"{total_identifier_values} identifier values)"
        )


class ValidateNpiFormatInIdentifiers(InLaw):
    """Validate that NPI values found in the identifier array are
    well-formed 10-digit numeric strings.

    Even if a practitioner has an identifier tagged with the NPI system
    URI, the value itself might be malformed (wrong length, non-numeric,
    etc.). This test ensures that NPI values in the identifier array are
    actually valid-looking NPIs.
    """

    title = "NPI identifiers should be well-formed 10-digit numbers"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Check that all identifiers whose system matches the NPI URI
        have a value that is exactly 10 digits.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # -----------------------------------------------------------------
        # Step 1: Count total NPI-tagged identifiers
        # -----------------------------------------------------------------
        total_npi_sql = f"""
            SELECT COUNT(*) AS total_npi_identifiers
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
              AND json_extract_string(
                      ident.identifier_element, '$.system'
                  ) = '{NPI_SYSTEM_URI}'
        """
        total_npi_gx_df = InLaw.to_gx_dataframe(total_npi_sql, engine)
        total_npi_df = total_npi_gx_df.active_batch.data.dataframe
        total_npi_identifiers = int(
            total_npi_df['total_npi_identifiers'].iloc[0]
        )

        if total_npi_identifiers == 0:
            # No NPI identifiers at all — the NPI coverage test above
            # would catch that issue; this format test is not applicable.
            return True

        # -----------------------------------------------------------------
        # Step 2: Count NPI identifiers with malformed values
        #
        # A well-formed NPI is exactly 10 decimal digits.
        # -----------------------------------------------------------------
        malformed_npi_sql = f"""
            SELECT COUNT(*) AS malformed_npi_count
            FROM fhir_resources,
                 LATERAL UNNEST(
                     CAST(json_extract(resource, '$.identifier') AS JSON[])
                 ) AS ident(identifier_element)
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
              AND json_extract_string(
                      ident.identifier_element, '$.system'
                  ) = '{NPI_SYSTEM_URI}'
              AND NOT REGEXP_MATCHES(
                  json_extract_string(
                      ident.identifier_element, '$.value'
                  ),
                  '^\\d{{10}}$'
              )
        """
        malformed_gx_df = InLaw.to_gx_dataframe(malformed_npi_sql, engine)
        malformed_df = malformed_gx_df.active_batch.data.dataframe
        malformed_npi_count = int(
            malformed_df['malformed_npi_count'].iloc[0]
        )

        # -----------------------------------------------------------------
        # Step 3: Evaluate — we expect zero malformed NPI values
        # -----------------------------------------------------------------
        if malformed_npi_count == 0:
            return True

        return (
            f"Found {malformed_npi_count} NPI identifier(s) with malformed "
            f"values (expected exactly 10 digits) out of "
            f"{total_npi_identifiers} total NPI identifiers"
        )


