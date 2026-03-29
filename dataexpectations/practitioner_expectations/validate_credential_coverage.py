"""
Practitioner Credential Coverage

Measures the percentage of practitioners who have at least one qualification
or credential listed in their FHIR Practitioner.qualification field, and
assesses the richness of that qualification data (issuer, dates, identifiers).

Credentials are fundamental to trust in healthcare - they demonstrate training,
certification, and legal authority to practice. A directory where most
practitioners lack credential information is of limited value for verification
purposes. Higher-trust directories will include issuer information, dates of
issuance, and credential identifiers that enable third-party verification.

A sudden drop in credential coverage may indicate a data processing change
that accidentally stripped qualification information, while consistently low
coverage suggests the directory source data lacks this critical trust dimension.

FHIR R4 Practitioner.qualification structure:
  - qualification[].code          (CodeableConcept - the credential itself)
  - qualification[].identifier    (Identifier[] - business identifiers)
  - qualification[].period        (Period - when the qualification is valid)
  - qualification[].issuer        (Reference(Organization) - who issued it)
"""
from src.utils.inlaw import InLaw


class ValidateCredentialCoverage(InLaw):
    """Validate that a sufficient percentage of practitioners have credential data."""

    title = "Practitioner credential coverage should meet minimum threshold"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate credential coverage across practitioners.

        Checks what percentage of practitioners have at least one entry in
        their FHIR Practitioner.qualification array. A directory with very
        low credential coverage cannot be trusted for provider verification.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (passed by InLaw.run_all)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # Minimum percentage of practitioners that must have at least one
        # qualification entry. 50% is a pragmatic lower bound: a credible
        # healthcare directory should have credentials for at least half its
        # practitioners, though some directories also include non-credentialed
        # staff.
        the_minimum_percent_practitioners_with_any_qualification = 0.50

        # Step 1: Count total practitioners
        total_practitioners_sql = """
            SELECT COUNT(*) AS total_practitioner_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
        """
        total_gx_df = InLaw.to_gx_dataframe(total_practitioners_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_practitioner_count = int(total_df['total_practitioner_count'].iloc[0])

        if total_practitioner_count == 0:
            return ("validate_credential_coverage.py Error: "
                    "No Practitioner resources found in fhir_resources table")

        # Step 2: Count practitioners with at least one qualification
        practitioners_with_qualifications_sql = """
            SELECT COUNT(*) AS practitioners_with_qualification_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Practitioner'
              AND json_array_length(
                    json_extract(resource, '$.qualification')
                  ) > 0
        """
        qual_gx_df = InLaw.to_gx_dataframe(
            practitioners_with_qualifications_sql, engine
        )
        qual_df = qual_gx_df.active_batch.data.dataframe
        practitioners_with_qualification_count = int(
            qual_df['practitioners_with_qualification_count'].iloc[0]
        )

        # Evaluate
        actual_percent = (
            practitioners_with_qualification_count / total_practitioner_count
        )

        if actual_percent < the_minimum_percent_practitioners_with_any_qualification:
            return (
                f"validate_credential_coverage.py: Only "
                f"{actual_percent:.1%} of practitioners have qualification "
                f"data ({practitioners_with_qualification_count}"
                f"/{total_practitioner_count}), below minimum threshold of "
                f"{the_minimum_percent_practitioners_with_any_qualification:.0%}"
            )

        return True


class ValidateCredentialRichness(InLaw):
    """Validate richness of qualification data (issuer, dates, identifiers)."""

    title = ("Practitioner credential richness should meet minimum "
             "thresholds for issuer, dates, and identifiers")

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate the richness of qualification data across practitioners.

        Beyond mere presence, higher-trust directories include issuer info,
        period/dates, and identifiers. This test unnests qualification
        entries and checks what fraction include these enrichment fields.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (passed by InLaw.run_all)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # Minimum % of qualifications with issuer. 30% is moderate since
        # many source systems omit issuer data.
        the_minimum_percent_qualifications_with_issuer = 0.30

        # Minimum % with period/date info. 25% is lenient since many
        # directories lack explicit validity periods.
        the_minimum_percent_qualifications_with_dates = 0.25

        # Minimum % with at least one identifier (e.g. license number).
        # 20% is lenient since many store code but not identifier.
        the_minimum_percent_qualifications_with_identifier = 0.20

        counts = ValidateCredentialRichness._get_richness_counts(
            engine=engine
        )
        if counts is None:
            return (
                "validate_credential_coverage.py Error: No qualification "
                "entries found - cannot assess richness"
            )

        total = counts['total']
        failures = []

        pct_issuer = counts['issuer'] / total
        if pct_issuer < the_minimum_percent_qualifications_with_issuer:
            failures.append(
                f"issuer coverage {pct_issuer:.1%} "
                f"({counts['issuer']}/{total}) below minimum "
                f"{the_minimum_percent_qualifications_with_issuer:.0%}"
            )

        pct_dates = counts['dates'] / total
        if pct_dates < the_minimum_percent_qualifications_with_dates:
            failures.append(
                f"date/period coverage {pct_dates:.1%} "
                f"({counts['dates']}/{total}) below minimum "
                f"{the_minimum_percent_qualifications_with_dates:.0%}"
            )

        pct_id = counts['identifier'] / total
        if pct_id < the_minimum_percent_qualifications_with_identifier:
            failures.append(
                f"identifier coverage {pct_id:.1%} "
                f"({counts['identifier']}/{total}) below minimum "
                f"{the_minimum_percent_qualifications_with_identifier:.0%}"
            )

        if failures:
            return (
                "validate_credential_coverage.py: Credential richness "
                "below thresholds: " + "; ".join(failures)
            )

        return True

    @staticmethod
    def _get_richness_counts(*, engine):
        """
        Query DuckDB for qualification richness counts.

        Runs separate simple SQL queries for each richness dimension
        rather than one complex combined query, per project conventions.

        Args:
            engine: SQLAlchemy engine for DuckDB connection

        Returns:
            dict with keys total, issuer, dates, identifier; or None
            if no qualification entries exist at all.
        """
        base_from = """
            FROM fhir_resources,
                 unnest(json_extract(resource, '$.qualification'))
                     AS qualification_entry
            WHERE json_extract_string(resource, '$.resourceType')
                  = 'Practitioner'
        """

        total_sql = f"SELECT COUNT(*) AS cnt {base_from}"
        total_gx = InLaw.to_gx_dataframe(total_sql, engine)
        total = int(total_gx.active_batch.data.dataframe['cnt'].iloc[0])
        if total == 0:
            return None

        issuer_sql = f"""
            SELECT COUNT(*) AS cnt {base_from}
              AND json_extract_string(
                      qualification_entry, '$.issuer'
                  ) IS NOT NULL
        """
        issuer_gx = InLaw.to_gx_dataframe(issuer_sql, engine)
        issuer_count = int(
            issuer_gx.active_batch.data.dataframe['cnt'].iloc[0]
        )

        dates_sql = f"""
            SELECT COUNT(*) AS cnt {base_from}
              AND json_extract_string(
                      qualification_entry, '$.period'
                  ) IS NOT NULL
        """
        dates_gx = InLaw.to_gx_dataframe(dates_sql, engine)
        dates_count = int(
            dates_gx.active_batch.data.dataframe['cnt'].iloc[0]
        )

        identifier_sql = f"""
            SELECT COUNT(*) AS cnt {base_from}
              AND json_array_length(
                      json_extract(qualification_entry, '$.identifier')
                  ) > 0
        """
        id_gx = InLaw.to_gx_dataframe(identifier_sql, engine)
        identifier_count = int(
            id_gx.active_batch.data.dataframe['cnt'].iloc[0]
        )

        return {
            'total': total,
            'issuer': issuer_count,
            'dates': dates_count,
            'identifier': identifier_count,
        }
