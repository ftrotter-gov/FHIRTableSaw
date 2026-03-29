"""
Contact Information Completeness

Measures the percentage of organizations that have at least one viable contact
method (phone number, fax, or email) populated in their telecom fields. Being
able to contact a healthcare organization is fundamental to interoperability -
without contact information, referrals cannot be coordinated, questions cannot
be answered, and trust cannot be established through direct communication.

This test distinguishes between different contact types and flags organizations
that have only fax numbers (increasingly obsolete) or that rely solely on a
single contact method. Multiple contact channels indicate organizational
maturity and reliability. Organizations lacking any contact information should
be treated as red flags within a healthcare directory.
"""
from src.utils.inlaw import InLaw


class ValidateContactCompleteness(InLaw):
    """Validate that organizations have viable contact methods in their telecom fields.

    Checks two tiers of contact completeness:
    1. Any contact at all (phone, fax, or email)
    2. Non-fax contact (phone or email) since fax is increasingly obsolete

    Uses the FHIR Organization resource's telecom array, examining the 'system'
    field of each telecom entry for values of 'phone', 'fax', or 'email'.
    """

    title = "Organizations should have viable contact information in telecom fields"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate contact information completeness across organizations.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (accepted for InLaw compatibility)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold variables ──────────────────────────────────────
        # Each threshold is a fraction (0.0-1.0) representing the minimum
        # acceptable percentage of organizations meeting the criterion.

        # At least 90% of organizations should have *some* contact method
        # (phone, fax, or email). A directory where more than 10% of orgs
        # are completely unreachable is not fit for operational use.
        the_minimum_percent_orgs_with_any_contact = 0.90

        # At least 70% of organizations should have a non-fax contact
        # (phone or email). Fax is increasingly obsolete; orgs reachable
        # only by fax represent a degraded contact experience. 70% is a
        # reasonable floor since many legacy healthcare orgs still rely
        # heavily on fax.
        the_minimum_percent_orgs_with_non_fax_contact = 0.70

        # ── Step 1: Count total organizations ────────────────────────
        total_orgs_sql = """
            SELECT COUNT(*) AS total_org_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
        """
        total_gx = InLaw.to_gx_dataframe(total_orgs_sql, engine)
        total_df = total_gx.active_batch.data.dataframe
        total_org_count = int(total_df['total_org_count'].iloc[0])

        if total_org_count == 0:
            return (
                "validate_contact_completeness.py Error: "
                "No Organization resources found in fhir_resources table"
            )

        # ── Step 2: Orgs with any contact (phone, fax, or email) ─────
        any_contact_sql = """
            SELECT COUNT(DISTINCT json_extract_string(org.resource, '$.id'))
                AS orgs_with_any_contact
            FROM fhir_resources AS org,
                 unnest(json_extract(org.resource, '$.telecom'))
                     AS telecom_entry
            WHERE json_extract_string(org.resource, '$.resourceType')
                      = 'Organization'
              AND json_extract_string(telecom_entry, '$.system')
                      IN ('phone', 'fax', 'email')
        """
        any_gx = InLaw.to_gx_dataframe(any_contact_sql, engine)
        any_df = any_gx.active_batch.data.dataframe
        orgs_with_any_contact = int(any_df['orgs_with_any_contact'].iloc[0])

        # ── Step 3: Orgs with non-fax contact (phone or email) ───────
        non_fax_sql = """
            SELECT COUNT(DISTINCT json_extract_string(org.resource, '$.id'))
                AS orgs_with_non_fax_contact
            FROM fhir_resources AS org,
                 unnest(json_extract(org.resource, '$.telecom'))
                     AS telecom_entry
            WHERE json_extract_string(org.resource, '$.resourceType')
                      = 'Organization'
              AND json_extract_string(telecom_entry, '$.system')
                      IN ('phone', 'email')
        """
        nonfax_gx = InLaw.to_gx_dataframe(non_fax_sql, engine)
        nonfax_df = nonfax_gx.active_batch.data.dataframe
        orgs_with_non_fax = int(nonfax_df['orgs_with_non_fax_contact'].iloc[0])

        return ValidateContactCompleteness._evaluate_thresholds(
            total_org_count=total_org_count,
            orgs_with_any_contact=orgs_with_any_contact,
            orgs_with_non_fax_contact=orgs_with_non_fax,
            min_any_contact=the_minimum_percent_orgs_with_any_contact,
            min_non_fax=the_minimum_percent_orgs_with_non_fax_contact,
        )

    @staticmethod
    def _evaluate_thresholds(
        *,
        total_org_count,
        orgs_with_any_contact,
        orgs_with_non_fax_contact,
        min_any_contact,
        min_non_fax,
    ):
        """Compare computed contact percentages against threshold values.

        Args:
            total_org_count: Total number of Organization resources
            orgs_with_any_contact: Orgs with phone, fax, or email
            orgs_with_non_fax_contact: Orgs with phone or email
            min_any_contact: Minimum fraction for any-contact check
            min_non_fax: Minimum fraction for non-fax-contact check

        Returns:
            True if all thresholds pass, or a failure message string
        """
        pct_any = orgs_with_any_contact / total_org_count
        pct_non_fax = orgs_with_non_fax_contact / total_org_count
        fax_only_count = orgs_with_any_contact - orgs_with_non_fax_contact
        no_contact_count = total_org_count - orgs_with_any_contact

        failures = []

        if pct_any < min_any_contact:
            failures.append(
                f"Only {pct_any:.1%} of organizations have any contact "
                f"method (phone/fax/email), below the "
                f"{min_any_contact:.0%} minimum. "
                f"{no_contact_count} of {total_org_count} orgs have "
                f"no contact info at all"
            )

        if pct_non_fax < min_non_fax:
            failures.append(
                f"Only {pct_non_fax:.1%} of organizations have a "
                f"non-fax contact method (phone/email), below the "
                f"{min_non_fax:.0%} minimum. "
                f"{fax_only_count} orgs are reachable only by fax"
            )

        if failures:
            return "; ".join(failures)

        return True
