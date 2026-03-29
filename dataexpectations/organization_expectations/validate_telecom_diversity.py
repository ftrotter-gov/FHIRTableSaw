"""
Telecom System Diversity Validation

Validates that organizations have multiple types of contact mechanisms (phone, fax,
email, SMS, URL) rather than relying on a single communication channel.

Communication redundancy is important for operational reliability - if the primary
contact method fails, alternative channels enable continued coordination. Organizations
with diverse contact options demonstrate higher operational maturity and are generally
more reliable directory participants.

This test measures both the count and types of telecom systems per organization,
flagging organizations that have only one contact method or that rely exclusively
on outdated methods (fax only). It validates that different system types are actually
present - not just multiple instances of the same type (three phone numbers but no
email). This test serves as both a data quality indicator and a trust signal about
organizational sophistication and accessibility.

Reference: AI_Instructions/OtherTests.md - "Telecom System Diversity" section
"""
from src.utils.inlaw import InLaw


class ValidateTelecomDiversity(InLaw):
    """Validate that organizations have diverse telecom contact mechanisms.

    Checks four conditions:
    1. A minimum percentage of organizations should have more than one distinct
       telecom system type (e.g., both phone and fax, not just two phone numbers).
    2. A minimum percentage of organizations should have a phone number.
    3. A minimum percentage of organizations should have an email address.
    4. Only a small percentage of organizations should have fax as their sole
       contact method.
    """

    title = "Organizations should have diverse telecom contact types (phone, fax, email, etc.)"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate telecom system diversity across organizations.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (thresholds are defined internally)

        Returns:
            True if all telecom diversity checks pass,
            error message string describing which checks failed otherwise
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold variables ──────────────────────────────────────────
        # At least 50% of orgs should have >1 distinct telecom system type.
        # Moderate bar — many small clinics may only list a phone number.
        the_minimum_percent_orgs_with_multiple_telecom_types = 0.50

        # At least 80% of orgs should have a phone number listed.
        # Phone is the most universal contact method for healthcare orgs.
        the_minimum_percent_orgs_with_phone = 0.80

        # At least 40% of orgs should have an email address listed.
        # Email is increasingly important but many legacy orgs still lack it.
        the_minimum_percent_orgs_with_email = 0.40

        # No more than 5% of orgs should have fax as their ONLY method.
        # Fax-only orgs indicate poor accessibility and outdated practices.
        the_maximum_percent_orgs_with_only_fax = 0.05

        # Gather per-org telecom data then evaluate thresholds
        raw_telecom_df = ValidateTelecomDiversity._query_org_telecom_data(
            engine=engine,
        )

        total_org_count = raw_telecom_df['org_id'].nunique()
        if total_org_count == 0:
            return ("validate_telecom_diversity Error: "
                    "No Organization resources found in fhir_resources table")

        org_telecom_types = ValidateTelecomDiversity._build_org_type_sets(
            raw_telecom_df=raw_telecom_df,
        )

        failures = ValidateTelecomDiversity._evaluate_thresholds(
            org_telecom_types=org_telecom_types,
            total_org_count=total_org_count,
            the_minimum_percent_orgs_with_multiple_telecom_types=the_minimum_percent_orgs_with_multiple_telecom_types,
            the_minimum_percent_orgs_with_phone=the_minimum_percent_orgs_with_phone,
            the_minimum_percent_orgs_with_email=the_minimum_percent_orgs_with_email,
            the_maximum_percent_orgs_with_only_fax=the_maximum_percent_orgs_with_only_fax,
        )

        if not failures:
            return True

        return "validate_telecom_diversity FAIL: " + "; ".join(failures)

    @staticmethod
    def _query_org_telecom_data(*, engine):
        """Query per-organization telecom system types from fhir_resources.

        Returns a pandas DataFrame with columns [org_id, telecom_system].
        Each row is one telecom entry for one organization. Organizations
        without any telecom entries get a single row with NULL telecom_system
        so they are still counted in the total.

        Args:
            engine: SQLAlchemy engine for DuckDB connection

        Returns:
            pandas DataFrame with org_id and telecom_system columns
        """
        per_org_telecom_sql = """
            SELECT
                json_extract_string(
                    org_resource.resource, '$.id'
                ) AS org_id,
                json_extract_string(
                    telecom_entry.telecom_item, '$.system'
                ) AS telecom_system
            FROM fhir_resources AS org_resource,
            LATERAL (
                SELECT UNNEST(
                    CASE
                        WHEN json_array_length(
                            json_extract(org_resource.resource, '$.telecom')
                        ) > 0
                        THEN from_json_strict(
                            json_extract(org_resource.resource, '$.telecom'),
                            '["json"]'
                        )
                        ELSE [NULL::JSON]
                    END
                ) AS telecom_item
            ) AS telecom_entry
            WHERE json_extract_string(
                org_resource.resource, '$.resourceType'
            ) = 'Organization'
        """
        gx_df = InLaw.to_gx_dataframe(per_org_telecom_sql, engine)
        return gx_df.active_batch.data.dataframe

    @staticmethod
    def _build_org_type_sets(*, raw_telecom_df):
        """Build a DataFrame of distinct telecom system type sets per org.

        Groups the raw telecom data by org_id and collects the set of
        distinct non-null telecom system values for each organization.
        Adds a 'distinct_type_count' column with the size of each set.

        Args:
            raw_telecom_df: pandas DataFrame with org_id, telecom_system

        Returns:
            pandas DataFrame with columns:
                org_id, telecom_system_set (set of strings),
                distinct_type_count (int)
        """
        org_telecom_types = (
            raw_telecom_df
            .dropna(subset=['telecom_system'])
            .groupby('org_id')['telecom_system']
            .apply(set)
            .reset_index()
        )
        org_telecom_types.columns = ['org_id', 'telecom_system_set']
        org_telecom_types['distinct_type_count'] = (
            org_telecom_types['telecom_system_set'].apply(len)
        )
        return org_telecom_types

    @staticmethod
    def _evaluate_thresholds(
        *,
        org_telecom_types,
        total_org_count,
        the_minimum_percent_orgs_with_multiple_telecom_types,
        the_minimum_percent_orgs_with_phone,
        the_minimum_percent_orgs_with_email,
        the_maximum_percent_orgs_with_only_fax,
    ):
        """Evaluate each telecom diversity threshold and return failures.

        Computes actual percentages from the org_telecom_types DataFrame
        and compares them against the provided threshold values.

        Args:
            org_telecom_types: DataFrame with org_id, telecom_system_set,
                              distinct_type_count columns
            total_org_count: total number of unique organizations
            the_minimum_percent_orgs_with_multiple_telecom_types: min fraction
            the_minimum_percent_orgs_with_phone: min fraction with phone
            the_minimum_percent_orgs_with_email: min fraction with email
            the_maximum_percent_orgs_with_only_fax: max fraction fax-only

        Returns:
            List of failure message strings (empty if all checks pass)
        """
        failures = []

        # Orgs with more than one distinct telecom system type
        orgs_with_multiple_types_count = int(
            (org_telecom_types['distinct_type_count'] > 1).sum()
        )
        actual_pct_multiple = orgs_with_multiple_types_count / total_org_count
        if actual_pct_multiple < the_minimum_percent_orgs_with_multiple_telecom_types:
            failures.append(
                f"Only {actual_pct_multiple:.1%} of organizations have "
                f"multiple distinct telecom types "
                f"({orgs_with_multiple_types_count}/{total_org_count}), "
                f"minimum required is "
                f"{the_minimum_percent_orgs_with_multiple_telecom_types:.0%}"
            )

        # Orgs with a phone number
        orgs_with_phone_count = int(
            org_telecom_types['telecom_system_set']
            .apply(lambda type_set: 'phone' in type_set)
            .sum()
        )
        actual_pct_phone = orgs_with_phone_count / total_org_count
        if actual_pct_phone < the_minimum_percent_orgs_with_phone:
            failures.append(
                f"Only {actual_pct_phone:.1%} of organizations have "
                f"a phone number "
                f"({orgs_with_phone_count}/{total_org_count}), "
                f"minimum required is "
                f"{the_minimum_percent_orgs_with_phone:.0%}"
            )

        # Orgs with an email address
        orgs_with_email_count = int(
            org_telecom_types['telecom_system_set']
            .apply(lambda type_set: 'email' in type_set)
            .sum()
        )
        actual_pct_email = orgs_with_email_count / total_org_count
        if actual_pct_email < the_minimum_percent_orgs_with_email:
            failures.append(
                f"Only {actual_pct_email:.1%} of organizations have "
                f"an email "
                f"({orgs_with_email_count}/{total_org_count}), "
                f"minimum required is "
                f"{the_minimum_percent_orgs_with_email:.0%}"
            )

        # Orgs whose ONLY telecom type is fax
        orgs_with_only_fax_count = int(
            org_telecom_types['telecom_system_set']
            .apply(lambda type_set: type_set == {'fax'})
            .sum()
        )
        actual_pct_only_fax = orgs_with_only_fax_count / total_org_count
        if actual_pct_only_fax > the_maximum_percent_orgs_with_only_fax:
            failures.append(
                f"{actual_pct_only_fax:.1%} of organizations have fax "
                f"as their only contact method "
                f"({orgs_with_only_fax_count}/{total_org_count}), "
                f"maximum allowed is "
                f"{the_maximum_percent_orgs_with_only_fax:.0%}"
            )

        return failures
