"""
Address Completeness Validation

Validates that organizations and locations have complete, usable physical addresses
with all critical components present: street address (line), city, state, and postal code.

Incomplete addresses represent a significant trust barrier in healthcare directories because
they suggest either poor data maintenance or entities that may not want to be easily found.
Healthcare system participants must be able to physically locate entities for patient referrals,
emergency situations, or business verification.

Beyond simple presence checks, this test also validates that state abbreviations are valid
US two-letter state codes. A sudden increase in incomplete addresses may indicate upstream
data quality degradation or problems with address parsing and normalization logic in the
ingestion pipeline.

This test covers both Organization and Location FHIR resource types, since both are expected
to carry physical address information in a healthcare directory context.

TODO: Add validation of a subset of addresses using SmartyStreets (et al) validated data.
"""
from src.utils.inlaw import InLaw


# Valid US state and territory two-letter abbreviations for address coherence checking.
# Includes all 50 states, DC, and common US territories that appear in healthcare data.
VALID_US_STATE_ABBREVIATIONS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
}


class ValidateAddressCompleteness(InLaw):
    """Validate that organizations and locations have complete, usable physical addresses.

    Checks for the presence of all critical address components (street line, city, state,
    postal code) across both Organization and Location FHIR resources. Also validates
    that state values are recognizable US two-letter abbreviations.

    A failure here indicates that a meaningful fraction of directory entries lack the
    address information needed for real-world use — patient referrals, business
    verification, or emergency contact — undermining trust in the directory overall.
    """

    title = "Organizations and Locations should have complete physical addresses with valid components"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate address completeness for Organization and Location resources.

        Runs two sets of checks:
        1. What percentage of resources have ALL critical address fields populated?
           (street line, city, state, postal code)
        2. Among resources that do have a state value, what percentage use a valid
           US two-letter state abbreviation?

        Both Organizations and Locations are evaluated independently and the worst-case
        result across both resource types determines the overall pass/fail.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (optional, thresholds are defined internally)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ──────────────────────────────────────────────────────────────
        # Threshold variables — defined here so every assumption is
        # visible and easy to adjust without hunting through logic.
        # ──────────────────────────────────────────────────────────────

        # The minimum fraction of resources that must have a complete
        # address (all four components: street line, city, state,
        # postal code). 80% is a reasonable starting point: most
        # healthcare directories are expected to have addresses for
        # the vast majority of orgs/locations, but some edge cases
        # (virtual-only orgs, data-in-progress) justify a buffer.
        the_minimum_percent_with_complete_address = 0.80

        # The minimum fraction of resources (among those that DO have
        # a state value) whose state is a valid US two-letter code.
        # Set high at 95% because state is a constrained value set.
        # The 5% tolerance covers occasional non-US entries or legacy
        # data quirks.
        the_minimum_percent_with_valid_state_abbreviation = 0.95

        # Minimum resource count before percentage checks are
        # meaningful.  Below this, a single missing record swings
        # the percentage too much, so we skip rather than fail.
        the_minimum_resource_count_for_meaningful_test = 10

        failures = []

        # ── Check 1: Organization address completeness ───────────
        # Organization.address is an ARRAY of Address objects in FHIR.
        # We inspect the first entry (index 0) as the primary address.
        org_sql = """
            SELECT
                json_extract_string(resource, '$.id') AS resource_id,
                json_extract_string(resource, '$.address[0].line[0]') AS address_line,
                json_extract_string(resource, '$.address[0].city') AS address_city,
                json_extract_string(resource, '$.address[0].state') AS address_state,
                json_extract_string(resource, '$.address[0].postalCode') AS address_postal_code
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
        """
        org_failure = ValidateAddressCompleteness._check_resource_addresses(
            engine=engine,
            sql=org_sql,
            resource_type_label="Organization",
            min_complete_pct=the_minimum_percent_with_complete_address,
            min_valid_state_pct=the_minimum_percent_with_valid_state_abbreviation,
            min_resource_count=the_minimum_resource_count_for_meaningful_test,
        )
        if org_failure is not None:
            failures.append(org_failure)

        # ── Check 2: Location address completeness ───────────────
        # Location.address is a SINGLE Address object (not an array),
        # so the JSON path does not use an array index.
        loc_sql = """
            SELECT
                json_extract_string(resource, '$.id') AS resource_id,
                json_extract_string(resource, '$.address.line[0]') AS address_line,
                json_extract_string(resource, '$.address.city') AS address_city,
                json_extract_string(resource, '$.address.state') AS address_state,
                json_extract_string(resource, '$.address.postalCode') AS address_postal_code
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Location'
        """
        loc_failure = ValidateAddressCompleteness._check_resource_addresses(
            engine=engine,
            sql=loc_sql,
            resource_type_label="Location",
            min_complete_pct=the_minimum_percent_with_complete_address,
            min_valid_state_pct=the_minimum_percent_with_valid_state_abbreviation,
            min_resource_count=the_minimum_resource_count_for_meaningful_test,
        )
        if loc_failure is not None:
            failures.append(loc_failure)

        if not failures:
            return True

        return "; ".join(failures)

    @staticmethod
    def _check_resource_addresses(
        *,
        engine,
        sql,
        resource_type_label,
        min_complete_pct,
        min_valid_state_pct,
        min_resource_count,
    ):
        """Evaluate address completeness and state validity for one resource type.

        Internal helper used by run() to avoid duplicating the same analysis
        logic for Organizations and Locations.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            sql: SQL query returning resource_id, address_line, address_city,
                 address_state, and address_postal_code columns
            resource_type_label: Human-readable label (e.g. "Organization")
            min_complete_pct: Minimum fraction with all four address components
            min_valid_state_pct: Minimum fraction with a valid US state code
            min_resource_count: Skip check if fewer resources than this

        Returns:
            None if the check passes, or an error message string
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        df = gx_df.active_batch.data.dataframe

        total_resource_count = len(df)

        # Too few resources — percentages would be misleading
        if total_resource_count < min_resource_count:
            return None

        sub_failures = []

        # ── Sub-check A: All four address components present ─────
        # A "complete" address has non-null, non-empty-string values
        # for street line, city, state, AND postal code.
        has_complete_address = (
            df['address_line'].notna()
            & (df['address_line'].str.strip() != '')
            & df['address_city'].notna()
            & (df['address_city'].str.strip() != '')
            & df['address_state'].notna()
            & (df['address_state'].str.strip() != '')
            & df['address_postal_code'].notna()
            & (df['address_postal_code'].str.strip() != '')
        )
        complete_count = int(has_complete_address.sum())
        complete_pct = complete_count / total_resource_count

        if complete_pct < min_complete_pct:
            sub_failures.append(
                f"{resource_type_label} complete address rate "
                f"{complete_pct:.1%} ({complete_count}/{total_resource_count}) "
                f"is below minimum threshold of {min_complete_pct:.0%}"
            )

        # ── Sub-check B: Valid US state abbreviation ─────────────
        # Among resources with a non-null, non-empty state, verify
        # the value is a recognized two-letter US state/territory.
        has_state = (
            df['address_state'].notna()
            & (df['address_state'].str.strip() != '')
        )
        resources_with_state = df[has_state]
        state_count = len(resources_with_state)

        if state_count >= min_resource_count:
            valid_state_mask = (
                resources_with_state['address_state']
                .str.strip()
                .str.upper()
                .isin(VALID_US_STATE_ABBREVIATIONS)
            )
            valid_state_count = int(valid_state_mask.sum())
            valid_state_pct = valid_state_count / state_count

            if valid_state_pct < min_valid_state_pct:
                sub_failures.append(
                    f"{resource_type_label} valid state abbreviation rate "
                    f"{valid_state_pct:.1%} ({valid_state_count}/{state_count}) "
                    f"is below minimum threshold of {min_valid_state_pct:.0%}"
                )

        if sub_failures:
            return "; ".join(sub_failures)

        return None
