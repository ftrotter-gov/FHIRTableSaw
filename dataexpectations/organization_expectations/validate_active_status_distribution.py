"""
Active Status Distribution

Monitors the ratio of active to inactive resources across all major resource types
(Organization, Practitioner, Location, PractitionerRole).

The expectation is that the vast majority of resources in an operational directory
should have an active status, with inactive resources representing a small, stable
percentage. Dramatic shifts in this ratio indicate either data quality problems or
significant real-world changes that require investigation.

Inactive resources should not be presented to directory consumers as viable options
for patient care. If the directory fails to properly mark inactive entities, it can
lead to failed referrals, patient safety issues, and loss of confidence in the
directory as a whole.

This test also flags resources missing the active field entirely, since unknown
status is as problematic as an incorrect one for downstream consumers.
"""
from src.utils.inlaw import InLaw


class ValidateActiveStatusDistribution(InLaw):
    """Validate active status distribution across resource types meets expectations.

    Checks Organization, Practitioner, Location, PractitionerRole.
    Each must meet its own minimum-active-percentage threshold, and no single
    resource type may exceed the maximum-inactive-percentage ceiling.
    Resources missing the active field are counted as not-active.
    """

    title = ("Active status distribution across resource types "
             "should show healthy active-to-inactive ratios")

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate active status distribution across major FHIR resource types.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (unused for thresholds;
                    all thresholds defined as internal variables below)

        Returns:
            True if all resource types pass, or error message string.
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold Variables ──────────────────────────────────────
        # Minimum fraction (0.0–1.0) of resources that must be active=true.

        # Organizations may have slightly more inactive entries due to
        # mergers, closures, and historical retention.
        the_minimum_percent_active_organizations = 0.85

        # Practitioners are expected to be overwhelmingly active because
        # credentialing processes typically deactivate departed providers.
        the_minimum_percent_active_practitioners = 0.90

        # Locations follow a similar pattern to organizations—closures and
        # relocations produce some inactive entries.
        the_minimum_percent_active_locations = 0.85

        # PractitionerRoles deactivate when a practitioner leaves a practice,
        # but overall the ratio should remain heavily active.
        the_minimum_percent_active_practitioner_roles = 0.80

        # Absolute ceiling: no resource type should have more than 20%
        # not-active (inactive + missing status).
        the_maximum_percent_inactive_any_resource_type = 0.20

        # Minimum records needed before enforcing the check. Avoids
        # misleading failures on resource types with very few records.
        the_minimum_resource_count_to_enforce = 10

        # ── Resource type → threshold mapping ────────────────────────
        resource_type_thresholds = {
            "Organization": the_minimum_percent_active_organizations,
            "Practitioner": the_minimum_percent_active_practitioners,
            "Location": the_minimum_percent_active_locations,
            "PractitionerRole": the_minimum_percent_active_practitioner_roles,
        }

        failures = []

        for resource_type_name, min_active in resource_type_thresholds.items():
            active_status_result = ValidateActiveStatusDistribution._check_resource_type(
                engine=engine,
                resource_type_name=resource_type_name,
                minimum_active_fraction=min_active,
                maximum_inactive_fraction=the_maximum_percent_inactive_any_resource_type,
                minimum_resource_count=the_minimum_resource_count_to_enforce,
            )
            if active_status_result is not True:
                failures.append(active_status_result)

        if not failures:
            return True

        return "; ".join(failures)

    @staticmethod
    def _check_resource_type(
        *,
        engine,
        resource_type_name: str,
        minimum_active_fraction: float,
        maximum_inactive_fraction: float,
        minimum_resource_count: int,
    ):
        """Check active-status distribution for a single FHIR resource type.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            resource_type_name: FHIR resourceType value (e.g. 'Organization')
            minimum_active_fraction: Minimum fraction that must be active
            maximum_inactive_fraction: Maximum fraction allowed not-active
            minimum_resource_count: Skip check if fewer records than this

        Returns:
            True if the resource type passes, or an error message string.
        """
        active_status_sql = f"""
            SELECT
                COUNT(*) AS total_count,
                COUNT(
                    CASE WHEN json_extract_string(resource, '$.active') = 'true'
                         THEN 1
                    END
                ) AS active_count,
                COUNT(
                    CASE WHEN json_extract_string(resource, '$.active') = 'false'
                         THEN 1
                    END
                ) AS inactive_count,
                COUNT(
                    CASE WHEN json_extract_string(resource, '$.active') IS NULL
                         THEN 1
                    END
                ) AS missing_active_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = '{resource_type_name}'
        """

        gx_df = InLaw.to_gx_dataframe(active_status_sql, engine)
        df = gx_df.active_batch.data.dataframe

        if len(df) == 0:
            return f"{resource_type_name}: query returned no rows"

        total_count = int(df["total_count"].iloc[0])
        active_count = int(df["active_count"].iloc[0])
        inactive_count = int(df["inactive_count"].iloc[0])
        missing_active_count = int(df["missing_active_count"].iloc[0])

        if total_count < minimum_resource_count:
            return True  # Too few records to evaluate meaningfully

        active_fraction = active_count / total_count
        not_active_fraction = (inactive_count + missing_active_count) / total_count

        # Check 1: minimum active percentage for this resource type
        if active_fraction < minimum_active_fraction:
            return (
                f"{resource_type_name}: active fraction {active_fraction:.2%} "
                f"below minimum {minimum_active_fraction:.0%} "
                f"(active={active_count}, inactive={inactive_count}, "
                f"missing_status={missing_active_count}, total={total_count})"
            )

        # Check 2: absolute ceiling on not-active percentage
        if not_active_fraction > maximum_inactive_fraction:
            return (
                f"{resource_type_name}: not-active fraction {not_active_fraction:.2%} "
                f"exceeds maximum {maximum_inactive_fraction:.0%} "
                f"(inactive={inactive_count}, missing_status={missing_active_count}, "
                f"total={total_count})"
            )

        return True
