"""
Hours of Operation Coverage

Measures the percentage of Location and Organization resources that have hours
of operation specified. Operating hours are critical for care coordination
because they enable appropriate scheduling, help patients avoid unnecessary
trips to closed facilities, and support automated appointment booking systems.

For Locations, FHIR R4 provides the native 'hoursOfOperation' array field
containing daysOfWeek, allDay, openingTime, and closingTime entries.

For Organizations, standard FHIR R4 does NOT include hoursOfOperation.
However, some NDH profiles add availability through extensions. We check for
the NDH availabletime extension, but expect very low coverage.

This test measures three facets:
1. Locations with ANY hours data (hoursOfOperation array present and non-empty)
2. Locations with COMPLETE weekly hours (all 7 days or an allDay entry)
3. Organizations with any hours-related extension data

Hours data is notoriously incomplete in most directories, so thresholds
are set conservatively low.
"""
from src.utils.inlaw import InLaw


class ValidateHoursOfOperationCoverage(InLaw):
    """Validate that a meaningful percentage of locations and organizations
    include hours of operation data for care coordination and scheduling."""

    title = ("Hours of operation coverage should meet minimum "
             "thresholds for locations and organizations")

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate hours of operation coverage across Location and
        Organization FHIR resources.

        Runs three checks:
        - Percentage of Locations with any hoursOfOperation data
        - Percentage of Locations with complete weekly hours
        - Percentage of Organizations with any hours/availability ext

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (accepted for InLaw
                    pattern compatibility)

        Returns:
            True if all checks pass, error message string if any fail
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold variables ─────────────────────────────────────
        # Hours data is notoriously incomplete in most healthcare
        # directories. These thresholds are intentionally modest.

        # Minimum % of Locations with at least one hoursOfOperation
        # entry (any day, any time). Even partial hours data is
        # better than none for scheduling purposes.
        the_minimum_percent_locations_with_any_hours = 0.30

        # Minimum % of Locations with "complete" weekly hours.
        # Complete = allDay=true OR daysOfWeek covering all 7 days.
        the_minimum_percent_locations_with_complete_weekly_hours = 0.15

        # Minimum % of Organizations with any hours/availability.
        # FHIR R4 Organization lacks native hoursOfOperation, so
        # this checks NDH extensions. Very low because most
        # directories put hours only on Locations.
        the_minimum_percent_orgs_with_any_hours = 0.20

        failures = ValidateHoursOfOperationCoverage._run_all_checks(
            engine=engine,
            min_pct_any=the_minimum_percent_locations_with_any_hours,
            min_pct_complete=the_minimum_percent_locations_with_complete_weekly_hours,
            min_pct_orgs=the_minimum_percent_orgs_with_any_hours,
        )

        if not failures:
            return True
        return "; ".join(failures)

    @staticmethod
    def _run_all_checks(*, engine, min_pct_any, min_pct_complete, min_pct_orgs):
        """Execute all three coverage checks; return list of failure messages."""
        failures = []

        # ── Check 1: Locations with any hours ───────────────────────
        loc = ValidateHoursOfOperationCoverage._get_location_hours_totals(
            engine=engine
        )
        if isinstance(loc, str):
            failures.append(loc)
        elif loc["total_locations"] == 0:
            failures.append(
                "validate_hours_coverage.py Error: "
                "No Location resources found in fhir_resources table"
            )
        else:
            pct = loc["locations_with_any_hours"] / loc["total_locations"]
            if pct < min_pct_any:
                failures.append(
                    f"Only {pct:.1%} of Locations have any hours of "
                    f"operation (minimum: {min_pct_any:.0%}). "
                    f"{loc['locations_with_any_hours']} of "
                    f"{loc['total_locations']} have hours data."
                )

        # ── Check 2: Locations with complete weekly hours ───────────
        comp = ValidateHoursOfOperationCoverage._get_complete_weekly(
            engine=engine
        )
        if isinstance(comp, str):
            failures.append(comp)
        elif comp["total_locations"] > 0:
            pct = comp["complete_count"] / comp["total_locations"]
            if pct < min_pct_complete:
                failures.append(
                    f"Only {pct:.1%} of Locations have complete weekly "
                    f"hours (minimum: {min_pct_complete:.0%}). "
                    f"{comp['complete_count']} of "
                    f"{comp['total_locations']} have full weekly "
                    f"coverage or allDay flag."
                )

        # ── Check 3: Organizations with any hours/availability ──────
        org = ValidateHoursOfOperationCoverage._get_org_hours_totals(
            engine=engine
        )
        if isinstance(org, str):
            failures.append(org)
        elif org["total_orgs"] > 0:
            pct = org["orgs_with_any_hours"] / org["total_orgs"]
            if pct < min_pct_orgs:
                failures.append(
                    f"Only {pct:.1%} of Organizations have any "
                    f"hours/availability data "
                    f"(minimum: {min_pct_orgs:.0%}). "
                    f"{org['orgs_with_any_hours']} of "
                    f"{org['total_orgs']} Organizations. "
                    f"Note: FHIR R4 Organization lacks native "
                    f"hoursOfOperation; this checks NDH "
                    f"availability extensions and raw JSON."
                )

        return failures

    @staticmethod
    def _get_location_hours_totals(*, engine):
        """Query total Locations and Locations with any hoursOfOperation.

        Returns:
            dict with total_locations and locations_with_any_hours,
            or error message string on failure.
        """
        sql_total = """
            SELECT COUNT(*) AS total_locations
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Location'
        """
        sql_with_hours = """
            SELECT COUNT(*) AS locations_with_any_hours
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Location'
              AND json_extract(resource, '$.hoursOfOperation') IS NOT NULL
              AND json_array_length(
                  json_extract(resource, '$.hoursOfOperation')
              ) > 0
        """
        try:
            gx_t = InLaw.to_gx_dataframe(sql_total, engine)
            total = int(gx_t.active_batch.data.dataframe['total_locations'].iloc[0])
            gx_h = InLaw.to_gx_dataframe(sql_with_hours, engine)
            with_hours = int(gx_h.active_batch.data.dataframe['locations_with_any_hours'].iloc[0])
            return {"total_locations": total, "locations_with_any_hours": with_hours}
        except Exception as exc:
            return (
                "validate_hours_coverage.py Error: "
                f"Failed to query location hours totals: {exc}"
            )

    @staticmethod
    def _get_complete_weekly(*, engine):
        """Query Locations with complete weekly hours coverage.

        A Location is complete if it has an allDay=true entry OR its
        daysOfWeek entries collectively cover all 7 FHIR day codes.

        Returns:
            dict with total_locations and complete_count,
            or error message string on failure.
        """
        sql_total = """
            SELECT COUNT(*) AS total_locations
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Location'
        """
        sql_allday = """
            SELECT COUNT(DISTINCT json_extract_string(resource, '$.id'))
                AS allday_count
            FROM fhir_resources,
                 unnest(json_extract(resource, '$.hoursOfOperation')) AS he
            WHERE json_extract_string(resource, '$.resourceType') = 'Location'
              AND json_extract(resource, '$.hoursOfOperation') IS NOT NULL
              AND json_array_length(
                  json_extract(resource, '$.hoursOfOperation')
              ) > 0
              AND json_extract_string(he, '$.allDay') = 'true'
        """
        sql_full_week = """
            SELECT COUNT(*) AS full_week_count
            FROM (
                SELECT json_extract_string(resource, '$.id') AS loc_id
                FROM fhir_resources,
                     unnest(json_extract(resource, '$.hoursOfOperation')) AS he,
                     unnest(json_extract(he, '$.daysOfWeek')) AS dow
                WHERE json_extract_string(resource, '$.resourceType') = 'Location'
                  AND json_extract(resource, '$.hoursOfOperation') IS NOT NULL
                  AND json_array_length(
                      json_extract(resource, '$.hoursOfOperation')
                  ) > 0
                  AND json_extract(he, '$.daysOfWeek') IS NOT NULL
                GROUP BY json_extract_string(resource, '$.id')
                HAVING COUNT(DISTINCT json_extract_string(dow, '$')) >= 7
            ) AS full_week_locs
        """
        try:
            total = int(InLaw.to_gx_dataframe(sql_total, engine)
                        .active_batch.data.dataframe['total_locations'].iloc[0])
            allday = int(InLaw.to_gx_dataframe(sql_allday, engine)
                         .active_batch.data.dataframe['allday_count'].iloc[0])
            full_wk = int(InLaw.to_gx_dataframe(sql_full_week, engine)
                          .active_batch.data.dataframe['full_week_count'].iloc[0])
            # Cap at total; sum is upper bound since sets may overlap
            return {"total_locations": total, "complete_count": min(allday + full_wk, total)}
        except Exception as exc:
            return (
                "validate_hours_coverage.py Error: "
                f"Failed to query complete weekly hours: {exc}"
            )

    @staticmethod
    def _get_org_hours_totals(*, engine):
        """Query Organizations with any hours/availability data.

        Standard FHIR R4 Organization has no hoursOfOperation field.
        We check for: (a) hoursOfOperation in raw JSON anyway, and
        (b) NDH availabletime extensions.

        Returns:
            dict with total_orgs and orgs_with_any_hours,
            or error message string on failure.
        """
        sql_total = """
            SELECT COUNT(*) AS total_orgs
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
        """
        sql_with_hours = """
            SELECT COUNT(*) AS orgs_with_hours
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract(resource, '$.hoursOfOperation') IS NOT NULL
              AND json_array_length(
                  json_extract(resource, '$.hoursOfOperation')
              ) > 0
        """
        sql_with_ext = """
            SELECT COUNT(DISTINCT json_extract_string(resource, '$.id'))
                AS orgs_with_avail_ext
            FROM fhir_resources,
                 unnest(json_extract(resource, '$.extension')) AS ext
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract(resource, '$.extension') IS NOT NULL
              AND json_array_length(
                  json_extract(resource, '$.extension')
              ) > 0
              AND LOWER(json_extract_string(ext, '$.url'))
                  LIKE '%availabletime%'
        """
        try:
            total = int(
                InLaw.to_gx_dataframe(sql_total, engine)
                .active_batch.data.dataframe['total_orgs'].iloc[0]
            )
            with_hrs = int(
                InLaw.to_gx_dataframe(sql_with_hours, engine)
                .active_batch.data.dataframe['orgs_with_hours'].iloc[0]
            )
            with_ext = int(
                InLaw.to_gx_dataframe(sql_with_ext, engine)
                .active_batch.data.dataframe['orgs_with_avail_ext'].iloc[0]
            )
            # Cap at total; sum is upper bound since sets may overlap
            return {
                "total_orgs": total,
                "orgs_with_any_hours": min(with_hrs + with_ext, total),
            }
        except Exception as exc:
            return (
                "validate_hours_coverage.py Error: "
                f"Failed to query org hours totals: {exc}"
            )

