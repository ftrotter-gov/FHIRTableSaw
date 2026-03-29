"""
Data Freshness Indicators

Examines temporal metadata fields (meta.lastUpdated timestamps) on Organization
resources to assess whether directory data is being actively maintained or has
become stale. Healthcare information changes frequently - practitioners move,
organizations close or reorganize, licenses expire - so data freshness is a
critical trust indicator.

A healthy directory shows continuous update activity. A deteriorating directory
shows an aging data population with update activity concentrated only in recent
entries. This test also helps detect broken update pipelines where new data
stops flowing even though the directory infrastructure remains operational.

Checks performed:
    1. The most recently updated Organization resource should not be older than
       a configurable threshold (default 365 days), ensuring the pipeline is
       still actively flowing data.
    2. The percentage of Organization resources older than one year should not
       exceed a configurable maximum (default 50%).
    3. A configurable minimum percentage of Organization resources should have
       been updated within the last six months (default 30%).
    4. The median age of Organization resources should not exceed a configurable
       maximum (default 548 days / ~18 months).
"""
from src.utils.inlaw import InLaw


class ValidateDataFreshness(InLaw):
    """Validate that Organization resource timestamps indicate active directory maintenance."""

    title = "Organization data freshness should indicate active directory maintenance"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate data freshness of Organization resources by examining
        meta.lastUpdated timestamps for recency and age distribution.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (thresholds can be overridden
                    via config keys documented below; sensible defaults are
                    provided for each)

        Returns:
            True if all freshness checks pass, error message string if any fail
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold variables ─────────────────────────────────────
        # Each is named to read as a sentence so the intent is obvious.

        # Maximum days allowed since the single most-recently updated
        # Organization resource.  If the newest record is older than
        # this, the update pipeline is likely broken or stalled.
        the_maximum_days_since_most_recent_update = config.get(
            'maximum_days_since_most_recent_update', 365
        )

        # Maximum fraction (0-1) of Organization resources whose
        # meta.lastUpdated is more than one year old.
        the_maximum_percent_resources_older_than_one_year = config.get(
            'maximum_percent_resources_older_than_one_year', 0.50
        )

        # Minimum fraction (0-1) of Organization resources updated
        # within the last six months.
        the_minimum_percent_resources_updated_within_six_months = config.get(
            'minimum_percent_resources_updated_within_six_months', 0.30
        )

        # Maximum acceptable median age (in days) across all
        # Organization resources with a meta.lastUpdated value.
        the_maximum_median_age_in_days = config.get(
            'maximum_median_age_in_days', 548  # roughly 18 months
        )

        failures = []

        # Delegate to private helpers for each check
        recency_failure = ValidateDataFreshness._check_most_recent_update(
            engine=engine,
            threshold_days=the_maximum_days_since_most_recent_update,
        )
        if isinstance(recency_failure, str) and recency_failure.startswith("BAIL:"):
            # No usable timestamps at all – return early
            return recency_failure.replace("BAIL:", "")
        if recency_failure is not None:
            failures.append(recency_failure)

        distribution_failures = ValidateDataFreshness._check_age_distribution(
            engine=engine,
            max_pct_older_than_one_year=the_maximum_percent_resources_older_than_one_year,
            min_pct_within_six_months=the_minimum_percent_resources_updated_within_six_months,
        )
        if isinstance(distribution_failures, str):
            return distribution_failures  # early bail
        failures.extend(distribution_failures)

        median_failure = ValidateDataFreshness._check_median_age(
            engine=engine,
            max_median_days=the_maximum_median_age_in_days,
        )
        if median_failure is not None:
            failures.append(median_failure)

        if not failures:
            return True
        return "; ".join(failures)

    # ── Private helpers (underscore-prefixed, @staticmethod) ────────

    @staticmethod
    def _check_most_recent_update(*, engine, threshold_days):
        """
        Check that the most recently updated Organization resource is
        not older than threshold_days.

        Returns:
            None if the check passes.
            A "BAIL:..." string if there are no timestamps at all.
            A plain failure message string if the check fails.
        """
        most_recent_sql = """
            SELECT
                MAX(TRY_CAST(
                    json_extract_string(resource, '$.meta.lastUpdated')
                    AS TIMESTAMP
                )) AS most_recent_update
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract_string(resource, '$.meta.lastUpdated') IS NOT NULL
        """
        gx_df = InLaw.to_gx_dataframe(most_recent_sql, engine)
        pandas_df = gx_df.active_batch.data.dataframe

        most_recent_value = pandas_df['most_recent_update'].iloc[0]
        if most_recent_value is None:
            return (
                "BAIL:validate_data_freshness.py: No Organization resources "
                "have a parseable meta.lastUpdated timestamp; cannot assess "
                "data freshness"
            )

        # Compute days-since inside DuckDB for timezone consistency
        days_since_sql = f"""
            SELECT DATE_DIFF(
                'day',
                TRY_CAST('{most_recent_value}' AS TIMESTAMP),
                CURRENT_TIMESTAMP
            ) AS days_since_most_recent
        """
        days_gx_df = InLaw.to_gx_dataframe(days_since_sql, engine)
        days_pandas_df = days_gx_df.active_batch.data.dataframe
        days_since = int(days_pandas_df['days_since_most_recent'].iloc[0])

        if days_since > threshold_days:
            return (
                f"Most recent Organization update was {days_since} days ago, "
                f"exceeds maximum of {threshold_days} days"
            )
        return None

    @staticmethod
    def _check_age_distribution(*, engine, max_pct_older_than_one_year, min_pct_within_six_months):
        """
        Check the age-bucket distribution of Organization resources.

        Returns:
            A list of failure message strings (empty if all pass).
            A plain string (not a list) if there are no usable timestamps
            (early-bail scenario).
        """
        age_distribution_sql = """
            SELECT
                COUNT(*) AS total_with_timestamp,
                COUNT(*) FILTER (
                    WHERE TRY_CAST(
                        json_extract_string(resource, '$.meta.lastUpdated')
                        AS TIMESTAMP
                    ) >= CURRENT_TIMESTAMP - INTERVAL '6 months'
                ) AS updated_within_six_months,
                COUNT(*) FILTER (
                    WHERE TRY_CAST(
                        json_extract_string(resource, '$.meta.lastUpdated')
                        AS TIMESTAMP
                    ) < CURRENT_TIMESTAMP - INTERVAL '1 year'
                ) AS older_than_one_year
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract_string(resource, '$.meta.lastUpdated') IS NOT NULL
              AND TRY_CAST(
                  json_extract_string(resource, '$.meta.lastUpdated')
                  AS TIMESTAMP
              ) IS NOT NULL
        """
        gx_df = InLaw.to_gx_dataframe(age_distribution_sql, engine)
        pandas_df = gx_df.active_batch.data.dataframe

        total_with_timestamp = int(pandas_df['total_with_timestamp'].iloc[0])
        if total_with_timestamp == 0:
            return (
                "validate_data_freshness.py: No Organization resources have "
                "a parseable meta.lastUpdated timestamp"
            )

        updated_within_six_months = int(pandas_df['updated_within_six_months'].iloc[0])
        older_than_one_year = int(pandas_df['older_than_one_year'].iloc[0])

        pct_older = older_than_one_year / total_with_timestamp
        pct_recent = updated_within_six_months / total_with_timestamp

        check_failures = []

        if pct_older > max_pct_older_than_one_year:
            check_failures.append(
                f"{pct_older:.1%} of Organization resources are older than "
                f"one year ({older_than_one_year}/{total_with_timestamp}), "
                f"exceeds maximum of {max_pct_older_than_one_year:.0%}"
            )

        if pct_recent < min_pct_within_six_months:
            check_failures.append(
                f"Only {pct_recent:.1%} of Organization resources were "
                f"updated within the last six months "
                f"({updated_within_six_months}/{total_with_timestamp}), "
                f"below minimum of {min_pct_within_six_months:.0%}"
            )

        return check_failures

    @staticmethod
    def _check_median_age(*, engine, max_median_days):
        """
        Check that the median age of Organization resources does not
        exceed max_median_days.

        Returns:
            None if the check passes.
            A failure message string if the check fails.
        """
        median_age_sql = """
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY DATE_DIFF(
                        'day',
                        TRY_CAST(
                            json_extract_string(resource, '$.meta.lastUpdated')
                            AS TIMESTAMP
                        ),
                        CURRENT_TIMESTAMP
                    )
                ) AS median_age_days
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_extract_string(resource, '$.meta.lastUpdated') IS NOT NULL
              AND TRY_CAST(
                  json_extract_string(resource, '$.meta.lastUpdated')
                  AS TIMESTAMP
              ) IS NOT NULL
        """
        gx_df = InLaw.to_gx_dataframe(median_age_sql, engine)
        pandas_df = gx_df.active_batch.data.dataframe
        median_age_days = float(pandas_df['median_age_days'].iloc[0])

        if median_age_days > max_median_days:
            return (
                f"Median Organization resource age is {median_age_days:.0f} "
                f"days, exceeds maximum of {max_median_days} days"
            )
        return None
