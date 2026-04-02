"""
Test #15: Maximum Phone and Fax Count per Organization

Identifies maximum telecom contact count per organization.
Flags suspicious orgs with bloated contact metadata.

TODO: Exclude from partial analysis
"""
from src.utils.inlaw import InLaw


class ValidateMaxTelecomPerOrganization(InLaw):
    """Validate maximum phone/fax per organization is reasonable."""

    title = "Maximum phone/fax per organization should be within reasonable bounds"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate max telecom per org.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - max_telecom_per_org: Maximum acceptable telecom entries (default 20)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        sql = """
            SELECT
                json_extract_string(resource, '$.id') AS org_id,
                json_array_length(json_extract(resource, '$.telecom')) AS telecom_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_array_length(json_extract(resource, '$.telecom')) > 0
            ORDER BY telecom_count DESC
            LIMIT 1
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)

        max_allowed = config.get('max_telecom_per_org', 20)

        result = gx_df.expect_column_values_to_be_between(
            column="telecom_count",
            min_value=0,
            max_value=max_allowed
        )

        if result.success:
            return True

        df = gx_df.active_batch.data.dataframe
        if len(df) > 0:
            actual_max = int(df['telecom_count'].iloc[0])
            org_id = df['org_id'].iloc[0]
            return f"Organization {org_id} has {actual_max} telecom entries, exceeds max {max_allowed}"

        return True
