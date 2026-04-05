"""
Test #14: Maximum Addresses per Organization

Identifies maximum address count per organization.
Outlier detector for duplication explosions or bad normalization.

TODO: Exclude from partial analysis
"""
from src.utils.inlaw import InLaw


class ValidateMaxAddressesPerOrganization(InLaw):
    """Validate maximum addresses per organization is reasonable."""

    title = "Maximum addresses per organization should be within reasonable bounds"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate max addresses per org.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - max_addresses_per_org: Maximum acceptable addresses per org (default 50)

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        sql = """
            SELECT
                json_extract_string(resource, '$.id') AS org_id,
                json_array_length(json_extract(resource, '$.address')) AS address_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Organization'
              AND json_array_length(json_extract(resource, '$.address')) > 0
            ORDER BY address_count DESC
            LIMIT 1
        """
        gx_df = InLaw.to_gx_dataframe(sql, engine)

        max_allowed = config.get('max_addresses_per_org', 50)

        result = gx_df.expect_column_values_to_be_between(
            column="address_count",
            min_value=0,
            max_value=max_allowed
        )

        if result.success:
            return True

        df = gx_df.active_batch.data.dataframe
        if len(df) > 0:
            actual_max = int(df['address_count'].iloc[0])
            org_id = df['org_id'].iloc[0]
            return f"Organization {org_id} has {actual_max} addresses, exceeds max {max_allowed}"

        return True
