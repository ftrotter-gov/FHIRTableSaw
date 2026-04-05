"""
Test #10: Total Location Count

Validates total number of location resources.
Structural signal for organizational/service-site geography loading.

TODO: Make this key against the states in input.
"""
from src.utils.inlaw import InLaw


class ValidateTotalLocationCount(InLaw):
    """Validate total location count is within expected range."""

    title = "Total Location count should be within expected range"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate total location count.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary containing:
                - min_total_locations: Minimum expected location count
                - max_total_locations: Maximum expected location count

        Returns:
            True if test passes, error message string if test fails
        """
        if config is None:
            return "SKIPPED: No config provided"

        # Query DuckDB for location count
        sql = "SELECT COUNT(*) AS location_count FROM fhir_resources"
        gx_df = InLaw.to_gx_dataframe(sql, engine)

        # Get expected range from config
        min_locs = config.get('min_total_locations', 1)
        max_locs = config.get('max_total_locations', 10000000)

        # Validate using Great Expectations
        result = gx_df.expect_column_values_to_be_between(
            column="location_count",
            min_value=min_locs,
            max_value=max_locs
        )

        if result.success:
            return True

        # Get actual count for error message
        df = gx_df.active_batch.data.dataframe
        actual_count = int(df['location_count'].iloc[0])

        return f"Location count {actual_count} outside expected range [{min_locs}, {max_locs}]"
