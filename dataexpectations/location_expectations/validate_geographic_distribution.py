"""
Geographic Distribution Reasonableness

Analyzes the geographic distribution of locations across US states, comparing
observed distribution to expected patterns based on population density.

Detects anomalies such as:
- Excessive clustering in a single state (e.g., 80% in one small state)
- Complete absence from major population centers (CA, TX, FL, NY, PA)
- Too few states represented for a national directory
- Statistically suspicious concentration (measured via Herfindahl index)

A national healthcare directory should roughly mirror population distribution.
Regional variation is expected and allowed, but extreme deviations indicate
data bias, incomplete source coverage, or potentially fraudulent clustering.

TODO: This is another test that cannot run on anything but the full dataset
"""
from src.utils.inlaw import InLaw


class ValidateGeographicDistribution(InLaw):
    """Validate that location geographic distribution across states is reasonable."""

    title = "Geographic distribution of locations across states should be reasonable"

    # Top 5 US states by population (2024 Census estimates).
    # Used to verify major population centers are represented.
    TOP_FIVE_POPULATION_STATES = ["CA", "TX", "FL", "NY", "PA"]

    # All 50 US states plus DC as valid two-letter abbreviations.
    # Filters out territories, military codes, and invalid entries.
    VALID_US_STATES = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
        "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
        "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
        "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
        "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
        "WY",
    }

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate geographic distribution of locations across US states.

        Checks four distribution criteria:
          1. No single state exceeds a maximum percentage threshold
          2. A minimum number of distinct states are represented
          3. Top 5 population states meet a minimum combined percentage
          4. Herfindahl-Hirschman Index stays below a concentration ceiling

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (optional, uses sensible defaults)

        Returns:
            True if all checks pass, error message string if any fail
        """
        if config is None:
            return "SKIPPED: No config provided"

        # =============================================================
        # THRESHOLD DEFINITIONS
        # Generous defaults allowing regional variation while catching
        # truly anomalous distributions.
        # =============================================================

        # Max share of all locations allowed in any single state.
        # Most populous state (CA) has ~11.7% of US population.
        # 25% cap gives double that, catching absurd clustering.
        the_maximum_percent_in_single_state = 0.25

        # Min distinct US states (of 51 incl DC) with >= 1 location.
        # A national directory should cover virtually every state.
        # 40 allows legitimately sparse specialty directories.
        the_minimum_number_of_states_represented = 40

        # Min combined share for top 5 states (CA,TX,FL,NY,PA).
        # These hold ~37% of US population; 20% floor is generous.
        the_minimum_percent_in_top_five_population_states = 0.20

        # Max Herfindahl-Hirschman Index for state concentration.
        # Uniform across 50 states ~ 0.02; single-state monopoly = 1.0.
        # 0.10 catches e.g. one state at ~30% with spread remainder.
        the_maximum_herfindahl_index = 0.10

        # =============================================================
        # STEP 1: Query state distribution from Location resources
        # =============================================================
        state_count_sql = """
            SELECT
                UPPER(TRIM(json_extract_string(resource, '$.address.state'))) AS state_code,
                COUNT(*) AS location_count
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.address.state') IS NOT NULL
              AND TRIM(json_extract_string(resource, '$.address.state')) != ''
            GROUP BY UPPER(TRIM(json_extract_string(resource, '$.address.state')))
            ORDER BY location_count DESC
        """
        gx_df = InLaw.to_gx_dataframe(state_count_sql, engine)
        df = gx_df.active_batch.data.dataframe

        if len(df) == 0:
            return (
                "validate_geographic_distribution Error: "
                "No location resources with address.state found"
            )

        total_locations_with_state = df['location_count'].sum()
        if total_locations_with_state == 0:
            return (
                "validate_geographic_distribution Error: "
                "Total location count with state is zero"
            )

        df = df.copy()
        df['share'] = df['location_count'] / total_locations_with_state

        # Run all four distribution checks
        failures = []

        check_result = ValidateGeographicDistribution._check_single_state_max(
            df=df, threshold=the_maximum_percent_in_single_state,
        )
        if check_result is not None:
            failures.append(check_result)

        check_result = ValidateGeographicDistribution._check_minimum_states(
            df=df, threshold=the_minimum_number_of_states_represented,
        )
        if check_result is not None:
            failures.append(check_result)

        check_result = ValidateGeographicDistribution._check_top_five_states(
            df=df, threshold=the_minimum_percent_in_top_five_population_states,
        )
        if check_result is not None:
            failures.append(check_result)

        check_result = ValidateGeographicDistribution._check_herfindahl_index(
            df=df, threshold=the_maximum_herfindahl_index,
        )
        if check_result is not None:
            failures.append(check_result)

        if failures:
            return "; ".join(failures)

        return True

    @staticmethod
    def _check_single_state_max(*, df, threshold):
        """
        Check that no single state exceeds the maximum allowed share.

        Args:
            df: pandas DataFrame with 'state_code' and 'share' columns
            threshold: maximum allowed fraction for any single state

        Returns:
            None if check passes, error message string if it fails
        """
        highest_share = df['share'].max()
        highest_state = df.loc[df['share'].idxmax(), 'state_code']

        if highest_share > threshold:
            return (
                f"State '{highest_state}' has {highest_share:.1%} of all "
                f"locations, exceeding maximum {threshold:.0%}. "
                f"This suggests excessive geographic clustering."
            )
        return None

    @staticmethod
    def _check_minimum_states(*, df, threshold):
        """
        Check that at least the minimum number of valid US states are
        represented with at least one location each.

        Args:
            df: pandas DataFrame with 'state_code' column
            threshold: minimum number of distinct states required

        Returns:
            None if check passes, error message string if it fails
        """
        valid_states_in_data = set(
            df[df['state_code'].isin(
                ValidateGeographicDistribution.VALID_US_STATES
            )]['state_code']
        )
        count_represented = len(valid_states_in_data)

        if count_represented < threshold:
            missing = (
                ValidateGeographicDistribution.VALID_US_STATES
                - valid_states_in_data
            )
            missing_sample = sorted(missing)[:10]
            missing_display = ", ".join(missing_sample)
            if len(missing) > 10:
                missing_display += f" ... and {len(missing) - 10} more"

            return (
                f"Only {count_represented} US states represented, "
                f"below minimum of {threshold}. "
                f"Missing: {missing_display}"
            )
        return None

    @staticmethod
    def _check_top_five_states(*, df, threshold):
        """
        Check that the top 5 population states (CA, TX, FL, NY, PA)
        collectively meet a minimum combined share.

        Args:
            df: pandas DataFrame with 'state_code' and 'share' columns
            threshold: minimum combined fraction for top 5 states

        Returns:
            None if check passes, error message string if it fails
        """
        top_five = ValidateGeographicDistribution.TOP_FIVE_POPULATION_STATES
        combined_share = df[
            df['state_code'].isin(top_five)
        ]['share'].sum()

        if combined_share < threshold:
            return (
                f"Top 5 population states ({', '.join(top_five)}) "
                f"have only {combined_share:.1%} of locations, "
                f"below minimum {threshold:.0%}. "
                f"Major population centers appear underrepresented."
            )
        return None

    @staticmethod
    def _check_herfindahl_index(*, df, threshold):
        """
        Check that the Herfindahl-Hirschman Index (sum of squared shares)
        stays below a concentration ceiling.

        HHI measures market concentration. Uniform distribution across
        50 states yields ~0.02; a single-state monopoly yields 1.0.
        This catches distributions where multiple states are each
        moderately overrepresented but no single one triggers the
        single-state check.

        Args:
            df: pandas DataFrame with 'state_code' and 'share' columns
            threshold: maximum allowed HHI value

        Returns:
            None if check passes, error message string if it fails
        """
        hhi = (df['share'] ** 2).sum()

        if hhi > threshold:
            top_rows = df.head(5)[['state_code', 'share']].to_dict('records')
            diagnostic = ", ".join(
                f"{r['state_code']}={r['share']:.1%}" for r in top_rows
            )
            return (
                f"Herfindahl-Hirschman Index is {hhi:.4f}, "
                f"exceeding maximum {threshold:.4f}. "
                f"Excessive geographic concentration. "
                f"Top states: {diagnostic}"
            )
        return None
