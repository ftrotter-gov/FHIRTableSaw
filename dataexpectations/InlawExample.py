# Define the table once to be used by all tests - will be redefined in each method using settings
# endpoint_DBTable = DBTable(schema='nppes_raw', table='endpoint_file')


class ValidateNpiCount(InLaw):
    """
    Validates that the distinct NPI count is within 5% of the expected baseline.
    Expected: 476,736
    """

    title = "NPI count should be within 5% of expected value"

    @staticmethod
    def run(engine, settings: Dynaconf | None = None):
        if settings is None:
            print("Error: This test requires settings. It needs to know the DB Tables")
            exit()

        endpoint_DBTable = DBTable(schema=settings.NPPES_RAW_SCHEMA, table="endpoint_file")
        sql = f"SELECT COUNT(DISTINCT npi) as value FROM {endpoint_DBTable}"
        gx_df = InLaw.to_gx_dataframe(sql, engine)
        result = gx_df.expect_column_values_to_be_between(
            column="value",
            min_value=452899,  # 476736 * 0.95
            max_value=500573,  # 476736 * 1.05
        )
        if result.success:
            return True
        return f"NPI count validation failed: {result.result}"
