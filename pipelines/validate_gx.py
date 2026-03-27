import sys
import pandas as pd
import great_expectations as gx

CSV = "data/bars/latest.csv"
df = pd.read_csv(CSV, parse_dates=["ts"])

ctx = gx.get_context(mode="ephemeral")

ds = ctx.data_sources.add_pandas(name="bars_reader")
asset = ds.add_dataframe_asset(name="bars_df")
bd = asset.add_batch_definition_whole_dataframe("whole_df")
batch = bd.get_batch(batch_parameters={"dataframe": df})

suite = ctx.suites.add_or_update("bars_suite")

suite.add_expectation(
    "expect_table_row_count_to_be_between",
    {"min_value": 100, "max_value": 100_000},
)
for col in ["ts", "open", "high", "low", "close", "volume"]:
    suite.add_expectation("expect_column_values_to_not_be_null", {"column": col})

for col in ["open", "high", "low", "close"]:
    suite.add_expectation(
        "expect_column_values_to_be_between",
        {"column": col, "min_value": 0, "max_value": 10_000_000},
    )
suite.add_expectation(
    "expect_column_values_to_be_between",
    {"column": "volume", "min_value": 0},
)

result = batch.validate(expectation_suite=suite)
print(result)
if not result["success"]:
    sys.exit("GX validation failed; aborting commit.")
