# pipelines/validate_gx.py
import sys
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

CSV = "data/bars/latest.csv"
df = pd.read_csv(CSV, parse_dates=["ts"])

# 1) Ephemeral context (in‑memory)
ctx = gx.get_context(mode="ephemeral")

# 2) Build a Batch from a Pandas DataFrame
ds = ctx.data_sources.add_pandas(name="bars_reader")
asset = ds.add_dataframe_asset(name="bars_df")
bd = asset.add_batch_definition_whole_dataframe("whole_df")
batch = bd.get_batch(batch_parameters={"dataframe": df})

# 3) Build an ExpectationSuite and add expectations
suite = gx.ExpectationSuite(name="bars_suite")

suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=100_000)
)

for col in ["ts", "open", "high", "low", "close", "volume"]:
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

for col in ["open", "high", "low", "close"]:
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=10_000_000)
    )

suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0)
)

# Optional OHLC sanity (high >= low)
suite.add_expectation(
    gxe.ExpectColumnPairValuesAToBeGreaterThanOrEqualToB(column_A="high", column_B="low")
)

ctx.suites.add(suite)

# 4) Validate
result = batch.validate(suite)
print(result)
if not result.success:
    sys.exit("❌ GX validation failed; aborting commit.")
