import sys
import pandas as pd
import great_expectations as gx
from great_expectations.expectations.core import (
    ExpectTableRowCountToBeBetween,
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnPairValuesAToBeGreaterThanB,
)

CSV = "data/bars/latest.csv"
df = pd.read_csv(CSV, parse_dates=["ts"])

# Ensure numerics are truly numeric (prevents false failures):
for c in ["open", "high", "low", "close", "volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# 1) Ephemeral context (in‑memory)
ctx = gx.get_context(mode="ephemeral")

# 2) Build a Batch from a Pandas DataFrame
ds = ctx.data_sources.add_pandas(name="bars_reader")
asset = ds.add_dataframe_asset(name="bars_df")
bd = asset.add_batch_definition_whole_dataframe("whole_df")
batch = bd.get_batch(batch_parameters={"dataframe": df})

# 3) Build an ExpectationSuite and add expectations
suite = gx.ExpectationSuite(name="bars_suite")

# Row count — your synthetic generator writes ~300 rows
suite.add_expectation(
    expectation=ExpectTableRowCountToBeBetween(min_value=100, max_value=100_000)
)

# Not‑nulls
for col in ["ts", "open", "high", "low", "close", "volume"]:
    suite.add_expectation(expectation=ExpectColumnValuesToNotBeNull(column=col))

# Reasonable numeric ranges
for col in ["open", "high", "low", "close"]:
    suite.add_expectation(
        expectation=ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=10_000_000)
    )
suite.add_expectation(
    expectation=ExpectColumnValuesToBeBetween(column="volume", min_value=0)
)

# OHLC relationships
# high >= low
suite.add_expectation(
    expectation=ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="high", column_B="low", or_equal=True
    )
)
# open, close must lie within [low, high]
for col in ["open", "close"]:
    suite.add_expectation(
        expectation=ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=col, column_B="low", or_equal=True
        )
    )
    suite.add_expectation(
        expectation=ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B=col, or_equal=True
        )
    )

# Register suite and validate
ctx.suites.add(suite)

result = batch.validate(expectation_suite=suite)

# Pretty-print minimal summary; you can also do result.to_json_dict()
print(result)

if not result.success:
    sys.exit("GX validation failed; aborting commit.")
