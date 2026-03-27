import sys
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe  # <— expectation classes

CSV = "data/bars/latest.csv"
df = pd.read_csv(CSV, parse_dates=["ts"])

# 1) Ephemeral context (in‑memory, nothing persisted to disk)
ctx = gx.get_context(mode="ephemeral")  # ok for stateless validation

# 2) Wire up a pandas Data Source → DataFrame Asset → Batch (whole DataFrame)
ds = ctx.data_sources.add_pandas(name="bars_reader")  # GX 1.x fluent API
asset = ds.add_dataframe_asset(name="bars_df")
bd = asset.add_batch_definition_whole_dataframe("whole_df")
batch = bd.get_batch(batch_parameters={"dataframe": df})  # one Batch of data

# 3) Build an Expectation Suite (object) and add Expectations (objects)
suite = gx.ExpectationSuite(name="bars_suite")

# Overall row count
suite.add_expectation(
    gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=100_000)
)

# Not-null constraints
for col in ["ts", "open", "high", "low", "close", "volume"]:
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))

# Price bounds (0 .. 10M)
for col in ["open", "high", "low", "close"]:
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column=col, min_value=0, max_value=10_000_000
        )
    )

# Volume nonnegative
suite.add_expectation(
    gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0)
)

# (Optional) register the suite on the context (keeps API consistent)
ctx.suites.add(suite)

# 4) Validate the whole suite against your Batch
result = batch.validate(suite)   # pass the suite POSITIONALLY
print(result)

# 5) Gate your run on the boolean result
if not result.success:           # it's an object, not a dict
    sys.exit("GX validation failed; aborting commit.")
