import sys
import pandas as pd
import great_expectations as ge  # legacy alias
from great_expectations.core.batch import BatchRequest

CSV = "data/bars/latest.csv"
df = pd.read_csv(CSV, parse_dates=["ts"])

for c in ["open", "high", "low", "close", "volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

context = ge.get_context()

# A quick runtime pandas datasource (names may vary per your config.yml)
datasource_name = "pandas_runtime"
if datasource_name not in [ds["name"] for ds in context.list_datasources()]:
    context.add_datasource(
        name=datasource_name,
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["id"],
            }
        },
    )

suite_name = "bars_suite"
if suite_name not in [s["name"] for s in context.list_expectation_suites()]:
    context.create_expectation_suite(suite_name)

validator = context.get_validator(
    batch_request=BatchRequest(
        datasource_name=datasource_name,
        data_connector_name="runtime_connector",
        data_asset_name="bars_df",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"id": "run-1"},
    ),
    expectation_suite_name=suite_name,
)

# Add expectations via validator.* methods
validator.expect_table_row_count_to_be_between(min_value=100, max_value=100_000)

for col in ["ts", "open", "high", "low", "close", "volume"]:
    validator.expect_column_values_to_not_be_null(column=col)

for col in ["open", "high", "low", "close"]:
    validator.expect_column_values_to_be_between(column=col, min_value=0, max_value=10_000_000)

validator.expect_column_values_to_be_between(column="volume", min_value=0)

validator.expect_column_pair_values_a_to_be_greater_than_b("high", "low", or_equal=True)
for col in ["open", "close"]:
    validator.expect_column_pair_values_a_to_be_greater_than_b(col, "low", or_equal=True)
    validator.expect_column_pair_values_a_to_be_greater_than_b("high", col, or_equal=True)

# Validate
res = validator.validate()
print(res)

if not res["success"]:
    sys.exit("GX validation failed; aborting commit.")
