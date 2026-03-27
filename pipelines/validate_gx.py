#!/usr/bin/env python3
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

def main() -> int:
    # Load data
    df = pd.read_csv(CSV, parse_dates=["ts"])

    # Ensure expected columns exist
    expected_cols = ["ts", "open", "high", "low", "close", "volume"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        print(f"[GX] Missing expected columns: {missing}", file=sys.stderr)
        return 2

    # Coerce numerics (prevents false failures when CSV has strings)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ephemeral context (no disk writes, no project directory needed)
    ctx = gx.get_context(mode="ephemeral")

    # Build a Batch from a Pandas DataFrame (Fluent)
    ds = ctx.data_sources.add_pandas(name="bars_reader")
    asset = ds.add_dataframe_asset(name="bars_df")
    bd = asset.add_batch_definition_whole_dataframe("whole_df")
    batch = bd.get_batch(batch_parameters={"dataframe": df})

    # Build ExpectationSuite and add expectations (typed Expectation objects)
    suite = gx.ExpectationSuite(name="bars_suite")

    # 1) Row count
    suite.add_expectation(
        expectation=ExpectTableRowCountToBeBetween(min_value=100, max_value=100_000)
    )

    # 2) Not-nulls
    for col in expected_cols:
        suite.add_expectation(expectation=ExpectColumnValuesToNotBeNull(column=col))

    # 3) Reasonable numeric ranges
    for col in ["open", "high", "low", "close"]:
        suite.add_expectation(
            expectation=ExpectColumnValuesToBeBetween(
                column=col, min_value=0, max_value=10_000_000
            )
        )
    suite.add_expectation(
        expectation=ExpectColumnValuesToBeBetween(column="volume", min_value=0)
    )

    # 4) OHLC relationships
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

    # Minimal readable summary and failures drill-down
    print(result)
    if not result.success:
        # Show which expectations failed for quick debugging
        for r in result.validation_results:
            if not r.success:
                etype = r.expectation_config.expectation_type
                unexpected = r.result.get("unexpected_count", 0)
                print(f"[GX] FAILED: {etype} (unexpected_count={unexpected})", file=sys.stderr)
        print("GX validation failed; aborting commit.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
