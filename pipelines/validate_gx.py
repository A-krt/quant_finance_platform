#!/usr/bin/env python3
import sys
import os
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe  # Expectation classes

CSV = "data/bars/latest.csv"

def opt_into_node24_for_github_actions() -> None:
    """
    Opt into Node.js 24 for JavaScript actions in GitHub Actions by writing
    FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true to the special $GITHUB_ENV file.
    This will affect *subsequent steps* in the same job.
    """
    try:
        if os.getenv("GITHUB_ACTIONS") == "true":
            env_file = os.getenv("GITHUB_ENV")
            if env_file:
                with open(env_file, "a", encoding="utf-8") as f:
                    f.write("FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true\n")
                print("[CI] Opted into Node 24: set FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true for subsequent steps.")
            else:
                print("[CI] GITHUB_ENV not set; cannot persist Node 24 opt-in from this step.", file=sys.stderr)
        else:
            # Not running in GitHub Actions; no-op.
            pass
    except Exception as e:
        print(f"[CI] Failed to write Node 24 opt-in to GITHUB_ENV: {e}", file=sys.stderr)

def main() -> int:
    # Proactively opt into Node 24 for later steps in the same job.
    opt_into_node24_for_github_actions()

    print(f"[GX] great_expectations version: {gx.__version__}")

    # Load data
    df = pd.read_csv(CSV, parse_dates=["ts"])

    # Ensure expected columns exist
    expected_cols = ["ts", "open", "high", "low", "close", "volume"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        print(f"[GX] Missing expected columns: {missing}", file=sys.stderr)
        return 2

    # Coerce numerics (avoids false negatives when numbers are strings)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 1) Ephemeral context (in‑memory, nothing persisted to disk)
    ctx = gx.get_context(mode="ephemeral")

    # 2) Pandas → DataFrame Asset → Batch (whole DataFrame)
    ds = ctx.data_sources.add_pandas(name="bars_reader")
    asset = ds.add_dataframe_asset(name="bars_df")
    bd = asset.add_batch_definition_whole_dataframe("whole_df")
    batch = bd.get_batch(batch_parameters={"dataframe": df})

    # 3) Expectation Suite and Expectations (typed expectation objects)
    suite = gx.ExpectationSuite(name="bars_suite")

    # Row count
    suite.add_expectation(
        gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=100_000)
    )

    # Not-null constraints
    for col in expected_cols:
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

    # OHLC relationships
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="low", or_equal=True
        )
    )
    for col in ["open", "close"]:
        suite.add_expectation(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A=col, column_B="low", or_equal=True
            )
        )
        suite.add_expectation(
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="high", column_B=col, or_equal=True
            )
        )

    # Register suite (keeps API consistent)
    ctx.suites.add(suite)

    # 4) Validate — pass suite POSITIONALLY (compatible with your GX build)
    result = batch.validate(suite)
    print(result)

    # Optional: quick drill-down if it fails
    if not result.success:
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
