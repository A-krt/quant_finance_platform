import csv, os, sys, time, json
from textwrap import dedent
import urllib.request

HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")

if not (HOST and TOKEN and WAREHOUSE_ID):
    sys.exit("Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID env vars")

API = f"{HOST.rstrip('/')}/api/2.0/sql/statements"

def post_stmt(sql: str) -> str:
    """POST one SQL statement, return statement_id or 'SUCCEEDED'."""
    payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID}
    req = urllib.request.Request(API, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Authorization": f"Bearer {TOKEN}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode())
    state = body.get("status", {}).get("state")
    if state == "SUCCEEDED":
        return "SUCCEEDED"
    stmt_id = body.get("statement_id")
    if not stmt_id:
        print("Submit failed; full body:", json.dumps(body, indent=2))
        sys.exit(1)
    return stmt_id

def poll_stmt(stmt_id: str, timeout_sec=900):
    """Poll until SUCCEEDED/FAILED/CANCELED or timeout."""
    url = f"{API}/{stmt_id}"
    start = time.time()
    while True:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode())
        state = body.get("status", {}).get("state")
        if state in {"SUCCEEDED", "FAILED", "CANCELED"}:
            if state == "SUCCEEDED":
                return
            print(json.dumps(body, indent=2))
            sys.exit(f"Statement {state}")
        if time.time() - start > timeout_sec:
            sys.exit("Timed out waiting for SQL to finish")
        time.sleep(2)

def run(sql: str):
    sid = post_stmt(sql)
    if sid != "SUCCEEDED":
        poll_stmt(sid)

# ---------- build the statements ----------

schema = "quant_finance"
staging = f"{schema}.staging_bars"
hist    = f"{schema}.hist_staging_bars"
integr  = f"{schema}.bars_1m"  # your integration table

# 1) create schema/table objects
statements = [
    f"CREATE SCHEMA IF NOT EXISTS {schema}",
    dedent(f"""
    CREATE TABLE IF NOT EXISTS {staging} (
      ts TIMESTAMP,
      open DOUBLE,
      high DOUBLE,
      low  DOUBLE,
      close DOUBLE,
      volume DOUBLE,
      ingest_run_ts TIMESTAMP
    ) USING DELTA
    """).strip(),
    dedent(f"""
    CREATE TABLE IF NOT EXISTS {hist} (
      load_ts TIMESTAMP,
      ts TIMESTAMP,
      open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE
    ) USING DELTA
    """).strip(),
    dedent(f"""
    CREATE TABLE IF NOT EXISTS {integr} (
      ts TIMESTAMP,
      open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE
    ) USING DELTA
    """).strip(),
]

# 2) load staging from CSV (INSERT VALUES for small files)
values = []
with open("data/bars/latest.csv", newline="", encoding="utf-8") as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        # ts like 2026-03-27T18:42:00+00:00  -> quote as TIMESTAMP literal
        ts = r["ts"].replace("'", "''")
        def num(x): return "NULL" if x.strip()=="" else x
        values.append(
            f"(TIMESTAMP '{ts}', {num(r['open'])}, {num(r['high'])}, "
            f"{num(r['low'])}, {num(r['close'])}, {num(r['volume'])})"
        )

if not values:
    print("No rows in CSV; nothing to load.")
    sys.exit(0)

insert_staging = (
    f"INSERT OVERWRITE {staging} (ts, open, high, low, close, volume) VALUES\n  "
    + ",\n  ".join(values)
)
statements.append(insert_staging)

# 3) tag staging rows with load timestamp (ingest_run_ts)
statements.append(
    f"UPDATE {staging} SET ingest_run_ts = current_timestamp() WHERE ingest_run_ts IS NULL"
)

# 4) append a snapshot to hist_staging
statements.append(
    f"INSERT INTO {hist} SELECT current_timestamp(), ts, open, high, low, close, volume FROM {staging}"
)

# 5) upsert into integration (newest wins)
statements.append(dedent(f"""
MERGE INTO {integr} t
USING {staging} s
ON t.ts = s.ts
WHEN MATCHED THEN UPDATE SET
  t.open=s.open, t.high=s.high, t.low=s.low, t.close=s.close, t.volume=s.volume
WHEN NOT MATCHED THEN INSERT (ts, open, high, low, close, volume)
VALUES (s.ts, s.open, s.high, s.low, s.close, s.volume)
""").strip())

# ---------- execute ----------
for sql in statements:
    print("\n=== Executing ===\n", sql)
    run(sql)

print("\nAll done.")
