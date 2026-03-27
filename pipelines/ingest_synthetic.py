# pipelines/ingest_synthetic.py
from datetime import datetime, timedelta, timezone
import random, csv, os

OUT = "data/bars/latest.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
start = end - timedelta(minutes=299)  # ~300 rows

rows = []
price = 50.0
for i in range(300):
    ts = start + timedelta(minutes=i)
    # simple random walk with sane OHLC constraints
    change = random.uniform(-0.5, 0.5)
    open_ = price
    high_ = max(open_, open_ + abs(change) * random.uniform(0.2, 1.2))
    low_  = min(open_, open_ - abs(change) * random.uniform(0.2, 1.2))
    close_= open_ + change
    vol   = max(0.0, random.uniform(10, 2000))
    price = close_
    rows.append([ts.isoformat(), round(open_,4), round(high_,4), round(low_,4), round(close_,4), round(vol,4)])

with open(OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts","open","high","low","close","volume"])
    w.writerows(rows)

print(f"Wrote {OUT} with {len(rows)} rows ending at {rows[-1][0]}")
