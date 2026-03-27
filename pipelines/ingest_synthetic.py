import csv, math, random, time, os
from datetime import datetime, timedelta, timezone

# generate 300 minutes of synthetic OHLCV around a drifting mid-price
now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
start = now - timedelta(minutes=299)
mid = 100.0
rows = []
rnd = random.Random(int(now.timestamp()))  # deterministic per run window

t = start
while t <= now:
    mid += rnd.uniform(-0.3, 0.3) + 0.05*math.sin(t.minute/60*2*math.pi)
    o = mid + rnd.uniform(-0.1, 0.1)
    h = o + abs(rnd.uniform(0.0, 0.6))
    l = o - abs(rnd.uniform(0.0, 0.6))
    c = o + rnd.uniform(-0.3, 0.3)
    v = max(1, int(abs(rnd.gauss(250, 60))))
    rows.append([t.isoformat(), round(o,6), round(h,6), round(l,6), round(c,6), v])
    t += timedelta(minutes=1)

os.makedirs("data/bars", exist_ok=True)
out = "data/bars/latest.csv"
with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ts","open","high","low","close","volume"])
    w.writerows(rows)

print(f"Wrote {out} with {len(rows)} rows ending at {now.isoformat()}")
