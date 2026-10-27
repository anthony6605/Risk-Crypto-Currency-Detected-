import sys
from pathlib import Path

# Make the project root importable when run as `python scripts/rebuild_bars.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from src.database.connection import get_engine  # noqa: E402
from src.streaming.bars import build_bars  # noqa: E402
from src.streaming.consumer import BRONZE_ROOT, upsert_bars  # noqa: E402

day = sys.argv[1]  # e.g. 2026-10-25
df = pd.read_parquet(BRONZE_ROOT / f"date={day}")
df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")

bars = build_bars(df)
print(f"{len(df)} events -> {len(bars)} bars")
print(f"Upserted {upsert_bars(get_engine(), bars)} bars")
