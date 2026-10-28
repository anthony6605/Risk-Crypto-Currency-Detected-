"""Copy finished hours of stream Bronze (local Parquet) into the Iceberg table."""

import argparse
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from src.database.connection import get_engine
from src.database.control import (
    close_abandoned_audits,
    finish_audit,
    get_watermark,
    start_audit,
    update_watermark,
)
from src.lakehouse.catalog import get_price_events_table

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "stream" / "prices"

PIPELINE_NAME = "iceberg_bronze"
WATERMARK_KEY = "ALL"
# The consumer flushes every 30 seconds and CoinGecko prices are under a minute old when
# polled, so an hour is complete a few minutes after it ends.
SETTLE_TIME = timedelta(minutes=5)

# The first load of an hour has nothing to replace; pyiceberg warns about that.
warnings.filterwarnings("ignore", message="Delete operation did not match any records")


def hours_to_load(root, watermark, now):
    """Return (hour_start, folder) for every finished hour at or after the watermark."""
    hours = []
    for folder in Path(root).glob("date=*/hour=*"):
        day = folder.parent.name.split("=", 1)[1]
        hour = folder.name.split("=", 1)[1]
        start = datetime.fromisoformat(f"{day}T{hour}:00:00+00:00")
        finished = start + timedelta(hours=1) + SETTLE_TIME <= now
        if finished and (watermark is None or start >= watermark):
            hours.append((start, folder))
    return sorted(hours)


def read_hour(folder, arrow_schema):
    df = pd.concat(pd.read_parquet(path) for path in sorted(folder.glob("*.parquet")))
    df = df.drop_duplicates(subset="event_id", keep="first")
    for field in arrow_schema:
        if field.name not in df.columns:
            df[field.name] = None  # a column added to the table after this file was written
    df = df[[field.name for field in arrow_schema]]
    for column in ("event_time", "ingested_at"):
        df[column] = pd.to_datetime(df[column], utc=True).dt.as_unit("us")
    return pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)


def load_hour(table, start, folder):
    """Replace this hour's rows in Iceberg with the files on disk (safe to repeat)."""
    rows = read_hour(folder, table.schema().as_arrow())
    end = start + timedelta(hours=1)
    table.overwrite(
        rows,
        overwrite_filter=And(
            GreaterThanOrEqual("event_time", start.isoformat()),
            LessThan("event_time", end.isoformat()),
        ),
        snapshot_properties={"riskcoin.hour": start.isoformat()},
    )
    return rows.num_rows


def run_once(engine=None, table=None, root=BRONZE_ROOT, now=None):
    engine = engine or get_engine()
    table = table or get_price_events_table()
    now = now or datetime.now(timezone.utc)

    close_abandoned_audits(engine, pipeline_name=PIPELINE_NAME)
    audit_id = start_audit(engine, WATERMARK_KEY, pipeline_name=PIPELINE_NAME)
    loaded = 0
    try:
        watermark = get_watermark(engine, WATERMARK_KEY, pipeline_name=PIPELINE_NAME)
        hours = hours_to_load(root, watermark, now)
        for start, folder in hours:
            loaded += load_hour(table, start, folder)
            update_watermark(engine, WATERMARK_KEY, start + timedelta(hours=1), pipeline_name=PIPELINE_NAME)
            print(f"loaded {start:%Y-%m-%d %H}:00 from {folder}")
        status = "SUCCESS" if hours else "SKIPPED"
        finish_audit(engine, audit_id, status, rows_extracted=loaded, rows_loaded=loaded)
    except Exception as exc:
        finish_audit(engine, audit_id, "FAILED", rows_loaded=loaded, error_message=str(exc)[:1000])
        raise
    return loaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--every", type=int, help="repeat every N seconds")
    args = parser.parse_args()
    while True:
        print(f"rows loaded: {run_once()}")
        if not args.every:
            break
        time.sleep(args.every)


if __name__ == "__main__":
    main()
