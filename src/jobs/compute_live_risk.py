import argparse
import time
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

from src.database.connection import get_engine
from src.database.control import close_abandoned_audits, finish_audit, start_audit
from src.database.schema import create_tables
from src.risk.live_features import MIN_BARS, compute_features, score_risk

PIPELINE_NAME = "live_risk"
BAR_RETENTION_DAYS = 8

READ_BARS = text(
    """
    SELECT base_asset, bar_start, close
    FROM price_bars_1m
    WHERE bar_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    """
)

READ_CATALOG = text("SELECT symbol, coin_id, name FROM coin_catalog")

UPSERT_RISK = text(
    """
    INSERT INTO live_risk (
        base_asset, coin_id, name, price, return_24h, vol_1h, vol_24h,
        max_drawdown_24h, bars_24h, risk_score, risk_level, computed_at
    )
    VALUES (
        :base_asset, :coin_id, :name, :price, :return_24h, :vol_1h, :vol_24h,
        :max_drawdown_24h, :bars_24h, :risk_score, :risk_level, :computed_at
    )
    ON CONFLICT (base_asset) DO UPDATE SET
        coin_id = EXCLUDED.coin_id,
        name = EXCLUDED.name,
        price = EXCLUDED.price,
        return_24h = EXCLUDED.return_24h,
        vol_1h = EXCLUDED.vol_1h,
        vol_24h = EXCLUDED.vol_24h,
        max_drawdown_24h = EXCLUDED.max_drawdown_24h,
        bars_24h = EXCLUDED.bars_24h,
        risk_score = EXCLUDED.risk_score,
        risk_level = EXCLUDED.risk_level,
        computed_at = EXCLUDED.computed_at
    """
)

DELETE_STALE_RISK = text("DELETE FROM live_risk WHERE computed_at < :computed_at")

DELETE_OLD_BARS = text(
    "DELETE FROM price_bars_1m WHERE bar_start < CURRENT_TIMESTAMP - make_interval(days => :days)"
)

# The consumer writes one audit row per flush (one per poll, about 150 a day); keep 14 days
DELETE_OLD_STREAM_AUDIT = text(
    """
    DELETE FROM pipeline_audit
    WHERE pipeline_name = 'price_stream'
      AND started_at < CURRENT_TIMESTAMP - INTERVAL '14 days'
    """
)


def to_db_value(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "item"):  # numpy number -> plain Python number
        return value.item()
    return value


def run_once(engine, min_bars=MIN_BARS):
    close_abandoned_audits(engine, pipeline_name=PIPELINE_NAME)
    audit_id = start_audit(engine, coin_id=None, pipeline_name=PIPELINE_NAME)
    computed_at = datetime.now(timezone.utc)

    try:
        with engine.connect() as connection:
            bars = pd.read_sql(READ_BARS, connection)
            catalog = pd.read_sql(READ_CATALOG, connection)

        scored = score_risk(compute_features(bars, min_bars=min_bars))

        names = catalog.set_index("symbol")
        symbols = scored["base_asset"].str.lower()
        scored["coin_id"] = symbols.map(names["coin_id"])
        scored["name"] = symbols.map(names["name"])
        scored["computed_at"] = computed_at

        rows = [
            {key: to_db_value(value) for key, value in row.items()}
            for row in scored.to_dict(orient="records")
        ]

        with engine.begin() as connection:
            if rows:
                connection.execute(UPSERT_RISK, rows)
            connection.execute(DELETE_STALE_RISK, {"computed_at": computed_at})
            deleted_bars = connection.execute(DELETE_OLD_BARS, {"days": BAR_RETENTION_DAYS}).rowcount
            connection.execute(DELETE_OLD_STREAM_AUDIT)

        finish_audit(engine, audit_id, "SUCCESS", rows_extracted=len(bars), rows_loaded=len(rows))
        print(f"Live risk computed for {len(rows)} coins from {len(bars)} bars "
              f"(removed {deleted_bars} old bars).")

    except Exception as exc:
        finish_audit(engine, audit_id, "FAILED", error_message=str(exc)[:2000])
        print(f"Live risk FAILED: {exc}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Compute live risk scores from 1-minute bars")
    parser.add_argument("--every", type=int, default=None, help="Repeat every N seconds")
    parser.add_argument("--min-bars", type=int, default=MIN_BARS,
                        help=f"Price updates a coin needs in 24h to be scored (default {MIN_BARS}, "
                             "about 10 hours of 10-minute polls; use a small number to test sooner)")
    args = parser.parse_args()

    engine = get_engine()
    create_tables(engine)

    while True:
        try:
            run_once(engine, min_bars=args.min_bars)
        except Exception:
            if args.every is None:
                raise SystemExit(1)
        if args.every is None:
            break
        time.sleep(args.every)


if __name__ == "__main__":
    main()
