from datetime import datetime, timedelta, timezone

import pandas as pd

INITIAL_LOOKBACK_DAYS = 30  # first run: backfill 30 days
MIN_WINDOW_DAYS = 2         # always ask for at least 2 days (keeps hourly granularity)
MAX_WINDOW_DAYS = 90        # CoinGecko switches to daily data above 90 days


def compute_extraction_window(
    watermark,
    now=None,
):
    now = now or datetime.now(timezone.utc)

    if watermark is None:
        start = now - timedelta(days=INITIAL_LOOKBACK_DAYS)
    else:
        start = min(
            watermark,
            now - timedelta(days=MIN_WINDOW_DAYS),
        )

    earliest_allowed = now - timedelta(days=MAX_WINDOW_DAYS)

    if start < earliest_allowed:
        print(
            f"Watermark is older than {MAX_WINDOW_DAYS} days; "
            f"capping the window at {earliest_allowed}."
        )
        start = earliest_allowed

    return start, now


def keep_new_rows(
    df: pd.DataFrame,
    watermark,
) -> pd.DataFrame:
    """Keep only rows strictly newer than the watermark."""

    if watermark is None:
        return df

    return df[df["timestamp"] > watermark]
