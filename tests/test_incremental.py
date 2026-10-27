from datetime import timedelta

import pandas as pd

from src.pipeline.incremental import compute_extraction_window, keep_new_rows


def test_first_run_backfills_30_days(now):
    start, end = compute_extraction_window(None, now)
    assert end == now
    assert start == now - timedelta(days=30)


def test_recent_watermark_still_requests_two_days(now):
    start, _ = compute_extraction_window(now - timedelta(hours=3), now)
    assert start == now - timedelta(days=2)


def test_old_watermark_requests_from_watermark(now):
    watermark = now - timedelta(days=7)
    start, _ = compute_extraction_window(watermark, now)
    assert start == watermark


def test_very_old_watermark_is_capped_at_90_days(now):
    start, _ = compute_extraction_window(now - timedelta(days=200), now)
    assert start == now - timedelta(days=90)


def test_keep_new_rows_drops_rows_at_or_before_watermark(now):
    df = pd.DataFrame({"timestamp": [now - timedelta(hours=h) for h in (2, 1, 0)]})
    kept = keep_new_rows(df, now - timedelta(hours=1))
    assert list(kept["timestamp"]) == [now]


def test_keep_new_rows_without_watermark_keeps_everything(now):
    df = pd.DataFrame({"timestamp": [now]})
    assert len(keep_new_rows(df, None)) == 1
