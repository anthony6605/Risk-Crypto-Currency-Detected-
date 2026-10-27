from datetime import datetime, timezone

from src.jobs.load_bronze_to_iceberg import hours_to_load

UTC = timezone.utc


def make_hours(root, *hours):
    for day, hour in hours:
        (root / f"date={day}" / f"hour={hour}").mkdir(parents=True)


def test_skips_the_current_hour_and_hours_before_the_watermark(tmp_path):
    make_hours(tmp_path, ("2026-10-25", "08"), ("2026-10-25", "09"), ("2026-10-25", "10"))
    now = datetime(2026, 10, 25, 10, 30, tzinfo=UTC)
    watermark = datetime(2026, 10, 25, 9, tzinfo=UTC)

    starts = [start for start, _ in hours_to_load(tmp_path, watermark, now)]

    assert starts == [datetime(2026, 10, 25, 9, tzinfo=UTC)]


def test_waits_for_late_files_after_the_hour_ends(tmp_path):
    make_hours(tmp_path, ("2026-10-25", "09"))

    assert hours_to_load(tmp_path, None, datetime(2026, 10, 25, 10, 2, tzinfo=UTC)) == []
    assert len(hours_to_load(tmp_path, None, datetime(2026, 10, 25, 10, 6, tzinfo=UTC))) == 1
