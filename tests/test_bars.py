import pandas as pd

from src.streaming.bars import build_bars


def to_frame(events):
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    return df


def test_ohlc_within_one_minute(make_event):
    df = to_frame([
        make_event(price=100.0, seconds=0),
        make_event(price=105.0, seconds=10),
        make_event(price=95.0, seconds=20),
        make_event(price=101.0, seconds=30),
    ])
    bars = build_bars(df)
    assert len(bars) == 1
    bar = bars.iloc[0]
    assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 105.0, 95.0, 101.0)


def test_open_and_close_follow_event_time_not_arrival_order(make_event):
    df = to_frame([make_event(price=101.0, seconds=30), make_event(price=100.0, seconds=0)])
    bar = build_bars(df).iloc[0]
    assert bar.open == 100.0 and bar.close == 101.0


def test_separate_bars_per_minute_and_coin(make_event):
    df = to_frame([
        make_event(base="BTC", seconds=0),
        make_event(base="BTC", seconds=60),
        make_event(base="ETH", seconds=0),
    ])
    assert len(build_bars(df)) == 3


def test_timestamps_with_and_without_fractions_parse(make_event):
    whole = make_event(price=100.0, event_time="2026-10-25T12:00:00+00:00")
    fraction = make_event(price=101.0, event_time="2026-10-25T12:00:30.250000+00:00", event_id="b")
    bar = build_bars(to_frame([fraction, whole])).iloc[0]
    assert bar.open == 100.0 and bar.close == 101.0


def test_duplicate_events_are_ignored(make_event):
    event = make_event(price=100.0)
    duplicate = dict(event, price=999.0)  # same event_id, different price
    bar = build_bars(to_frame([event, duplicate])).iloc[0]
    assert bar.high == 100.0
