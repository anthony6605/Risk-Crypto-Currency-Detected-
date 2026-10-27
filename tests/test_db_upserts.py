import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.database.schema import create_tables
from src.streaming.bars import build_bars
from src.streaming.consumer import latest_per_coin, upsert_bars, upsert_live_prices

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"), reason="set TEST_DATABASE_URL to run database tests"
)


@pytest.fixture
def engine():
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    create_tables(engine)
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE price_bars_1m, live_prices"))
    yield engine
    engine.dispose()


def to_frame(events):
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, format="ISO8601")
    return df


def read_bar(engine):
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT open, high, low, close FROM price_bars_1m")
        ).fetchall()


def test_bar_upsert_is_idempotent(engine, make_event):
    df = to_frame([make_event(price=p, seconds=s) for p, s in [(100, 0), (105, 10), (101, 20)]])
    bars = build_bars(df)

    upsert_bars(engine, bars)
    upsert_bars(engine, bars)  # replay

    assert read_bar(engine) == [(100.0, 105.0, 100.0, 101.0)]


def test_bar_merges_across_flushes_in_any_order(engine, make_event):
    first = [make_event(price=100, seconds=0), make_event(price=95, seconds=10)]
    second = [make_event(price=110, seconds=40), make_event(price=104, seconds=50)]

    upsert_bars(engine, build_bars(to_frame(second)))  # later half arrives first
    upsert_bars(engine, build_bars(to_frame(first)))

    assert read_bar(engine) == [(100.0, 110.0, 95.0, 104.0)]


def test_live_price_ignores_older_events(engine, make_event):
    newer = to_frame([make_event(price=105, seconds=30)])
    older = to_frame([make_event(price=100, seconds=0)])

    upsert_live_prices(engine, latest_per_coin(newer))
    upsert_live_prices(engine, latest_per_coin(older))

    with engine.connect() as connection:
        price = connection.execute(text("SELECT price FROM live_prices")).scalar_one()
    assert price == 105.0
