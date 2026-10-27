from datetime import datetime, timezone

from src.streaming.producer import MarketFilter, build_event
from src.streaming.validation import validate_event

POLLED_AT = datetime(2026, 10, 25, 12, 0, 30, tzinfo=timezone.utc)


def market(coin_id="bitcoin", symbol="btc", price=100.0, last_updated="2026-10-25T12:00:00.000Z", **extra):
    row = {"id": coin_id, "symbol": symbol, "current_price": price, "high_24h": 110.0, "low_24h": 90.0,
           "price_change_24h": 5.0, "total_volume": 1_000_000.0, "last_updated": last_updated}
    row.update(extra)
    return row


def test_build_event_maps_coingecko_fields():
    event = build_event(market(), POLLED_AT)
    assert event["event_id"] == "coingecko:bitcoin:1792929600000"
    assert (event["base_asset"], event["pair"], event["quote_asset"]) == ("BTC", "BTCUSD", "USD")
    assert event["open_24h"] == 95.0          # price minus the 24h change
    assert event["volume_24h_base"] == 10_000.0
    assert event["event_time"] == "2026-10-25T12:00:00+00:00"


def test_built_events_pass_validation():
    assert validate_event(build_event(market(), POLLED_AT), now=POLLED_AT) == []


def test_excluded_and_incomplete_coins_are_skipped():
    f = MarketFilter(size=10, exclude=["tether"])
    rows = [market("tether", "usdt"), market("ethereum", "eth", high_24h=None), market()]
    assert [m["id"] for m in f.select(rows)] == ["bitcoin"]


def test_shared_symbol_keeps_the_bigger_coin():
    f = MarketFilter(size=10)
    rows = [market("bitcoin", "btc"), market("bitcoin-clone", "btc")]  # ordered by market cap
    assert [m["id"] for m in f.select(rows)] == ["bitcoin"]


def test_unchanged_price_is_not_published_twice():
    f = MarketFilter(size=10)
    assert len(f.select([market()])) == 1
    assert f.select([market()]) == []                                        # same last_updated
    assert len(f.select([market(last_updated="2026-10-25T12:01:00.000Z")])) == 1


def test_size_limits_the_number_of_coins():
    f = MarketFilter(size=2)
    rows = [market(f"coin-{i}", f"c{i}") for i in range(5)]
    assert [m["id"] for m in f.select(rows)] == ["coin-0", "coin-1"]
