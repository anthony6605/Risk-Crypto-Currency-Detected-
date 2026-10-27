from datetime import datetime, timedelta, timezone

import pytest

NOW = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def now():
    return NOW


@pytest.fixture
def make_event():
    """Factory for valid producer events. Override any field with keyword arguments."""
    def _make(base="BTC", price=100.0, seconds=0, event_id=None, **overrides):
        event_time = NOW + timedelta(seconds=seconds)
        event = {
            "schema_version": 1,
            "event_id": event_id or f"coingecko:{base.lower()}:{int(event_time.timestamp() * 1000)}",
            "exchange": "coingecko",
            "pair": f"{base}USD",
            "base_asset": base,
            "quote_asset": "USD",
            "price": price,
            "open_24h": 95.0,
            "high_24h": 110.0,
            "low_24h": 90.0,
            "volume_24h_base": 1000.0,
            "volume_24h_quote": 5_000_000.0,
            "event_time": event_time.isoformat(),
            "ingested_at": event_time.isoformat(),
        }
        event.update(overrides)
        return event

    return _make
