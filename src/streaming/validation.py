import math
from datetime import datetime, timedelta, timezone

SUPPORTED_SCHEMA_VERSIONS = {1}
MAX_FUTURE_SKEW = timedelta(seconds=60)
MAX_EVENT_AGE = timedelta(hours=1)
PRICE_RANGE_TOLERANCE = 0.01  # 1%

REQUIRED_FIELDS = {
    "schema_version": int,
    "event_id": str,
    "exchange": str,
    "pair": str,
    "base_asset": str,
    "price": (int, float),
    "high_24h": (int, float),
    "low_24h": (int, float),
    "open_24h": (int, float),
    "volume_24h_quote": (int, float),
    "event_time": str,
    "ingested_at": str,
}


def validate_event(event, now=None):
    """Return a list of problems. An empty list means the event is valid."""
    now = now or datetime.now(timezone.utc)

    if not isinstance(event, dict):
        return ["not_an_object"]

    errors = []

    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in event or event[field] is None:
            errors.append(f"missing:{field}")
        elif isinstance(event[field], bool) or not isinstance(event[field], expected_type):
            errors.append(f"wrong_type:{field}")

    if errors:
        return errors  # the checks below need the fields to exist and have the right type

    if event["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
        errors.append("unsupported_schema_version")

    price = event["price"]
    if not math.isfinite(price) or price <= 0:
        errors.append("price_not_positive")

    if not math.isfinite(event["volume_24h_quote"]) or event["volume_24h_quote"] < 0:
        errors.append("volume_negative")

    low, high = event["low_24h"], event["high_24h"]
    if high < low:
        errors.append("high_below_low")
    elif price > 0 and not (low * (1 - PRICE_RANGE_TOLERANCE) <= price <= high * (1 + PRICE_RANGE_TOLERANCE)):
        errors.append("price_outside_24h_range")

    try:
        event_time = datetime.fromisoformat(event["event_time"])
        if event_time.tzinfo is None:
            errors.append("event_time_not_utc")
        elif event_time > now + MAX_FUTURE_SKEW:
            errors.append("event_time_in_future")
        elif event_time < now - MAX_EVENT_AGE:
            errors.append("event_time_too_old")
    except ValueError:
        errors.append("event_time_unparseable")

    return errors
