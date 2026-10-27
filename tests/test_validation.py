import pytest

from src.streaming.validation import validate_event


def test_valid_event_has_no_errors(make_event, now):
    assert validate_event(make_event(), now) == []


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"price": 0.0}, "price_not_positive"),
        ({"price": float("nan")}, "price_not_positive"),
        ({"price": 500.0}, "price_outside_24h_range"),
        ({"high_24h": 80.0}, "high_below_low"),
        ({"volume_24h_quote": -1.0}, "volume_negative"),
        ({"schema_version": 99}, "unsupported_schema_version"),
        ({"event_time": "2026-10-25T14:00:00+00:00"}, "event_time_in_future"),
        ({"event_time": "2026-10-25T09:00:00+00:00"}, "event_time_too_old"),
        ({"event_time": "yesterday"}, "event_time_unparseable"),
        ({"event_time": "2026-10-25T12:00:00"}, "event_time_not_utc"),
    ],
)
def test_bad_values_are_rejected(make_event, now, overrides, expected):
    assert expected in validate_event(make_event(**overrides), now)


def test_missing_and_wrong_type_fields(make_event, now):
    event = make_event()
    del event["price"]
    event["volume_24h_quote"] = "lots"
    errors = validate_event(event, now)
    assert "missing:price" in errors
    assert "wrong_type:volume_24h_quote" in errors


def test_bool_is_not_accepted_as_a_number(make_event, now):
    assert "wrong_type:price" in validate_event(make_event(price=True), now)


def test_non_object_is_rejected(now):
    assert validate_event([1, 2, 3], now) == ["not_an_object"]
