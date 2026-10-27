import io
import json
from datetime import datetime, timezone

import fastavro

from src.streaming.avro_codec import SCHEMA_PATH, event_to_record, record_to_event
from src.streaming.producer import build_event

MARKET = {"id": "bitcoin", "symbol": "btc", "current_price": 68000.5, "high_24h": 69000,
          "low_24h": 66000, "price_change_24h": 1000.5, "total_volume": 6_800_000,
          "last_updated": "2026-10-25T12:00:00.123Z"}
POLLED_AT = datetime(2026, 10, 25, 12, 0, 5, 456789, tzinfo=timezone.utc)


def load_schema():
    return json.loads(SCHEMA_PATH.read_text())


def test_schema_has_exactly_the_producer_fields():
    event = build_event(MARKET, POLLED_AT)
    schema_fields = {field["name"] for field in load_schema()["fields"]}

    assert set(event) == schema_fields


def test_event_survives_an_avro_round_trip():
    schema = fastavro.parse_schema(load_schema())
    event = build_event(MARKET, POLLED_AT)

    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, event_to_record(event))
    buffer.seek(0)

    assert record_to_event(fastavro.schemaless_reader(buffer, schema)) == event
