"""Avro encoding of price events, with schemas stored in the Confluent Schema Registry."""

import os
from datetime import datetime
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "price_event.avsc"

# The rest of the pipeline passes timestamps as ISO-8601 strings; Avro stores them as numbers.
TIME_FIELDS = ("event_time", "ingested_at")


def registry_client():
    return SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})


def subject_for(topic):
    """Default subject naming: one schema history per topic's message values."""
    return f"{topic}-value"


def event_to_record(event, ctx=None):
    return {**event, **{field: datetime.fromisoformat(event[field]) for field in TIME_FIELDS}}


def record_to_event(record, ctx=None):
    return {**record, **{field: record[field].isoformat() for field in TIME_FIELDS}}


class AvroEventCodec:
    def __init__(self, topic, client=None, schema_path=SCHEMA_PATH):
        client = client or registry_client()
        schema_str = Path(schema_path).read_text()
        self.topic = topic
        self.context = SerializationContext(topic, MessageField.VALUE)
        # auto.register.schemas=False: the producer can only write schemas that were
        # registered (and compatibility-checked) beforehand by register_schema.py.
        self.serializer = AvroSerializer(
            client, schema_str, to_dict=event_to_record, conf={"auto.register.schemas": False}
        )
        # Passing our schema as the reader schema makes Avro convert data written with
        # older (compatible) schemas into the shape this code expects.
        self.deserializer = AvroDeserializer(client, schema_str, from_dict=record_to_event)

    def encode(self, event):
        return self.serializer(event, self.context)

    def decode(self, value):
        return self.deserializer(value, self.context)
