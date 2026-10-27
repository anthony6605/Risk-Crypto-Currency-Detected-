"""Check a schema against the registry and register it: python -m src.streaming.register_schema"""

import argparse
import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from src.streaming.avro_codec import SCHEMA_PATH, registry_client, subject_for

DEFAULT_TOPIC = "crypto.prices.avro"
COMPATIBILITY = "BACKWARD"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("schema", nargs="?", default=str(SCHEMA_PATH))
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--check-only", action="store_true", help="don't register, just test compatibility")
    args = parser.parse_args()

    client = registry_client()
    subject = subject_for(args.topic)
    schema = Schema(Path(args.schema).read_text(), schema_type="AVRO")

    client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
    if subject in client.get_subjects():
        compatible = client.test_compatibility(subject, schema)
        print(f"{args.schema} is {'compatible' if compatible else 'NOT compatible'} "
              f"with {subject} ({COMPATIBILITY})")
        if not compatible:
            sys.exit(1)
    else:
        print(f"{subject} has no versions yet; this will be version 1")

    if args.check_only:
        return

    try:
        schema_id = client.register_schema(subject, schema)
    except SchemaRegistryError as exc:
        print(f"Registration refused: {exc}")
        sys.exit(1)
    latest = client.get_latest_version(subject)
    print(f"registered {subject}: schema id {schema_id}, version {latest.version}")


if __name__ == "__main__":
    main()
