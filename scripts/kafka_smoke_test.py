import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092"
# Its own topic (created by kafka-init, kept for 1 hour), so the test messages
# never reach the consumer, Spark or scripts/check_stream.py
TOPIC = "crypto.smoke_test"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery FAILED: {err}")
    else:
        print(f"Delivered key={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP,
        # Hash keys like Kafka's Java tools do (the Python client defaults to CRC32),
        # so a coin lands on the same partition whichever client wrote it
        "partitioner": "murmur2_random",
    }
)

for coin_id in ["bitcoin", "ethereum", "solana"]:
    event = {
        "coin_id": coin_id,
        "price_usd": 0.0,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "smoke_test",
    }
    producer.produce(
        TOPIC,
        key=coin_id,
        value=json.dumps(event),
        callback=delivery_report,
    )

producer.flush()

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"smoke-test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

received = 0
empty_polls = 0
while empty_polls < 5:
    msg = consumer.poll(1.0)
    if msg is None:
        empty_polls += 1
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    received += 1
    print(f"Read  key={msg.key().decode()} partition={msg.partition()} value={msg.value().decode()}")

consumer.close()
print(f"Total messages read: {received}")
