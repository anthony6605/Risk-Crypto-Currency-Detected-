import json
import uuid
from collections import defaultdict
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition

TOPIC = "crypto.prices.raw"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"check-{uuid.uuid4()}",
        "enable.auto.commit": False,
    }
)

# Read everything up to the end of each partition as it is right now.
# (Stopping after a few empty polls never happens while the producer keeps writing.)
partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
end_offsets = {}
assignments = []
for p in partitions:
    low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    if high > low:
        end_offsets[p] = high
        assignments.append(TopicPartition(TOPIC, p, low))
consumer.assign(assignments)

events = 0
bad = 0
partitions_by_key = defaultdict(set)
last_time_by_pair = {}
out_of_order = 0
seen_ids = set()
duplicate_ids = 0
finished = set()
idle_polls = 0

while len(finished) < len(end_offsets) and idle_polls < 10:
    msg = consumer.poll(1.0)
    if msg is None:
        idle_polls += 1
        continue
    idle_polls = 0
    if msg.error():
        continue

    if msg.offset() >= end_offsets[msg.partition()] - 1:
        finished.add(msg.partition())
    if msg.offset() >= end_offsets[msg.partition()]:
        continue  # arrived after the snapshot

    event = json.loads(msg.value())
    events += 1
    partitions_by_key[msg.key().decode()].add(msg.partition())

    if event["price"] <= 0 or event["volume_24h_quote"] <= 0:
        bad += 1

    if event["event_id"] in seen_ids:
        duplicate_ids += 1
    seen_ids.add(event["event_id"])

    event_time = datetime.fromisoformat(event["event_time"])
    previous = last_time_by_pair.get(event["pair"])
    if previous and event_time < previous:
        out_of_order += 1
    last_time_by_pair[event["pair"]] = event_time

consumer.close()

split_keys = [k for k, p in partitions_by_key.items() if len(p) > 1]
print(f"events read:            {events}")
print(f"distinct coins:         {len(partitions_by_key)}")
print(f"bad price/volume:       {bad}")
print(f"out-of-order per pair:  {out_of_order}")
print(f"keys on >1 partition:   {len(split_keys)}")
print(f"duplicate event_ids:    {duplicate_ids}")
