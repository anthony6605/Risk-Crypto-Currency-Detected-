import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

TOPIC = "crypto.prices.dlq"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dlq-inspect-{uuid.uuid4()}",
        "enable.auto.commit": False,
    }
)

# Read every message currently in the DLQ, then stop
partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
end_offsets = {}
assignments = []
for p in partitions:
    low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    if high > low:
        end_offsets[p] = high
        assignments.append(TopicPartition(TOPIC, p, low))
consumer.assign(assignments)

reasons = Counter()
examples = {}
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
        continue  # arrived after we started

    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    for reason in headers.get("error", "unknown").split(","):
        reasons[reason] += 1
        examples.setdefault(reason, (msg.value() or b"")[:200])

consumer.close()

print(f"DLQ messages by reason ({sum(reasons.values())} reasons total):")
for reason, count in reasons.most_common():
    print(f"  {reason:<30} {count:>6}   e.g. {examples[reason]!r}")
