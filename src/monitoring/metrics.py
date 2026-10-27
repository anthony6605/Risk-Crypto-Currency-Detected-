"""Prometheus metrics for the streaming services. Each service serves its own at /metrics.

The metrics are created unregistered (registry=None), and each service registers only its
own group when it starts the metrics server. Otherwise the producer would also export the
consumer's metrics as zeros (and the reverse), and a "last flush" time of 0, in 1970, would
make the "consumer hasn't flushed" alert fire for the producer.
"""

import os

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------- producer
POLLS = Counter("riskcoin_producer_polls", "CoinGecko /coins/markets requests made", registry=None)
POLL_ERRORS = Counter(
    "riskcoin_producer_poll_errors", "CoinGecko requests that failed (retried at the next poll)",
    registry=None,
)
EVENTS_PUBLISHED = Counter(
    "riskcoin_producer_events_published", "Events handed to the Kafka client", ["topic"], registry=None
)
DELIVERY_FAILURES = Counter(
    "riskcoin_producer_delivery_failures", "Messages Kafka reported as not delivered", registry=None
)
LAST_EXCHANGE_EVENT = Gauge(
    "riskcoin_producer_last_event_timestamp_seconds",
    "Event time (CoinGecko last_updated) of the newest published event",
    registry=None,
)
PRODUCER = (POLLS, POLL_ERRORS, EVENTS_PUBLISHED, DELIVERY_FAILURES, LAST_EXCHANGE_EVENT)

# ---------------------------------------------------------------- consumer
MESSAGES_CONSUMED = Counter("riskcoin_consumer_messages_consumed", "Kafka messages read", registry=None)
EVENTS_WRITTEN = Counter(
    "riskcoin_consumer_events_written", "Valid events written to Bronze and PostgreSQL", registry=None
)
EVENTS_REJECTED = Counter(
    "riskcoin_consumer_events_rejected", "Messages sent to the DLQ", ["reason"], registry=None
)
DUPLICATES_DROPPED = Counter(
    "riskcoin_consumer_duplicates_dropped", "Repeated event_ids skipped", registry=None
)
FLUSH_SECONDS = Histogram(
    "riskcoin_consumer_flush_duration_seconds",
    "Time to write one batch (DLQ, Bronze, PostgreSQL, commit)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
    registry=None,
)
FLUSH_FAILURES = Counter("riskcoin_consumer_flush_failures", "Batches that raised an error", registry=None)
LAST_FLUSH = Gauge(
    "riskcoin_consumer_last_flush_timestamp_seconds", "When the last batch was committed", registry=None
)
EVENT_LAG = Gauge(
    "riskcoin_consumer_event_lag_seconds",
    "Seconds between the newest event_time and the flush that wrote it",
    registry=None,
)
KAFKA_LAG = Gauge(
    "riskcoin_consumer_kafka_lag_messages",
    "Messages in Kafka not yet read, per partition",
    ["partition"],
    registry=None,
)
CONSUMER = (
    MESSAGES_CONSUMED, EVENTS_WRITTEN, EVENTS_REJECTED, DUPLICATES_DROPPED, FLUSH_SECONDS,
    FLUSH_FAILURES, LAST_FLUSH, EVENT_LAG, KAFKA_LAG,
)


def start_metrics_server(default_port, service_metrics):
    for metric in service_metrics:
        REGISTRY.register(metric)
    port = int(os.getenv("METRICS_PORT", default_port))
    start_http_server(port)
    print(f"Metrics on http://localhost:{port}/metrics")
