import json
import os
import signal
import time
import uuid
from pathlib import Path

import pandas as pd
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError
from sqlalchemy import text

from src.database.connection import get_engine
from src.database.control import finish_audit, start_audit
from src.database.schema import create_tables
from src.monitoring import metrics
from src.streaming.bars import build_bars
from src.streaming.validation import validate_event

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "stream.yaml"
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "stream" / "prices"

PIPELINE_NAME = "price_stream"
GROUP_ID = "riskcoin-consumer"

# A message with a valid Avro header but a corrupt body makes the Avro reader raise
# plain Python errors, not SerializationError. Without catching them, one bad message
# would crash the consumer again on every restart.
AVRO_BODY_ERRORS = (EOFError, IndexError, ValueError, TypeError, UnicodeDecodeError, OverflowError)
FLUSH_MAX_EVENTS = 5000
FLUSH_MAX_SECONDS = 30

UPSERT_LIVE_PRICE = text(
    """
    INSERT INTO live_prices (
        base_asset, exchange, pair, price, open_24h, high_24h, low_24h,
        volume_24h_quote, change_24h_pct, event_time, updated_at
    )
    VALUES (
        :base_asset, :exchange, :pair, :price, :open_24h, :high_24h, :low_24h,
        :volume_24h_quote, :change_24h_pct, :event_time, CURRENT_TIMESTAMP
    )
    ON CONFLICT (base_asset) DO UPDATE SET
        exchange = EXCLUDED.exchange,
        pair = EXCLUDED.pair,
        price = EXCLUDED.price,
        open_24h = EXCLUDED.open_24h,
        high_24h = EXCLUDED.high_24h,
        low_24h = EXCLUDED.low_24h,
        volume_24h_quote = EXCLUDED.volume_24h_quote,
        change_24h_pct = EXCLUDED.change_24h_pct,
        event_time = EXCLUDED.event_time,
        updated_at = CURRENT_TIMESTAMP
    WHERE live_prices.event_time < EXCLUDED.event_time
    """
)

UPSERT_BARS = text(
    """
    INSERT INTO price_bars_1m (
        base_asset, bar_start, open, high, low, close, first_event_time, last_event_time
    )
    VALUES (
        :base_asset, :bar_start, :open, :high, :low, :close, :first_event_time, :last_event_time
    )
    ON CONFLICT (base_asset, bar_start) DO UPDATE SET
        open = CASE WHEN EXCLUDED.first_event_time < price_bars_1m.first_event_time
                    THEN EXCLUDED.open ELSE price_bars_1m.open END,
        high = GREATEST(price_bars_1m.high, EXCLUDED.high),
        low  = LEAST(price_bars_1m.low, EXCLUDED.low),
        close = CASE WHEN EXCLUDED.last_event_time > price_bars_1m.last_event_time
                     THEN EXCLUDED.close ELSE price_bars_1m.close END,
        first_event_time = LEAST(price_bars_1m.first_event_time, EXCLUDED.first_event_time),
        last_event_time  = GREATEST(price_bars_1m.last_event_time, EXCLUDED.last_event_time)
    """
)


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        config = yaml.safe_load(f)

    # Inside Docker this points at the Kafka container; on your Mac it isn't set
    config["kafka"]["bootstrap_servers"] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", config["kafka"]["bootstrap_servers"]
    )
    return config


def events_to_frame(events):
    df = pd.DataFrame(events)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="ISO8601")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True, format="ISO8601")
    return df


def write_bronze(df, root=BRONZE_ROOT):
    """Write events as Parquet, one file per (date, hour) in this batch."""
    files = 0
    partition_date = df["event_time"].dt.strftime("%Y-%m-%d")
    partition_hour = df["event_time"].dt.strftime("%H")

    for (day, hour), part in df.groupby([partition_date, partition_hour]):
        folder = Path(root) / f"date={day}" / f"hour={hour}"
        folder.mkdir(parents=True, exist_ok=True)
        part.to_parquet(folder / f"part-{uuid.uuid4().hex}.parquet", index=False)
        files += 1

    return files


def latest_per_coin(df):
    latest = df.sort_values("event_time").groupby("base_asset").tail(1).copy()
    open_24h = latest["open_24h"].where(latest["open_24h"] > 0)
    latest["change_24h_pct"] = (latest["price"] - open_24h) / open_24h * 100
    return latest


def upsert_live_prices(engine, latest):
    rows = []
    for row in latest.itertuples(index=False):
        rows.append(
            {
                "base_asset": row.base_asset,
                "exchange": row.exchange,
                "pair": row.pair,
                "price": float(row.price),
                "open_24h": float(row.open_24h),
                "high_24h": float(row.high_24h),
                "low_24h": float(row.low_24h),
                "volume_24h_quote": float(row.volume_24h_quote),
                "change_24h_pct": None if pd.isna(row.change_24h_pct) else float(row.change_24h_pct),
                "event_time": row.event_time.to_pydatetime(),
            }
        )

    with engine.begin() as connection:
        connection.execute(UPSERT_LIVE_PRICE, rows)

    return len(rows)


def upsert_bars(engine, bars):
    rows = [
        {
            "base_asset": r.base_asset,
            "bar_start": r.bar_start.to_pydatetime(),
            "open": float(r.open),
            "high": float(r.high),
            "low": float(r.low),
            "close": float(r.close),
            "first_event_time": r.first_event_time.to_pydatetime(),
            "last_event_time": r.last_event_time.to_pydatetime(),
        }
        for r in bars.itertuples(index=False)
    ]
    with engine.begin() as connection:
        connection.execute(UPSERT_BARS, rows)
    return len(rows)


class StreamConsumer:
    def __init__(self, config, engine):
        self.engine = engine
        # Step 49: EVENT_FORMAT=avro switches to the Avro topic (rollback: unset it)
        self.event_format = os.getenv("EVENT_FORMAT", "json")
        self.avro_codec = None
        if self.event_format == "avro":
            from src.streaming.avro_codec import AvroEventCodec

            self.topic = config["kafka"]["avro_topic"]
            self.avro_codec = AvroEventCodec(self.topic)
        else:
            self.topic = config["kafka"]["topic"]
        self.consumer = Consumer(
            {
                "bootstrap.servers": config["kafka"]["bootstrap_servers"],
                "group.id": GROUP_ID,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self.dlq_topic = config["kafka"].get("dlq_topic", "crypto.prices.dlq")
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": config["kafka"]["bootstrap_servers"],
                "client.id": "riskcoin-consumer-dlq",
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self.buffer = []
        self.seen_ids = set()
        self.uncommitted = 0
        self.batch_consumed = 0
        self.batch_rejected = 0
        self.rejected_reasons = {}
        self.last_flush = time.monotonic()
        metrics.LAST_FLUSH.set_to_current_time()  # so "no flush for 25 minutes" counts from start-up
        self.running = True
        self.counts = {"consumed": 0, "duplicates": 0, "rejected": 0, "written": 0, "flushes": 0}

    def stop(self, signum=None, frame=None):
        print("Stop requested, finishing current batch...")
        self.running = False

    def send_to_dlq(self, msg, reasons):
        headers = [
            ("error", ",".join(reasons).encode()),
            ("source_topic", msg.topic().encode()),
            ("source_partition", str(msg.partition()).encode()),
            ("source_offset", str(msg.offset()).encode()),
        ]
        self.dlq_producer.produce(self.dlq_topic, key=msg.key(), value=msg.value(), headers=headers)
        self.dlq_producer.poll(0)

        self.batch_rejected += 1
        self.counts["rejected"] += 1
        for reason in reasons:
            self.rejected_reasons[reason] = self.rejected_reasons.get(reason, 0) + 1
            metrics.EVENTS_REJECTED.labels(reason=reason.split(":")[0]).inc()

    def decode(self, msg):
        """Return the event as a dict, or send the message to the DLQ and return None."""
        if self.avro_codec is not None:
            try:
                return self.avro_codec.decode(msg.value())
            except (SerializationError, *AVRO_BODY_ERRORS):
                self.send_to_dlq(msg, ["bad_avro"])
                return None
            except SchemaRegistryError as exc:
                # An id the registry doesn't know means the message is bad. Any other
                # registry error (it's down) must not send good messages to the DLQ:
                # crash instead, and the restarted consumer re-reads them.
                if exc.http_status_code != 404:
                    raise
                self.send_to_dlq(msg, ["bad_avro"])
                return None
        try:
            return json.loads(msg.value())
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            # TypeError: a message with no value at all
            self.send_to_dlq(msg, ["bad_json"])
            return None

    def handle(self, msg):
        self.uncommitted += 1
        self.batch_consumed += 1
        self.counts["consumed"] += 1
        metrics.MESSAGES_CONSUMED.inc()

        event = self.decode(msg)
        if event is None:
            return

        reasons = validate_event(event)
        if reasons:
            self.send_to_dlq(msg, reasons)
            return

        if event["event_id"] in self.seen_ids:
            self.counts["duplicates"] += 1
            metrics.DUPLICATES_DROPPED.inc()
            return

        self.seen_ids.add(event["event_id"])
        self.buffer.append(event)

    def should_flush(self):
        if self.uncommitted == 0:
            return False
        return (
            len(self.buffer) >= FLUSH_MAX_EVENTS
            or time.monotonic() - self.last_flush >= FLUSH_MAX_SECONDS
        )

    def flush(self):
        if self.uncommitted == 0:
            return

        audit_id = start_audit(self.engine, coin_id=None, pipeline_name=PIPELINE_NAME)
        started = time.monotonic()
        newest_event = None
        try:
            # 1. Bad messages must be safely in the DLQ before we move past them
            undelivered = self.dlq_producer.flush(30)
            if undelivered:
                raise RuntimeError(f"{undelivered} DLQ messages not delivered")

            # 2. Good messages to Bronze, live_prices and bars
            files = coins = bars = 0
            if self.buffer:
                df = events_to_frame(self.buffer)
                files = write_bronze(df)
                coins = upsert_live_prices(self.engine, latest_per_coin(df))
                bars = upsert_bars(self.engine, build_bars(df))
                self.counts["written"] += len(df)
                newest_event = df["event_time"].max()

            # 3. Only now commit the offsets
            self.consumer.commit(asynchronous=False)

            summary = ", ".join(f"{k}={v}" for k, v in sorted(self.rejected_reasons.items())) or None
            finish_audit(
                self.engine, audit_id, "SUCCESS",
                rows_extracted=self.batch_consumed,
                rows_loaded=len(self.buffer),
                rows_rejected=self.batch_rejected,
                error_message=summary,
            )
        except Exception as exc:
            finish_audit(self.engine, audit_id, "FAILED", error_message=str(exc)[:2000])
            metrics.FLUSH_FAILURES.inc()
            raise

        metrics.FLUSH_SECONDS.observe(time.monotonic() - started)
        metrics.LAST_FLUSH.set_to_current_time()
        metrics.EVENTS_WRITTEN.inc(len(self.buffer))
        if newest_event is not None:
            metrics.EVENT_LAG.set(time.time() - newest_event.timestamp())
        self.update_kafka_lag()

        self.counts["flushes"] += 1
        c = self.counts
        print(
            f"[flush] events={len(self.buffer)} rejected={self.batch_rejected} files={files} "
            f"coins={coins} bars={bars} | total consumed={c['consumed']} written={c['written']} "
            f"rejected={c['rejected']} duplicates={c['duplicates']}"
        )

        self.buffer = []
        self.seen_ids = set()
        self.uncommitted = 0
        self.batch_consumed = 0
        self.batch_rejected = 0
        self.rejected_reasons = {}
        self.last_flush = time.monotonic()

    def update_kafka_lag(self):
        """Messages per partition that Kafka has but this consumer hasn't read yet."""
        try:
            assigned = self.consumer.assignment()
            for tp, position in zip(assigned, self.consumer.position(assigned)):
                low, high = self.consumer.get_watermark_offsets(tp, timeout=5)
                read_up_to = position.offset if position.offset >= 0 else low
                metrics.KAFKA_LAG.labels(partition=str(tp.partition)).set(max(high - read_up_to, 0))
        except KafkaException as exc:
            print(f"Could not read Kafka lag: {exc}")

    def run(self):
        self.consumer.subscribe([self.topic])
        print(f"Consuming '{self.topic}' ({self.event_format}) as group '{GROUP_ID}'. Press Ctrl+C to stop.")

        try:
            while self.running:
                for msg in self.consumer.consume(num_messages=500, timeout=1.0):
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"Kafka error: {msg.error()}")
                        continue
                    self.handle(msg)

                if self.should_flush():
                    self.flush()
        finally:
            # Close the consumer even if this last flush fails (e.g. PostgreSQL is down).
            # Anything not committed is simply read again on the next start.
            try:
                self.flush()
            finally:
                self.dlq_producer.flush(10)
                self.consumer.close()
                print("Consumer closed.")


def main():
    config = load_config()
    metrics.start_metrics_server(8001, metrics.CONSUMER)
    engine = get_engine()
    create_tables(engine)

    stream_consumer = StreamConsumer(config, engine)
    signal.signal(signal.SIGINT, stream_consumer.stop)
    signal.signal(signal.SIGTERM, stream_consumer.stop)
    stream_consumer.run()


if __name__ == "__main__":
    main()
