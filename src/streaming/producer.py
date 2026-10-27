"""CoinGecko -> Kafka: poll /coins/markets and publish one price event per coin.

CoinGecko is a REST API, not a push stream, so the stream starts with this poller:
one request returns every tracked coin, and each coin becomes one event in Kafka.
"""

import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from confluent_kafka import Producer

from src.config.coins import load_coin_config
from src.ingestion.coingecko import CoinGeckoClient
from src.monitoring import metrics

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "stream.yaml"

SOURCE = "coingecko"
QUOTE_ASSET = "USD"
REQUIRED_FIELDS = ("id", "symbol", "current_price", "high_24h", "low_24h", "last_updated")


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        config = yaml.safe_load(f)

    # Inside Docker this points at the Kafka container; on your Mac it isn't set
    config["kafka"]["bootstrap_servers"] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", config["kafka"]["bootstrap_servers"]
    )
    return config


def iso_to_ms(value):
    # CoinGecko times look like 2026-10-27T00:48:51.000Z
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


class MarketFilter:
    """Decides which /coins/markets rows become Kafka events."""

    def __init__(self, size, exclude=()):
        self.size = size
        self.exclude = set(exclude)
        self.last_updated = {}  # coin id -> last_updated already published

    def select(self, markets):
        """Rows to publish now, at most `size` coins, biggest market cap first."""
        selected = []
        symbols = set()
        tracked = 0

        for market in markets:  # CoinGecko returns them ordered by market cap
            if tracked >= self.size:
                break
            if market.get("id") in self.exclude:
                continue
            if any(market.get(field) is None for field in REQUIRED_FIELDS):
                continue

            # Live tables are keyed by symbol; if two coins share one, the bigger wins
            symbol = market["symbol"].upper()
            if symbol in symbols:
                continue
            symbols.add(symbol)
            tracked += 1

            # Nothing new since the last poll: don't publish the same price again
            if self.last_updated.get(market["id"]) == market["last_updated"]:
                continue
            self.last_updated[market["id"]] = market["last_updated"]
            selected.append(market)

        return selected


def build_event(market, polled_at):
    """Turn one CoinGecko /coins/markets row into RiskCoin's event format."""
    price = float(market["current_price"])
    volume_quote = float(market.get("total_volume") or 0.0)
    event_ms = iso_to_ms(market["last_updated"])
    symbol = market["symbol"].upper()

    return {
        "schema_version": 1,
        "event_id": f"{SOURCE}:{market['id']}:{event_ms}",
        "exchange": SOURCE,
        "pair": f"{symbol}{QUOTE_ASSET}",
        "base_asset": symbol,
        "quote_asset": QUOTE_ASSET,
        "price": price,
        # CoinGecko gives the 24h change rather than the 24h open
        "open_24h": price - float(market.get("price_change_24h") or 0.0),
        "high_24h": float(market["high_24h"]),
        "low_24h": float(market["low_24h"]),
        "volume_24h_base": volume_quote / price if price > 0 else 0.0,
        "volume_24h_quote": volume_quote,
        "event_time": datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).isoformat(),
        "ingested_at": polled_at.isoformat(),
    }


class Stats:
    def __init__(self):
        self.counts = {"polls": 0, "published": 0, "delivered": 0, "failed": 0, "poll_errors": 0}

    def on_delivery(self, err, msg):
        if err is not None:
            self.counts["failed"] += 1
            metrics.DELIVERY_FAILURES.inc()
            print(f"Delivery FAILED for key={msg.key()}: {err}")
        else:
            self.counts["delivered"] += 1

    def line(self):
        return " ".join(f"{k}={v}" for k, v in self.counts.items())


def publish(producer, topic, key, value, stats):
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=stats.on_delivery)
            return
        except BufferError:
            # Local queue is full (e.g. Kafka is down): wait for deliveries, then retry
            producer.poll(1)


def main():
    config = load_config()
    metrics.start_metrics_server(8000, metrics.PRODUCER)
    coins = load_coin_config()["top_n"]
    interval = int(config["poll_interval_seconds"])
    topic = config["kafka"]["topic"]

    client = CoinGeckoClient()
    market_filter = MarketFilter(coins["size"], coins.get("exclude", []))
    # Ask for extra rows so `size` coins remain after the excluded ones are skipped
    per_page = min(250, coins["size"] + len(coins.get("exclude", [])))

    producer = Producer(
        {
            "bootstrap.servers": config["kafka"]["bootstrap_servers"],
            "client.id": "riskcoin-producer",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
            # Hash keys like Kafka's Java tools do (the Python client defaults to CRC32),
            # so a coin lands on the same partition whichever client wrote it
            "partitioner": "murmur2_random",
        }
    )
    stats = Stats()

    # Also publish every event as Avro (the JSON topic stays for the Spark job)
    avro_codec = None
    avro_topic = config["kafka"].get("avro_topic")
    if avro_topic:
        from src.streaming.avro_codec import AvroEventCodec

        avro_codec = AvroEventCodec(avro_topic)
        print(f"Also publishing Avro to '{avro_topic}'")

    # Start each topic's counter at 0, so "published nothing" alerts work before the first event
    for name in filter(None, [topic, avro_topic]):
        metrics.EVENTS_PUBLISHED.labels(topic=name)

    running = True

    def stop(signum=None, frame=None):
        nonlocal running
        print("Stopping producer...")
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"Polling CoinGecko every {interval}s for the top {coins['size']} coins -> '{topic}'. "
          "Press Ctrl+C to stop.")
    try:
        while running:
            started = time.monotonic()
            stats.counts["polls"] += 1
            metrics.POLLS.inc()
            try:
                markets = client.get_market_data(currency="usd", limit=per_page)
                polled_at = datetime.now(timezone.utc)
                selected = market_filter.select(markets)

                for market in selected:
                    event = build_event(market, polled_at)
                    key = event["base_asset"].lower()
                    publish(producer, topic, key, json.dumps(event), stats)
                    metrics.EVENTS_PUBLISHED.labels(topic=topic).inc()
                    if avro_codec is not None:
                        publish(producer, avro_codec.topic, key, avro_codec.encode(event), stats)
                        metrics.EVENTS_PUBLISHED.labels(topic=avro_codec.topic).inc()
                    stats.counts["published"] += 1
                if selected:
                    newest_ms = max(iso_to_ms(market["last_updated"]) for market in selected)
                    metrics.LAST_EXCHANGE_EVENT.set(newest_ms / 1000)

                producer.flush(30)
                print(f"[poll] {len(selected)} new prices | {stats.line()}")
            except requests.RequestException as exc:
                # Network problem or rate limit (429): try again at the next poll
                stats.counts["poll_errors"] += 1
                metrics.POLL_ERRORS.inc()
                print(f"CoinGecko request failed: {exc}. Retrying in {interval}s")

            # Sleep in short steps so Ctrl+C / docker stop is handled quickly
            while running and time.monotonic() - started < interval:
                time.sleep(1)
    finally:
        remaining = producer.flush(10)
        print(f"Flushed. Messages not delivered: {remaining}")


if __name__ == "__main__":
    main()
