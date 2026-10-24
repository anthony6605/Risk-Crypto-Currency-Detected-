import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets
import yaml
from confluent_kafka import Producer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "stream.yaml"
STATS_EVERY_SECONDS = 30


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        return yaml.safe_load(f)


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class TickerFilter:
    """Decides which ticker updates become Kafka events."""

    def __init__(self, config):
        # Longest first, so e.g. "USDT" is tried before "USD"
        self.quote_assets = sorted(config["quote_assets"], key=len, reverse=True)
        self.exclude = {a.upper() for a in config.get("exclude_base_assets", [])}
        self.min_quote_volume = float(config.get("min_quote_volume_24h", 0))
        self.min_interval_ms = int(config.get("min_publish_interval_seconds", 0) * 1000)
        self.last_published_ms = {}
        self.main_pair = {}  # base asset -> (pair, 24h quote volume)

    def split_pair(self, pair):
        for quote in self.quote_assets:
            if pair.endswith(quote) and len(pair) > len(quote):
                return pair[: -len(quote)], quote
        return None, None

    def is_main_pair(self, base, pair, quote_volume):
        """
        A coin can trade against several quote assets (on Binance.US: BTCUSD and
        BTCUSDT, a few dollars apart). Mixing them would look like price jumps, so
        only the most liquid pair is published and each coin has one price series.
        """
        main = self.main_pair.get(base)
        if main is None or main[0] == pair or quote_volume > main[1]:
            self.main_pair[base] = (pair, quote_volume)
            return True
        return False

    def seed_main_pairs(self, tickers_24h):
        """
        Pick each coin's main pair up front from the exchange's 24h ticker list.
        Otherwise the first pair to trade after a restart wins until the busier
        one trades, and that switch shows up as a fake price jump.
        """
        for t in tickers_24h:
            base, _ = self.split_pair(t["symbol"])
            quote_volume = float(t["quoteVolume"])
            if base is None or base in self.exclude or quote_volume < self.min_quote_volume:
                continue
            self.is_main_pair(base, t["symbol"], quote_volume)

    def accept(self, ticker):
        """Return (base, quote) if this update should be published, else None."""
        base, quote = self.split_pair(ticker["s"])
        if base is None or base in self.exclude:
            return None

        quote_volume = float(ticker["q"])
        if quote_volume < self.min_quote_volume:
            return None

        if not self.is_main_pair(base, ticker["s"], quote_volume):
            return None

        event_ms = ticker["E"]
        last_ms = self.last_published_ms.get(ticker["s"])
        if last_ms is not None and event_ms - last_ms < self.min_interval_ms:
            return None

        self.last_published_ms[ticker["s"]] = event_ms
        return base, quote


def build_event(ticker, base, quote, exchange):
    """Turn a raw Binance miniTicker into RiskCoin's event format."""
    return {
        "schema_version": 1,
        "event_id": f"{exchange}:{ticker['s']}:{ticker['E']}",
        "exchange": exchange,
        "pair": ticker["s"],
        "base_asset": base,
        "quote_asset": quote,
        "price": float(ticker["c"]),
        "open_24h": float(ticker["o"]),
        "high_24h": float(ticker["h"]),
        "low_24h": float(ticker["l"]),
        "volume_24h_base": float(ticker["v"]),
        "volume_24h_quote": float(ticker["q"]),
        "event_time": ms_to_iso(ticker["E"]),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


class Stats:
    def __init__(self):
        self.counts = {"received": 0, "filtered": 0, "published": 0, "delivered": 0, "failed": 0}
        self.coins = set()
        self.last_print = time.monotonic()

    def on_delivery(self, err, msg):
        if err is not None:
            self.counts["failed"] += 1
            print(f"Delivery FAILED for key={msg.key()}: {err}")
        else:
            self.counts["delivered"] += 1

    def maybe_print(self):
        if time.monotonic() - self.last_print < STATS_EVERY_SECONDS:
            return
        c = self.counts
        print(
            f"[stats] received={c['received']} filtered={c['filtered']} "
            f"published={c['published']} delivered={c['delivered']} "
            f"failed={c['failed']} coins_seen={len(self.coins)}"
        )
        self.last_print = time.monotonic()


def publish(producer, topic, key, value, stats):
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=stats.on_delivery)
            return
        except BufferError:
            # Local queue is full (e.g. Kafka is down): wait for deliveries, then retry
            producer.poll(1)


async def stream_forever(config, producer, ticker_filter, stats):
    url = config["websocket_url"]
    topic = config["kafka"]["topic"]
    exchange = config["exchange"]
    backoff = 1

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**23,
            ) as ws:
                print(f"Connected to {url}")
                backoff = 1

                async for raw in ws:
                    tickers = json.loads(raw)
                    stats.counts["received"] += len(tickers)

                    for ticker in tickers:
                        accepted = ticker_filter.accept(ticker)
                        if accepted is None:
                            stats.counts["filtered"] += 1
                            continue

                        base, quote = accepted
                        event = build_event(ticker, base, quote, exchange)
                        publish(producer, topic, base.lower(), json.dumps(event), stats)
                        stats.counts["published"] += 1
                        stats.coins.add(base)

                    producer.poll(0)  # run delivery callbacks
                    stats.maybe_print()

            # A clean close (e.g. Binance's 24-hour limit) ends the loop without an error
            print("Connection closed by the server. Reconnecting.")

        except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as exc:
            print(f"WebSocket problem: {type(exc).__name__}: {exc}. Reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


def main():
    config = load_config()

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

    ticker_filter = TickerFilter(config)
    stats = Stats()

    rest_url = config.get("rest_ticker_url")
    if rest_url:
        try:
            response = requests.get(rest_url, timeout=15)
            response.raise_for_status()
            ticker_filter.seed_main_pairs(response.json())
            print(f"Picked the most liquid pair for {len(ticker_filter.main_pair)} coins")
        except requests.RequestException as exc:
            print(f"Could not load 24h tickers ({exc}); picking pairs as updates arrive")

    print(f"Streaming to Kafka topic '{config['kafka']['topic']}'. Press Ctrl+C to stop.")
    try:
        asyncio.run(stream_forever(config, producer, ticker_filter, stats))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        remaining = producer.flush(10)
        print(f"Flushed. Messages not delivered: {remaining}")


if __name__ == "__main__":
    main()
