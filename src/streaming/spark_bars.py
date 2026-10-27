"""Spark Structured Streaming: Kafka price events -> 1-minute OHLC bars in PostgreSQL.

Runs next to the Python consumer and writes to its own table, price_bars_1m_spark,
so the two implementations can be compared before either replaces the other.
"""

import os
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sqlalchemy import text

from src.database.connection import get_engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "spark_bars"
CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", str(DEFAULT_CHECKPOINT_DIR))

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
TOPIC = "crypto.prices.raw"
WATERMARK = "2 minutes"
TRIGGER = "30 seconds"
TIME_COLUMNS = ("bar_start", "first_event_time", "last_event_time")

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("base_asset", T.StringType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("event_time", T.StringType()),
    ]
)

CREATE_TABLE = text(
    """
    CREATE TABLE IF NOT EXISTS price_bars_1m_spark (
        base_asset        VARCHAR(30) NOT NULL,
        bar_start         TIMESTAMPTZ NOT NULL,
        open              DOUBLE PRECISION NOT NULL,
        high              DOUBLE PRECISION NOT NULL,
        low               DOUBLE PRECISION NOT NULL,
        close             DOUBLE PRECISION NOT NULL,
        first_event_time  TIMESTAMPTZ NOT NULL,
        last_event_time   TIMESTAMPTZ NOT NULL,
        ticks             INTEGER NOT NULL,
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (base_asset, bar_start)
    )
    """
)

# Spark keeps each bar's full state, so every row it emits is the complete bar so far:
# replacing the stored row is enough (no merge rules as in the Python consumer).
UPSERT = text(
    """
    INSERT INTO price_bars_1m_spark (
        base_asset, bar_start, open, high, low, close, first_event_time, last_event_time, ticks
    )
    VALUES (
        :base_asset, :bar_start, :open, :high, :low, :close, :first_event_time, :last_event_time, :ticks
    )
    ON CONFLICT (base_asset, bar_start) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        first_event_time = EXCLUDED.first_event_time,
        last_event_time = EXCLUDED.last_event_time,
        ticks = EXCLUDED.ticks,
        updated_at = CURRENT_TIMESTAMP
    """
)


def parse_events(raw):
    """Kafka rows (binary key/value) -> one typed row per valid price event."""
    events = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select(
        "e.event_id",
        F.upper("e.base_asset").alias("base_asset"),
        "e.price",
        F.col("e.event_time").cast("timestamp").alias("event_time"),
    )
    # The Python consumer sends bad events to the DLQ; here they are simply skipped.
    return events.where(
        F.col("event_id").isNotNull()
        & F.col("base_asset").isNotNull()
        & F.col("event_time").isNotNull()
        & (F.col("price") > 0)
    )


def build_bars(events):
    """Events -> 1-minute OHLC bars, de-duplicated on event_id."""
    return (
        events.withWatermark("event_time", WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_time", "1 minute").alias("bar"), "base_asset")
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
            F.count("*").cast("int").alias("ticks"),
        )
        .select(
            "base_asset",
            F.col("bar.start").alias("bar_start"),
            "open",
            "high",
            "low",
            "close",
            "first_event_time",
            "last_event_time",
            "ticks",
        )
    )


def to_utc(timestamp):
    # toPandas() returns times without a timezone (in the session timezone, UTC).
    # Mark them as UTC so PostgreSQL doesn't have to assume its own default timezone.
    timestamp = pd.Timestamp(timestamp)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.to_pydatetime()


def make_writer(engine):
    def write_batch(batch_df, batch_id):
        rows = batch_df.toPandas()
        if rows.empty:
            return
        records = [
            {**row, **{column: to_utc(row[column]) for column in TIME_COLUMNS}}
            for row in rows.to_dict("records")
        ]
        with engine.begin() as connection:
            connection.execute(UPSERT, records)
        print(f"[batch {batch_id}] upserted {len(records)} bars")

    return write_batch


def main():
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(CREATE_TABLE)

    spark = (
        SparkSession.builder.appName("riskcoin-spark-bars")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 200_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        build_bars(parse_events(raw))
        .writeStream.outputMode("update")
        .foreachBatch(make_writer(engine))
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
