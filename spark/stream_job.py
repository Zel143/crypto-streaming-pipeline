"""Spark Structured Streaming job: Kafka topics pipeline.prices + pipeline.market-stats -> Cassandra

Reads both topics, parses each message's JSON value into typed columns, and writes every
micro-batch to Cassandra with foreachBatch + cassandra-driver (no Spark Cassandra Connector
exists for Spark 4.x). SINK=console prints the batches instead, so no Cassandra is needed.
Run with spark/run.sh, not plain python.
"""
import os
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoints")
SINK = os.getenv("SINK", "cassandra")
CASSANDRA_HOSTS = [h.strip() for h in os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",") if h.strip()]
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "pipeline")

# Must match the JSON schemas in contract.md. Field names double as Cassandra column names.
PRICES_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("bid_price", DoubleType()),
    StructField("bid_qty", DoubleType()),
    StructField("ask_price", DoubleType()),
    StructField("ask_qty", DoubleType()),
    StructField("captured_at", TimestampType()),
])

MARKET_STATS_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("last_price", DoubleType()),
    StructField("price_change_pct", DoubleType()),
    StructField("high_price", DoubleType()),
    StructField("low_price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("quote_volume", DoubleType()),
    StructField("trade_count", LongType()),
    StructField("captured_at", TimestampType()),
])

# Kafka topic -> (Cassandra table, message schema). Table names must match contract.md.
STREAMS = {
    "pipeline.prices": ("prices", PRICES_SCHEMA),
    "pipeline.market-stats": ("market_stats", MARKET_STATS_SCHEMA),
}

_session = None


def read_topic(spark, topic, schema):
    """Subscribe to one Kafka topic and turn each message's JSON value into typed columns."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    return (
        raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .where(col("symbol").isNotNull() & col("captured_at").isNotNull())
    )


def cassandra_session():
    """Connect on the first batch and reuse the same session for every batch after it."""
    global _session
    if _session is None:
        _session = Cluster(CASSANDRA_HOSTS).connect(KEYSPACE)
    return _session


def to_cassandra_value(value):
    """Spark gives timestamps to Python as naive local-time datetimes; make them explicit UTC."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return value


def cassandra_writer(table, columns):
    """Build the foreachBatch function that inserts one micro-batch into table."""
    insert_cql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
    prepared = None

    def write_batch(batch_df, batch_id):
        nonlocal prepared
        rows = batch_df.select(*columns).collect()
        if not rows:
            return
        session = cassandra_session()
        if prepared is None:
            prepared = session.prepare(insert_cql)
        params = [tuple(to_cassandra_value(v) for v in row) for row in rows]
        execute_concurrent_with_args(session, prepared, params, raise_on_first_error=True)
        print(f"[spark] batch {batch_id}: wrote {len(rows)} rows to {KEYSPACE}.{table}", flush=True)

    return write_batch


def start_query(df, table):
    """Start one streaming query; its checkpoint lets a restart resume where it stopped."""
    writer = (
        df.writeStream.queryName(table)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, SINK, table))
        .outputMode("append")
    )
    if SINK == "console":
        writer = writer.format("console").option("truncate", "false")
    else:
        writer = writer.foreachBatch(cassandra_writer(table, df.columns))
    return writer.start()


def main():
    spark = SparkSession.builder.appName("crypto-streaming-pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for topic, (table, schema) in STREAMS.items():
        start_query(read_topic(spark, topic, schema), table)
    print(f"[spark] {len(STREAMS)} streaming queries started (sink={SINK}); Spark UI at http://localhost:4040", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
