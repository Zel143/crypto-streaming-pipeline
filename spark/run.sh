#!/usr/bin/env bash
# Launch spark/stream_job.py from the repo root: bash spark/run.sh
# SPARK_HOME defaults to ~/spark; the job's Python is the repo .venv (needed for cassandra-driver in stage 2).
set -euo pipefail

cd "$(dirname "$0")/.."
SPARK_HOME="${SPARK_HOME:-$HOME/spark}"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

exec "$SPARK_HOME/bin/spark-submit" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.sql.session.timeZone=UTC \
  spark/stream_job.py
