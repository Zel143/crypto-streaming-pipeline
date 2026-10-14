# Spark job + local run (Ranzel's machine)

`spark/stream_job.py` reads `pipeline.prices` and `pipeline.market-stats`, parses the JSON,
and writes each micro-batch to Cassandra with `foreachBatch` + `cassandra-driver`.
Spark 4.2.0 has no Spark Cassandra Connector, so this is the write path (see contract.md).

| What | Path / value |
|---|---|
| Spark | `~/spark` (4.2.0, runs on Java 25) |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` (fetched by `run.sh`) |
| Cassandra | `$CASSANDRA_HOME` = `~/cassandra-install/apache-cassandra-5.0.9`, set in `~/.bashrc` (needs Java 17, not 25) |
| Python venv | repo `.venv` (`pip install -r requirements.txt`) |
| Checkpoints | `checkpoints/<sink>/<table>/` (gitignored) |

## One-time setup
```bash
cd ~/crypto-streaming-pipeline
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run the pipeline (one terminal tab each, in this order)
```bash
# 1. Kafka broker (see kafka/README.md)
~/kafka-activity/kafka_2.13-4.3.1/bin/kafka-server-start.sh ~/crypto-pipeline-kafka/server.properties

# 2. Cassandra, ready when the log says "Starting listening for CQL clients"
#    (heap caps MAX_HEAP_SIZE=1G / HEAP_NEWSIZE=200M come from ~/.bashrc)
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH \
$CASSANDRA_HOME/bin/cassandra -f
#    first time only: cqlsh -f cassandra/schema.cql

# 3. Spark job, ready when it prints "streaming queries started"
cd ~/crypto-streaming-pipeline && bash spark/run.sh

# 4. Producer 1
cd ~/crypto-streaming-pipeline && . .venv/bin/activate && python producers/producer1.py

# 5. Producer 2
cd ~/crypto-streaming-pipeline && . .venv/bin/activate && python producers/producer2.py

# 6. Flask page: http://127.0.0.1:5000
cd ~/crypto-streaming-pipeline && . .venv/bin/activate && python flask_app/app.py
```
Spark prints one `[spark] batch N: wrote X rows to pipeline.<table>` line per micro-batch.
Spark UI: http://localhost:4040 → Structured Streaming tab (open it from a Windows browser too).
Stop everything with Ctrl+C, producers first.

## Settings (environment variables)
| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | producers, Spark |
| `SYMBOLS` | `BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT` | producers |
| `POLL_SECONDS` | `5` (Producer 1), `10` (Producer 2) | producers |
| `SINK` | `cassandra` (`console` prints batches instead, no Cassandra needed) | Spark |
| `CASSANDRA_HOSTS` | `127.0.0.1` | Spark |
| `CASSANDRA_KEYSPACE` | `pipeline` | Spark |

Table names are in `STREAMS` in `stream_job.py` and must match contract.md.

## cqlsh on this machine
Cassandra 5.0.9's cqlsh supports Python 3.6-3.13, and the system Python is 3.14, so `~/.bashrc`
sets `CQLSH_PYTHON=$HOME/.local/bin/python3.11`. Then plain `cqlsh` works:
```bash
cqlsh -e "SELECT * FROM pipeline.prices WHERE symbol = 'BTCUSDT' LIMIT 5;"
```

## Cassandra tables
Created from `cassandra/schema.cql` (written by Marcus): keyspace `pipeline`, tables `prices` and `market_stats`.
They match `STREAMS`, `PRICES_SCHEMA` and `MARKET_STATS_SCHEMA` in `stream_job.py` (checked with `DESCRIBE` on 2026-10-11).
If a table or column ever changes, change those places in `stream_job.py` too, then stop Spark and `rm -rf checkpoints/`.
`cassandra/dummy_rows.cql` is only for testing Flask without Spark; `TRUNCATE` both tables after using it.

## Troubleshooting
- **Changed a table, a schema, or the sink:** stop Spark and `rm -rf checkpoints/` before restarting, so it does not resume from a checkpoint made for the old setup.
- **Timestamps:** producers send UTC (`...Z`), `run.sh` sets the Spark session time zone to UTC, and the writer converts to UTC before inserting. cqlsh shows `+0000`. Stored values match the producer output exactly (checked 2026-10-10).
- **Memory:** this PC has 6.9 GB RAM and WSL is capped at 5 GB (`.wslconfig`). Kafka + Spark + Cassandra + both producers used about 3.0 GB of 4.8 GB (Cassandra ~1.1 GB with a 768 MB heap, Spark ~0.7 GB, Kafka ~0.5 GB; the 1 GB heap from `~/.bashrc` adds roughly 0.3 GB). Close heavy Windows apps before recording the video.
- **Producer startup warning:** kafka-python 3.0 prints a `DeprecationWarning` about lambda serializers. It is harmless.

## Test log
- 2026-10-10: stage 1 (console) and stage 2 (Cassandra) tested with Producer 1 + Producer 2 running together.
  2 min run: 120 rows in `prices` (5 symbols every 5 s), 60 in `market_stats` (5 symbols every 10 s). Proof in `proof/` (Spark UI screenshots, Spark write log, producer output, topic describe, consumer read).
- 2026-10-11: full run on `main`: both producers, Spark, Marcus's tables and Flask page all live. Proof: `proof/terminal-cqlsh.png`, `proof/flask-page.png`.
