# crypto-streaming-pipeline

A small real-time data pipeline: live crypto market data from the Binance public API flows through Kafka,
Spark Structured Streaming and Cassandra, and shows up on a Flask web page.

```
Binance API ──> Producer 1 ──> pipeline.prices ───────┐
                                                      ├──> Spark Structured Streaming ──> Cassandra ──> Flask page
Binance API ──> Producer 2 ──> pipeline.market-stats ─┘
```

| Part | File | What it does |
|---|---|---|
| Producer 1 | `producers/producer1.py` | Best bid/ask (`/api/v3/ticker/bookTicker`) every 5 s -> topic `pipeline.prices` |
| Producer 2 | `producers/producer2.py` | Rolling 24 h stats (`/api/v3/ticker/24hr`) every 10 s -> topic `pipeline.market-stats` |
| Stream job | `spark/stream_job.py` | Reads both topics, parses the JSON, writes each micro-batch to Cassandra |
| Storage | `cassandra/schema.cql` | Keyspace `pipeline`, tables `prices` and `market_stats` (newest row first per symbol) |
| Web page | `flask_app/app.py` | Reads the newest rows from Cassandra and shows them in the browser |

Symbols: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT. No API key is needed.
Topic names, message fields and table layouts are defined in [`contract.md`](contract.md).

## Stack
Kafka 4.3.1 (KRaft, single node), Spark 4.2.0, Cassandra 5.0, Python 3 with `kafka-python`, `requests`,
`cassandra-driver` and `flask` (see `requirements.txt`). Developed on WSL2 Ubuntu.

Spark 4.x has no Spark Cassandra Connector yet, so the stream job writes with `foreachBatch` and the
Python `cassandra-driver` instead.

## Running it
- Kafka broker and topics: [`kafka/README.md`](kafka/README.md)
- Full run order, settings and troubleshooting: [`spark/README.md`](spark/README.md)
- Screenshots and logs from a full run: [`proof/`](proof/)

## Team
Built as a two-person university course project.

- **Ranzel Jude Virtucio** ([@Zel143](https://github.com/Zel143)): Kafka setup, Producer 1, Spark stream job
- **Marcus Agustin** ([@ohheymarcus](https://github.com/ohheymarcus)): Producer 2, Cassandra schema, Flask page

Each of us reviewed the other's code before it was merged.

## AI use
Claude Code, an AI coding assistant, was used during this project, including help with setup,
debugging, code review and some of the commits.

## License
[MIT](LICENSE)
