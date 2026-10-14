# Shared Contract

Change nothing here without telling the other member first (and note it in the change log below).

Status: LOCKED 2026-10-11. Source, topics, symbols, JSON schemas, timestamp format and Cassandra tables all agreed by both members.

## Data source
- Source: Binance public REST API (`https://api.binance.com`)
- Poll interval: 5-10 s
- API key needed: No
- Symbols (both topics): `BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`

## Kafka
- Bootstrap server: `localhost:9092` (use `127.0.0.1:9092` if WSL2 localhost gives trouble)
- Topics:

| Topic | Endpoint | Producer | Owner | Partitions |
|---|---|---|---|---|
| `pipeline.prices` | `GET /api/v3/ticker/bookTicker` | Producer 1 | Ranzel | 1 |
| `pipeline.market-stats` | `GET /api/v3/ticker/24hr` | Producer 2 | Marcus | 1 |

Topic naming rule: do not mix `.` and `_` in topic names (Kafka warns they can collide in metrics).

## JSON schema per topic

One JSON message per symbol per poll. Message key = `symbol`.

### Topic 1: `pipeline.prices` (Producer 1, bookTicker, every 5 s)
| Field | Type | Example | Notes |
|---|---|---|---|
| `symbol` | string | `"BTCUSDT"` | |
| `bid_price` | float | `82908.0` | best bid |
| `bid_qty` | float | `2.55272` | |
| `ask_price` | float | `82908.01` | best ask |
| `ask_qty` | float | `8.76905` | |
| `captured_at` | string | `"2026-10-10T11:56:56.918Z"` | see Timestamps |

### Topic 2: `pipeline.market-stats` (Producer 2, 24hr, every 10 s)
| Field | Type | Example | Notes |
|---|---|---|---|
| `symbol` | string | `"BTCUSDT"` | |
| `last_price` | float | `83053.35` | |
| `price_change_pct` | float | `0.27` | rolling 24 h, percent |
| `high_price` | float | `83234.0` | rolling 24 h |
| `low_price` | float | `82778.0` | rolling 24 h |
| `volume` | float | `6538.77` | base asset, 24 h |
| `quote_volume` | float | `542531102.0` | USDT, 24 h |
| `trade_count` | int | `601677` | 24 h |
| `captured_at` | string | `"2026-10-11T08:11:35.663Z"` | see Timestamps |

Timestamps: `captured_at` is ISO 8601 UTC with milliseconds and a trailing `Z`, both topics.

## Cassandra
- Keyspace: `pipeline` (SimpleStrategy, replication_factor 1). CQL: `cassandra/schema.cql`
- Column names = JSON field names. Types: `text` for symbol, `double` for floats, `bigint` for `trade_count`, `timestamp` for `captured_at`.
- Tables:

| Table | Fed by topic | Partition key | Clustering key |
|---|---|---|---|
| `prices` | `pipeline.prices` | `symbol` | `captured_at DESC` |
| `market_stats` | `pipeline.market-stats` | `symbol` | `captured_at DESC` |

Table name uses `_` because `-` is not valid in an unquoted CQL name.

## Spark -> Cassandra write method
No Spark Cassandra Connector exists for Spark 4.x yet (latest on Maven Central is 3.5.1, built for Spark 3.5).
Plan: `foreachBatch` + the Python `cassandra-driver`.

## Change log
| Date | Who | Change |
|---|---|---|
| 2026-09-30 | Ranzel | Created draft |
| 2026-09-30 | Ranzel | Placeholder `pipeline.market_stats` -> `pipeline.market-stats` (Kafka naming warning). Still TBD until source is chosen. Marcus told. |
| 2026-10-09 | Ranzel | Source agreed: Binance public API. Topic names final: `pipeline.prices` (bookTicker), `pipeline.market-stats` (24hr). Marcus confirmed. |
| 2026-10-11 | Ranzel | Locked: symbols, JSON schemas, timestamp format, keyspace and tables. Marcus agreed. |
