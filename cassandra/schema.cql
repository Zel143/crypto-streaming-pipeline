-- Run with: cqlsh -f cassandra/schema.cql
-- Layout: keyspace `pipeline`, one table per topic,
-- column names = JSON field names, newest row first inside each symbol.

CREATE KEYSPACE IF NOT EXISTS pipeline
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pipeline;

-- Fed by topic pipeline.prices (GET /api/v3/ticker/bookTicker)
CREATE TABLE IF NOT EXISTS prices (
  symbol      text,
  captured_at timestamp,
  bid_price   double,
  bid_qty     double,
  ask_price   double,
  ask_qty     double,
  PRIMARY KEY ((symbol), captured_at)
) WITH CLUSTERING ORDER BY (captured_at DESC);

-- Fed by topic pipeline.market-stats (GET /api/v3/ticker/24hr)
-- Table name uses an underscore because a hyphen is not valid in an unquoted CQL name.
CREATE TABLE IF NOT EXISTS market_stats (
  symbol           text,
  captured_at      timestamp,
  last_price       double,
  price_change_pct double,
  high_price       double,
  low_price        double,
  volume           double,
  quote_volume     double,
  trade_count      bigint,
  PRIMARY KEY ((symbol), captured_at)
) WITH CLUSTERING ORDER BY (captured_at DESC);
