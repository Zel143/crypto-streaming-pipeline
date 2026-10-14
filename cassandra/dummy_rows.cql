-- Hand-inserted test rows so Flask can be tested before Spark is running.
-- Remove them afterwards: TRUNCATE pipeline.prices; TRUNCATE pipeline.market_stats;
USE pipeline;

INSERT INTO prices (symbol, captured_at, bid_price, bid_qty, ask_price, ask_qty) VALUES ('BTCUSDT', '2026-10-11 08:00:00.000+0000', 112000.10, 1.25, 112000.20, 0.80);
INSERT INTO prices (symbol, captured_at, bid_price, bid_qty, ask_price, ask_qty) VALUES ('BTCUSDT', '2026-10-11 08:00:10.000+0000', 112010.50, 0.90, 112010.60, 1.10);
INSERT INTO prices (symbol, captured_at, bid_price, bid_qty, ask_price, ask_qty) VALUES ('ETHUSDT', '2026-10-11 08:00:00.000+0000', 4100.10, 12.5, 4100.15, 9.2);
INSERT INTO prices (symbol, captured_at, bid_price, bid_qty, ask_price, ask_qty) VALUES ('ETHUSDT', '2026-10-11 08:00:10.000+0000', 4101.00, 10.0, 4101.05, 8.4);

INSERT INTO market_stats (symbol, captured_at, last_price, price_change_pct, high_price, low_price, volume, quote_volume, trade_count) VALUES ('BTCUSDT', '2026-10-11 08:00:00.000+0000', 112000.15, 1.35, 113500.00, 110200.00, 18250.4, 2041000000.0, 1520344);
INSERT INTO market_stats (symbol, captured_at, last_price, price_change_pct, high_price, low_price, volume, quote_volume, trade_count) VALUES ('BTCUSDT', '2026-10-11 08:00:10.000+0000', 112010.55, 1.36, 113500.00, 110200.00, 18251.1, 2041100000.0, 1520410);
INSERT INTO market_stats (symbol, captured_at, last_price, price_change_pct, high_price, low_price, volume, quote_volume, trade_count) VALUES ('ETHUSDT', '2026-10-11 08:00:00.000+0000', 4100.12, -0.42, 4180.00, 4050.00, 210500.3, 862000000.0, 980120);
INSERT INTO market_stats (symbol, captured_at, last_price, price_change_pct, high_price, low_price, volume, quote_volume, trade_count) VALUES ('ETHUSDT', '2026-10-11 08:00:10.000+0000', 4101.02, -0.40, 4180.00, 4050.00, 210510.0, 862040000.0, 980150);
