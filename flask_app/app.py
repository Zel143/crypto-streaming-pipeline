"""Flask page that reads the newest rows from Cassandra (keyspace `pipeline`).

Run:  python flask_app/app.py      then open http://127.0.0.1:5000
"""
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "pipeline"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
ROWS_PER_SYMBOL = 20          # newest N rows per symbol, per table

PRICE_COLS = "symbol, captured_at, bid_price, bid_qty, ask_price, ask_qty"
STATS_COLS = ("symbol, captured_at, last_price, price_change_pct, high_price, "
              "low_price, volume, quote_volume, trade_count")

app = Flask(__name__)
_session = None
_stmts = {}


def get_session():
    """Connect once and reuse the connection (and prepared statements)."""
    global _session
    if _session is None:
        _session = Cluster(CASSANDRA_HOSTS).connect(KEYSPACE)
        _stmts["prices"] = _session.prepare(
            f"SELECT {PRICE_COLS} FROM prices WHERE symbol = ? LIMIT {ROWS_PER_SYMBOL}")
        _stmts["market_stats"] = _session.prepare(
            f"SELECT {STATS_COLS} FROM market_stats WHERE symbol = ? LIMIT {ROWS_PER_SYMBOL}")
    return _session


def newest_rows(table):
    """Newest rows of every symbol, merged into one list (newest first)."""
    session = get_session()
    rows = []
    for sym in SYMBOLS:
        for r in session.execute(_stmts[table], (sym,)):
            d = r._asdict()
            # driver returns a naive datetime in UTC; show it as ISO 8601 with 'Z'
            d["captured_at"] = d["captured_at"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            rows.append(d)
    rows.sort(key=lambda d: d["captured_at"], reverse=True)
    return rows


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    try:
        return jsonify(prices=newest_rows("prices"), stats=newest_rows("market_stats"))
    except Exception as e:  # Cassandra down, keyspace missing, ...
        return jsonify(error=str(e)), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
