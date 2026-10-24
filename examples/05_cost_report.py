"""What did that cost? Run some searches, save the request log, and query it.

Every request the client makes is logged with billed bytes, server time, and
cache temperature. save_log() appends the log to a DuckDB table, so a
warehouse file can keep the history across runs.

  uv run examples/05_cost_report.py
  uv run examples/05_cost_report.py --db lab.duckdb    # keep the history in a file
"""

import duckdb

import tpuf_arrow as ta

from _common import DATA, client, parser

ap = parser(__doc__)
ap.add_argument("--db", default=":memory:", help="DuckDB file for the tpuf_requests table")
args = ap.parse_args()
ns = args.namespace

with client() as tp, duckdb.connect(args.db) as con:
    ta.register(con, tp)
    con.sql(f"""
        SELECT count(*) FROM '{DATA / "queries.parquet"}' q,
             LATERAL (SELECT unnest(tpuf_ann('{ns}', 'text', q.text, 10, NULL))) h
    """).fetchall()
    tp.scan(ns, columns=["brand"]).read_all()
    tp.save_log(con, "tpuf_requests")

    # List price as of 2026-10: $1 per PB queried, $0.05 per GB returned.
    con.sql("""
        SELECT kind, count(*) AS requests, sum(rows) AS rows,
               round(median(server_ms)) AS p50_server_ms,
               round(quantile_cont(server_ms, 0.99)) AS p99_server_ms,
               any_value(cache_temperature) AS cache,
               printf('$%.7f', sum(billable_bytes_queried) / 1e15
                              + sum(billable_bytes_returned) / 1e9 * 0.05) AS list_price
        FROM tpuf_requests GROUP BY kind ORDER BY kind
    """).show()
