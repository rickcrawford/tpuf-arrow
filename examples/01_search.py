"""Search turbopuffer from Python and from SQL, and join the hits to a DuckDB table.

uv run scripts/load_sample.py     # once
uv run examples/01_search.py
"""

import duckdb

import tpuf_arrow as ta

from _common import DATA, client, parser

args = parser(__doc__).parse_args()
ns = args.namespace

with client() as tp, duckdb.connect() as con:
    # One search from Python. The result is an Arrow table, and DuckDB can
    # query a local Arrow table by its variable name.
    hits = tp.query(ns, ("title", "BM25", "uconn hoodie"), top_k=5, include=["title", "brand"])
    con.sql("SELECT rank, round(score, 2) AS score, brand, title FROM hits").show()

    # The same kind of search from SQL, once per row of a table. tpuf_ann has
    # turbopuffer embed each query; tpuf_bm25 runs full-text search.
    ta.register(con, tp)
    con.execute(f"CREATE TABLE queries AS SELECT * FROM '{DATA / 'queries.parquet'}' LIMIT 3")
    con.sql(f"""
        SELECT q.text AS query, h.rank, h.id
        FROM queries q,
             LATERAL (SELECT unnest(tpuf_ann('{ns}', 'text', q.text, 3, NULL), recursive := true)) h
        ORDER BY q.text, h.rank
    """).show()

    # Hits carry ids, so everything else comes from a join, here to the product
    # file. The sample has three black watch bands, so ranks 4 and 5 are just
    # the nearest other black products.
    con.sql(f"""
        SELECT h.rank, p.brand, p.color, left(p.title, 60) AS title
        FROM (SELECT unnest(tpuf_ann('{ns}', 'text', 'apple watch band', 5,
                                      json_array('color', 'Eq', 'Black')), recursive := true)) h
        JOIN '{DATA / "products.parquet"}' p USING (id)
        ORDER BY h.rank
    """).show()
