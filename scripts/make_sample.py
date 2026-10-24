"""Cut the sample dataset in data/ out of a DuckDB warehouse of the full ESCI data.

  uv run scripts/make_sample.py --warehouse path/to/warehouse.duckdb

The warehouse needs three tables, all with a dataset column ('esci-us'):
queries (query_id, text, slice, sampled), judgments (query_id, doc_id, grade,
source), and docs (doc_id, title, body, brand, color). data/ is checked in,
so you only need this to cut a different sample.

The sample is real Amazon Shopping Queries (ESCI) data, Apache-2.0:
  queries.parquet    24 shopping queries
  judgments.parquet  every human grade for those queries (0-3, 3 = Exact)
  products.parquet   every graded product, plus 300 ungraded distractors

Queries are picked by hash so the cut is stable, and only queries with at
least three Exact products are kept, so every query has something to find.
Bodies are cut to 600 characters to keep the files small.
"""

import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--warehouse", type=Path, required=True)
    ap.add_argument("--queries", type=int, default=24)
    ap.add_argument("--distractors", type=int, default=300)
    args = ap.parse_args()

    out = ROOT / "data"
    out.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{args.warehouse}' AS lab (READ_ONLY)")
    con.execute(f"""
        CREATE TEMP TABLE picked AS
        SELECT q.query_id, q.text, q.slice
        FROM lab.queries q
        JOIN lab.judgments j ON j.dataset = q.dataset AND j.query_id = q.query_id AND j.source = 'human'
        WHERE q.dataset = 'esci-us' AND q.sampled
        GROUP BY q.query_id, q.text, q.slice
        HAVING count(*) FILTER (WHERE j.grade = 3) >= 3
        ORDER BY hash(q.query_id)
        LIMIT {args.queries}
    """)
    con.execute("""
        CREATE TEMP TABLE graded AS
        SELECT j.query_id, j.doc_id, j.grade
        FROM lab.judgments j JOIN picked USING (query_id)
        WHERE j.dataset = 'esci-us' AND j.source = 'human'
    """)
    con.execute(f"""
        CREATE TEMP TABLE products AS
        SELECT doc_id AS id, title, left(body, 600) AS body, brand, color
        FROM lab.docs
        WHERE dataset = 'esci-us'
          AND (doc_id IN (SELECT doc_id FROM graded)
               OR doc_id IN (SELECT doc_id FROM lab.docs WHERE dataset = 'esci-us'
                             AND doc_id NOT IN (SELECT doc_id FROM graded)
                             ORDER BY hash(doc_id) LIMIT {args.distractors}))
        ORDER BY id
    """)
    for table, sql in {
        "queries": "SELECT * FROM picked ORDER BY query_id",
        "judgments": "SELECT * FROM graded ORDER BY query_id, doc_id",
        "products": "SELECT * FROM products",
    }.items():
        path = out / f"{table}.parquet"
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
        (n,) = con.execute(f"SELECT count(*) FROM '{path}'").fetchall()[0]
        print(f"{path.relative_to(ROOT)}: {n:,} rows, {path.stat().st_size / 1024:,.0f} KB")
    con.close()


if __name__ == "__main__":
    main()
