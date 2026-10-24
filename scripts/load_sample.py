"""Load the sample products into a turbopuffer namespace for the examples.

  uv run scripts/load_sample.py [--namespace tpuf-arrow-sample]
  uv run scripts/load_sample.py --delete

title and body get BM25. A short `text` field (title, brand, color, and the
start of the body) is embedded by turbopuffer with voyage-4-lite, about 85k
tokens for the 843 products. Re-running is safe: writes are upserts on id.
"""

import argparse
from pathlib import Path

import duckdb

import tpuf_arrow as ta

ROOT = Path(__file__).resolve().parents[1]
FTS = {"language": "english", "stemming": True}
SCHEMA = {
    "title": {"type": "string", "full_text_search": FTS},
    "body": {"type": "string", "full_text_search": FTS},
    "text": {"type": "string", "embed": "voyage/voyage-4-lite", "filterable": False},
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", default="tpuf-arrow-sample")
    ap.add_argument("--delete", action="store_true", help="delete the namespace instead")
    args = ap.parse_args()

    env = ROOT / ".env"
    tp = ta.connect(env_file=env if env.is_file() else None)
    if args.delete:
        print("deleted" if tp.delete_namespace(args.namespace) else "no such namespace", args.namespace)
        return
    con = duckdb.connect()
    rel = con.sql(f"""
        SELECT id, title, body, brand, color,
               concat_ws('. ', title, brand, color, left(body, 300)) AS text
        FROM '{ROOT / "data" / "products.parquet"}'
    """)
    stats = tp.upsert(args.namespace, rel, schema=SCHEMA, distance_metric="cosine_distance")
    con.close()
    print(
        f"{stats.rows:,} rows in {stats.requests} requests, {stats.seconds:.1f}s, "
        f"{stats.billable_bytes:,} billable bytes -> {args.namespace}"
    )


if __name__ == "__main__":
    main()
