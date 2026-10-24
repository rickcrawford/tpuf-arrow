"""Load a Parquet, CSV, or JSON file into a new turbopuffer namespace from SQL.

Any DuckDB relation can be written: here, a SELECT over a Parquet file that
renames the id column and builds the text turbopuffer embeds. title and body
get BM25, and `text` is embedded by turbopuffer, so no embedding API key is
needed.

  uv run examples/03_load_your_data.py --target my-products
  uv run examples/03_load_your_data.py --target my-products --file my.csv --id sku  # edit the SELECT first
"""

from pathlib import Path

import duckdb

import tpuf_arrow as ta

from _common import DATA, client, parser

ap = parser(__doc__)
ap.add_argument("--target", required=True, help="namespace to create or add to")
ap.add_argument("--file", default=str(DATA / "products.parquet"), help="Parquet, CSV, or JSON file")
ap.add_argument("--id", default="id", help="the file's id column")
args = ap.parse_args()

english = {"language": "english", "stemming": True}
schema = {
    "title": {"type": "string", "full_text_search": english},
    "body": {"type": "string", "full_text_search": english},
    "text": {"type": "string", "embed": "voyage/voyage-4-lite", "filterable": False},
}

with client() as tp, duckdb.connect() as con:
    ta.enable_logging()  # one INFO line when the upsert finishes
    read = {".parquet": con.read_parquet, ".csv": con.read_csv, ".json": con.read_json}
    src = read[Path(args.file).suffix.lower()](args.file)
    # Edit this SELECT for your columns. It picks the id, the attributes to
    # store, and the text turbopuffer embeds.
    rel = con.sql(f"""
        SELECT "{args.id}"::VARCHAR AS id, title, body, brand, color,
               concat_ws('. ', title, brand, color, left(body, 300)) AS text
        FROM src
    """)
    stats = tp.upsert(args.target, rel, schema=schema, distance_metric="cosine_distance")
    print(f"wrote {stats.rows:,} rows in {stats.requests} requests ({stats.seconds:.1f}s)")
    print(f"{args.target} now has {tp.count(args.target):,} rows")
