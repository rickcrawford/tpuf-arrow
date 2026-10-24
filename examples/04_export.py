"""Export a namespace, or the part of it a filter matches, to a Parquet file.

The filter runs in turbopuffer, so only matching rows are fetched. The scan
pages through the namespace in id order, one billed query per 10,000 rows.

  uv run examples/04_export.py --out garmin.parquet --brand Garmin
  uv run examples/04_export.py --out everything.parquet
"""

import duckdb

from _common import client, parser

ap = parser(__doc__)
ap.add_argument("--out", required=True, help="Parquet file to write")
ap.add_argument("--brand", help="export only this brand")
args = ap.parse_args()

with client() as tp, duckdb.connect() as con:
    rows = tp.scan(
        args.namespace,
        filters=("brand", "Eq", args.brand) if args.brand else None,
        columns=["title", "brand", "color"],
    )
    con.register("rows", rows)
    con.execute("COPY rows TO ? (FORMAT parquet)", [args.out])
    (n,) = con.execute("SELECT count(*) FROM read_parquet(?)", [args.out]).fetchall()[0]
    pages = tp.log.num_rows
    print(f"wrote {n:,} rows to {args.out} in {pages} page(s)")
