# Sample data

A slice of Amazon's [Shopping Queries dataset (ESCI)](https://github.com/amazon-science/esci-data), which is Apache-2.0. `scripts/make_sample.py` cut it from a local DuckDB copy of the full dataset, and re-running it on the same copy gives the same files.

| File | Rows | Columns |
| --- | --- | --- |
| `products.parquet` | 843 | `id`, `title`, `body` (first 600 characters), `brand`, `color` |
| `queries.parquet` | 24 | `query_id`, `text`, `slice` (length and whether it names a brand) |
| `judgments.parquet` | 543 | `query_id`, `doc_id`, `grade` |

Grades are the human ESCI labels: 3 Exact (242 rows), 2 Substitute (197), 1 Complement (38), 0 Irrelevant (66).

How it was cut: 24 test-split queries, picked by hash from a 500-query evaluation sample, keeping only queries with at least three Exact products. Every product graded for those queries is in `products.parquet`, plus 300 ungraded products picked by hash so the retrievers have something to get wrong. 21 products have no brand and 260 have no color, as in the source.

The queries are real and messy: "dish netwok wireless speakers", "cuerdas cello 4/4", "big boys size 6 ascics sneakers".
