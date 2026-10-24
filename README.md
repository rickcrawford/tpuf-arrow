<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/turbopuffer/lockup_dark.svg">
  <img alt="turbopuffer" src="https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/turbopuffer/lockup_light.svg" height="44">
</picture>

# tpuf-arrow

[![ci](https://github.com/rickcrawford/tpuf-arrow/actions/workflows/ci.yml/badge.svg)](https://github.com/rickcrawford/tpuf-arrow/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Query [turbopuffer](https://turbopuffer.com) namespaces from [DuckDB](https://duckdb.org). Results come back as Arrow tables, and a set of SQL functions runs searches from inside a query.

Unofficial. Not affiliated with turbopuffer or the DuckDB Foundation.

![Architecture overview](https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/overview.svg)

## Features

- Ranked queries (BM25, vector, or any `rank_by`) returned as Arrow tables
- Hybrid search with `multi_query`, and hosted rerankers through `rerank_by`
- SQL functions `tpuf_search`, `tpuf_bm25`, and `tpuf_ann`, callable once per row
- Full namespace scans in id order, with filters and columns sent to turbopuffer
- Upserts from any Arrow stream, including DuckDB relations, batched by request size
- A per-request log of billed bytes and timings that can be saved to DuckDB

## Requirements

- Python 3.12+
- DuckDB 2.x
- A turbopuffer API key

## Installation

```sh
pip install tpuf-arrow
```

## Quick start

```python
import duckdb
import tpuf_arrow as ta

tp = ta.connect()            # uses TURBOPUFFER_API_KEY and TURBOPUFFER_REGION
con = duckdb.connect()
ta.register(con, tp)         # adds the SQL functions to this connection

# Python
hits = tp.query("products", ("title", "BM25", "running shoes"), top_k=5, include=["title"])
con.sql("SELECT rank, title FROM hits").show()

# SQL
con.sql("SELECT unnest(tpuf_bm25('products', 'title', 'running shoes', 5, NULL), recursive := true)").show()

# Scan rows into a DuckDB table
tp.copy_to(con, "products", "asics", filters=("brand", "Eq", "ASICS"))
```

## Configuration

| Setting | Environment variable | Default |
| --- | --- | --- |
| API key | `TURBOPUFFER_API_KEY` | required |
| Region | `TURBOPUFFER_REGION` | `aws-us-east-1` |

Both can also be passed to `connect()`, or read from a file with `connect(env_file=".env")`.

## Documentation

- [Guide](docs/guide.md)
- [API reference](docs/reference.md)
- [Architecture](docs/architecture.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Changelog](CHANGELOG.md)

## Examples

The examples run against a small sample dataset in [`data/`](data/README.md). Load it into your account first:

```sh
git clone https://github.com/rickcrawford/tpuf-arrow && cd tpuf-arrow
echo 'TURBOPUFFER_API_KEY=tpuf_...' > .env
uv run scripts/load_sample.py
uv run examples/01_search.py
```

| Example | |
| --- | --- |
| [`01_search.py`](examples/01_search.py) | Search from Python and SQL, with a filter and a join |
| [`02_evaluate.py`](examples/02_evaluate.py) | nDCG@10 for BM25 and vector search against relevance grades |
| [`03_load_your_data.py`](examples/03_load_your_data.py) | Load a Parquet, CSV, or JSON file into a namespace |
| [`04_export.py`](examples/04_export.py) | Export a namespace to Parquet |
| [`05_cost_report.py`](examples/05_cost_report.py) | Cost and latency from the request log |

## Development

```sh
uv sync
uv run pytest -m "not live"
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for live tests, linting, and releases.

## License

MIT. See [LICENSE](LICENSE).

The turbopuffer logo belongs to turbopuffer. DuckDB is a trademark of the DuckDB Foundation. The sample data comes from Amazon's [Shopping Queries dataset](https://github.com/amazon-science/esci-data) (Apache-2.0).
