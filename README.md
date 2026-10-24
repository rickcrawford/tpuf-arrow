<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/turbopuffer/lockup_dark.svg">
  <img alt="turbopuffer" src="https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/turbopuffer/lockup_light.svg" height="44">
</picture>

# turbopuffer + DuckDB

tpuf-arrow lets you search [turbopuffer](https://turbopuffer.com) from [DuckDB](https://duckdb.org) SQL. You can run a search for every row of a table, join the hits to your own data, score them against relevance grades, and see what each request cost, without leaving the SQL session.

It's unofficial and not affiliated with turbopuffer or the DuckDB Foundation.

![DuckDB and tpuf-arrow run in your Python process and trade Arrow tables; tpuf-arrow talks to turbopuffer over HTTPS](https://raw.githubusercontent.com/rickcrawford/tpuf-arrow/main/docs/images/overview.svg)

## Why you'd want it

turbopuffer does the retrieval: BM25 and vector search over data in object storage, and it can embed your text for you. It doesn't do joins, and you wouldn't compute nDCG in it. DuckDB is good at both and runs in-process. Without something between them you end up writing the same loop every time: run a query, collect the ids, build a DataFrame, join it. This package turns that loop into SQL.

```sql
-- search every query in the table, join the hits to human grades
SELECT q.query_id, h.rank, j.grade
FROM queries q,
     LATERAL (SELECT unnest(tpuf_ann('products', 'text', q.text, 10, NULL), recursive := true)) h
LEFT JOIN judgments j ON j.query_id = q.query_id AND j.doc_id = h.id;
```

[`examples/02_evaluate.py`](examples/02_evaluate.py) does this for the bundled sample (24 real shopping queries, 843 products) and prints nDCG@10 of about 0.76 for vector search and 0.67 for BM25, with bootstrap intervals. Each request is also logged with its billed bytes and server time, and at list price those 48 searches cost about $0.00006 ([`05_cost_report.py`](examples/05_cost_report.py) shows how to get that number).

## Install

```sh
pip install tpuf-arrow        # or: uv add tpuf-arrow
```

You need Python 3.12 or later and a turbopuffer API key. It installs DuckDB 2.x, pyarrow, numpy, python-dotenv, and the turbopuffer SDK.

## Try it

Clone the repo and load the sample into your turbopuffer account:

```sh
git clone https://github.com/rickcrawford/tpuf-arrow && cd tpuf-arrow
echo 'TURBOPUFFER_API_KEY=tpuf_...' > .env
uv run scripts/load_sample.py      # 843 products into tpuf-arrow-sample, about 85k embedding tokens
```

Then, in `uv run python`:

```python
import duckdb
import tpuf_arrow as ta

tp = ta.connect(env_file=".env")
con = duckdb.connect()
ta.register(con, tp)               # adds tpuf_search, tpuf_bm25, and tpuf_ann to con
ns = "tpuf-arrow-sample"

# From Python. The result is an Arrow table, and DuckDB can query it by name.
hits = tp.query(ns, ("title", "BM25", "running shoes"), top_k=5, include=["title"])
con.sql("SELECT rank, title FROM hits").show()

# From SQL
con.sql(f"SELECT unnest(tpuf_ann('{ns}', 'text', 'running shoes', 5, NULL), recursive := true)").show()

# Copy the rows a filter matches into a DuckDB table. The filter runs in turbopuffer.
tp.copy_to(con, ns, "asics", filters=("brand", "Eq", "ASICS"))
```

Or run the examples: `uv run examples/01_search.py`.

## Docs

| | |
| --- | --- |
| [Guide](docs/guide.md) | Searching, scanning, writing, costs, and logging, with examples |
| [Reference](docs/reference.md) | Every function, parameter, SQL function, and error |
| [Architecture](docs/architecture.md) | How it works, what runs in turbopuffer versus DuckDB, and what a native extension would change |
| [Troubleshooting](docs/troubleshooting.md) | Error messages and what to do about them |
| [Changelog](CHANGELOG.md) | Release notes, including the rough edges in this version |

## Examples

| | |
| --- | --- |
| [`01_search.py`](examples/01_search.py) | Search from Python and from SQL, with a filter, and join the hits to a table |
| [`02_evaluate.py`](examples/02_evaluate.py) | Compare BM25 and vector search with nDCG@10 and a bootstrap interval |
| [`03_load_your_data.py`](examples/03_load_your_data.py) | Load a Parquet, CSV, or JSON file into a new namespace with BM25 and embeddings |
| [`04_export.py`](examples/04_export.py) | Export a namespace, or the rows a filter matches, to Parquet |
| [`05_cost_report.py`](examples/05_cost_report.py) | Request counts, latency percentiles, and list-price cost from the request log |

The sample in [`data/`](data/README.md) is a slice of Amazon's [Shopping Queries dataset](https://github.com/amazon-science/esci-data) (Apache-2.0), with human relevance grades.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The tests run offline against a fake turbopuffer, and against the real service when `TURBOPUFFER_API_KEY` is set.

## License

MIT, see [LICENSE](LICENSE). The turbopuffer logo belongs to turbopuffer and is shown here to refer to their product. DuckDB is a trademark of the DuckDB Foundation.
