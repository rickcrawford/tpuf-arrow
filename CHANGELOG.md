# Changelog

## 0.1.0 (unreleased)

The first release.

### What's in it

- `connect()` reads settings from arguments, the environment, or a dotenv file you name.
- `Client.query()` runs one ranked query and returns an Arrow table, optionally reranked by a hosted model.
- `Client.multi_query()` runs several queries against one snapshot, fused with reciprocal rank fusion or reranked together, for hybrid search.
- `Client.scan()` reads a whole namespace, or the rows a filter matches, in id order, and `copy_to()` writes the rows into a DuckDB table. `count()` counts on the server.
- `Client.upsert()` writes any Arrow stream, such as a DuckDB relation. Requests are sized by bytes to earn the batch discount, and new attributes are typed from Arrow.
- `register()` adds `tpuf_search`, `tpuf_bm25`, and `tpuf_ann` to a DuckDB connection, to search once per row from SQL.
- Every request is logged with billed bytes, timings, and cache details, and `save_log()` writes the log to DuckDB.
- Library logging on the `tpuf_arrow` logger, and `enable_logging()` for scripts.
- A sample dataset (843 products, 24 queries, 543 human grades from the Shopping Queries dataset) and five examples.

### Rough edges

**Reading**

- No pushdown from SQL. A `WHERE` on a scan runs in DuckDB after every page arrives, so pass `filters=` to `scan()` ([guide](docs/guide.md#reading-a-whole-namespace)).
- Scans are serial and aren't snapshots.
- A scan's reader can be read once. Use `copy_to()` or `.read_all()` to read it again.
- Ranked queries stop at 10,000 hits, turbopuffer's `top_k` limit.
- `datetime` attributes come back as ISO 8601 text. Cast with `::TIMESTAMPTZ`.
- `query()` won't include attributes named `rank` or `score`.
- The SQL functions return ids as `VARCHAR`, even in a namespace with integer ids.

**Writing**

- Float lists need a type in `schema=`, except a column named `vector` ([guide](docs/guide.md#vectors)).
- NaN and infinity become null, nanosecond timestamps are truncated to microseconds, and non-whole decimals become floats.
- BLOB, INTERVAL, STRUCT, and MAP columns raise `SchemaError`.
- A failed upsert leaves the successful requests' rows written. Re-run it; upserts are keyed on id.
- Writes to a namespace with an embedded attribute go out 256 rows at a time.

**SQL, costs, and logging**

- A search per row waits on turbopuffer once per batch of up to 2,048 rows, so turbopuffer's latency and `max_concurrency` set the pace.
- The SQL functions run single queries. Hybrid search and reranking are Python only.
- Every query is billed for at least 1.28 GB, so on a small namespace each search costs about the same.
- The request log grows in memory until `save_log()`.
- DEBUG logging records search text, and `TURBOPUFFER_LOG=debug` records full request bodies ([guide](docs/guide.md#logging)).

**Dependencies**

- numpy is required because DuckDB 2.0's Arrow UDFs import it.
- Python 3.12 or later, DuckDB 2.x, turbopuffer SDK 2.14 or later.
