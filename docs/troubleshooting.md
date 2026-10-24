# Troubleshooting

Errors from inside a SQL function arrive wrapped by DuckDB as `InvalidInputException: ... Python exception occurred while executing the UDF: NotFoundError: ...`. Look for the name after `UDF:` below.

## Setup

**`ConfigError: No turbopuffer API key`**
Set `TURBOPUFFER_API_KEY`, pass `api_key=`, or pass `env_file=` pointing at a file that sets it. `connect()` doesn't go looking for a `.env` file by itself, so having one in the working directory isn't enough.

**`ConfigError: env_file '...' doesn't exist`**
The path is resolved from the current working directory, not from the script's location. Build it from `Path(__file__)` if the script might be run from somewhere else.

**`AuthenticationError` (401)**
The key is wrong, or it belongs to a different region. Check `TURBOPUFFER_REGION`, which defaults to `aws-us-east-1`.

**`NotFoundError` (404) on a query**
The namespace doesn't exist, or it's in another region. `tp.namespaces(prefix="...")` lists what the key can see.

## Searching

**`Catalog Error: Scalar Function with name tpuf_bm25 does not exist`**
Call `ta.register(con, tp)` on that connection first. Functions are registered per connection, and the macros are temporary, so a new connection or a reopened file needs it again.

**`tpuf_search: rank_by and filters must be JSON`**
You passed `tpuf_search` text that isn't JSON. Build it with `json_array(...)` instead of concatenating strings, which also takes care of quotes in the query text.

**`SchemaError: attribute 'score' would overwrite the 'score' column`**
`query()` uses the names `rank` and `score` for its own columns. Leave that attribute out of `include=`, or read it with `scan()`.

**Every hit's join comes back NULL**
Ids from the SQL functions are `VARCHAR`. If your table has integer ids, cast one side: `ON p.id = h.id::BIGINT`.

**`RateLimitError` (429) after retries**
Something else is querying the same namespace, or you have several clients running. Lower `max_concurrency` in `connect()`. `count()` takes 4 of turbopuffer's 16 per-namespace slots, so running counts alongside searches fills up quickly.

**A search with a filter returns fewer hits than `top_k`**
That's how filtered search works: turbopuffer returns only what matches. Check the count with `tp.count(ns, filters=...)`.

## Scanning

**`This RecordBatchReader has already been consumed`**
A reader can be read once. Use `tp.copy_to(con, ns, "table")`, or call `.read_all()` to get a table you can read again.

**A scan with a `WHERE` clause is slow**
The `WHERE` runs in DuckDB, after the whole namespace has been downloaded. Pass the condition as `filters=` to `scan()` so turbopuffer filters before sending anything.

**Datetime columns are text**
turbopuffer returns datetimes as ISO 8601 strings. Cast them in SQL: `created_at::TIMESTAMPTZ`.

**`connection has an open result` or `connection has an unfinished result`**
An earlier result on the same connection is still open. Finish reading it, or call `reader.close()`. `upsert()` and `copy_to()` close their readers themselves.

## Writing

**`SchemaError: column 'embedding' is a list of floats, so pass its type in schema=`**
A float list could be a vector or a plain array, and turbopuffer can't change the type once it's set. Say which:

```python
schema={"embedding": {"type": "[1024]f32", "ann": True}}    # a vector
schema={"embedding": {"type": "[]float"}}                    # a plain array
```

The first write that includes a vector also needs `distance_metric=`.

**`SchemaError: column 'x' has Arrow type ..., which turbopuffer can't store`**
BLOB, INTERVAL, STRUCT, and MAP have no turbopuffer type. Cast the column in your SELECT (`x::VARCHAR`, `to_json(x)`), or leave it out.

**`SchemaError: id column 'id' has type ...`**
turbopuffer ids are strings, unsigned integers, or UUIDs. Cast it: `id::VARCHAR` or `id::UBIGINT`.

**`SchemaError: the data has an 'id' column as well as id_column=...`**
turbopuffer would keep only one of them. Drop or rename `id` in your SELECT.

**`BadRequestError` (400) about a type mismatch**
The attribute already exists in the namespace with a different type, often one turbopuffer inferred from an earlier write. `tp.attr_types(ns, refresh=True)` shows the current types. Cast your column to match, or write to a new attribute name.

**`UpsertError: ... stopped after a client error`**
A request got a 4xx, so no more were sent. `e.stats.errors` lists the failed requests with the ids they carried, and `e.stats.rows` says how many rows made it. Fix the cause and re-run the whole upsert. Upserts are keyed on id, so rows already written are just overwritten.

**Writes are slow on an embedding namespace**
Those requests are capped at 256 rows, and turbopuffer computes the embeddings while the request is open. Raise `workers`, and `max_concurrency` in `connect()` if `workers` goes above it.

## Logging

**Nothing is logged**
The package doesn't print anything until you ask. Call `ta.enable_logging()`, or configure the `tpuf_arrow` logger in your logging setup.

**The log contains search text or row data**
`tpuf_arrow` at DEBUG logs every `rank_by` and filter. `TURBOPUFFER_LOG=debug` makes the SDK log full request bodies. Leave both off in production.

**`nanobind: leaked ... instances` when the script exits**
A DuckDB connection wasn't closed. It's harmless, and `con.close()` or `with duckdb.connect() as con:` stops it.
