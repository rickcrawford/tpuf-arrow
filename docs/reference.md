# Reference

Everything public in `tpuf_arrow`. The [guide](guide.md) has the longer explanations and examples.

```python
import tpuf_arrow as ta
```

## Connecting

### `ta.connect(api_key=None, region=None, *, env_file=None, max_concurrency=8, max_retries=6)`

Returns a `Client`.

| Parameter | Default | |
| --- | --- | --- |
| `api_key` | `TURBOPUFFER_API_KEY` | Checked in this order: argument, environment, `env_file` |
| `region` | `TURBOPUFFER_REGION`, then `aws-us-east-1` | Same order |
| `env_file` | none | A dotenv file to read settings from. It's never loaded into `os.environ`. |
| `max_concurrency` | 8 | Most requests in flight at once, across all threads |
| `max_retries` | 6 | Passed to the turbopuffer SDK, which retries 429 and 5xx responses with backoff |

Raises `ConfigError` if there's no key, or if `env_file` doesn't exist.

## `Client`

Safe to share across threads. It works as a context manager, and `close()` closes its HTTP connections. `tp.tpuf` is the underlying `turbopuffer.Turbopuffer` client, for anything this package doesn't wrap.

### Reading

| Method | Returns | Requests |
| --- | --- | --- |
| `query(namespace, rank_by, *, top_k=10, filters=None, include=None, run_id=None, query_id=None)` | `pa.Table` | 1 |
| `scan(namespace, *, filters=None, columns=True, page_size=10_000, limit=None, run_id=None)` | `pa.RecordBatchReader` | 1 per page |
| `count(namespace, *, filters=None, run_id=None)` | `int` | 1 aggregation |
| `copy_to(con, namespace, table, **scan_args)` | `int`, rows copied | same as `scan` |

**`query`** returns `id`, `rank` (int32, starting at 1), `score` (float64, turbopuffer's `$dist`), then the included attributes. `include` is a list of attribute names, or `True` for every attribute except vectors. Including an attribute named `rank` or `score` raises `SchemaError`, since it would overwrite those columns. Column types come from the namespace schema, so an empty result has the same columns as a full one. `top_k` can be at most 10,000.

**`scan`** pages through the namespace in id order: rank by id, filter on `id > last id seen`, `page_size` rows per request. `filters` and `columns` are sent to turbopuffer. `columns=True` means every attribute except vectors. `limit` stops after that many rows. The reader is lazy, can be read once, and isn't a snapshot.

**`count`** uses turbopuffer's `Count` aggregation. It counts as 4 against `max_concurrency`.

**`copy_to`** runs `scan` and writes the rows to a DuckDB table, replacing it if it exists. `table` can be schema-qualified (`"main.products"`). A dot in the name always separates schema and table.

### Namespaces

| Method | Returns |
| --- | --- |
| `namespaces(prefix=None)` | `pa.Table` with a `namespace` column, all pages |
| `metadata(namespace)` | `dict`: `approx_row_count`, `approx_logical_bytes`, `index_status`, `created_at`, `updated_at`, `last_write_at`, `schema` |
| `attr_types(namespace, *, refresh=False)` | `dict` of attribute name to turbopuffer type, such as `"string"` or `"[1024]f16"`. Cached per namespace. `upsert` clears the cache for the namespace it writes. |
| `delete_namespace(namespace)` | `True`, or `False` if it didn't exist |

### Writing

#### `upsert(namespace, data, *, schema=None, id_column="id", batch_bytes=4_000_000, max_rows=None, workers=4, distance_metric=None, run_id=None)`

Returns `WriteStats`.

| Parameter | |
| --- | --- |
| `data` | Anything with `__arrow_c_stream__`: a DuckDB relation, a pyarrow table, or a `RecordBatchReader` |
| `schema` | turbopuffer schema entries, such as `{"title": {"type": "string", "full_text_search": True}}`. These win over the inferred types. |
| `id_column` | The column to use as the id, renamed to `id` on the way out. A string or a non-negative integer. If it isn't `id`, the data can't also have a column named `id`. |
| `batch_bytes` | Target JSON size per request |
| `max_rows` | Most rows per request, 10,000 by default. Embedding namespaces are capped at 256 either way. |
| `workers` | Requests in flight at once |
| `distance_metric` | `"cosine_distance"` or `"euclidean_squared"`, needed on the first write that includes a vector |

Attributes that are new to the namespace get types from their Arrow types (see the [guide](guide.md#types)). A float list raises `SchemaError` unless it's in `schema` or named `vector`. So does any column turbopuffer can't store.

Raises `UpsertError` if any request fails.

#### `WriteStats`

| Field | |
| --- | --- |
| `rows` | Rows in requests that succeeded |
| `requests` | Requests that succeeded |
| `billable_bytes` | Billed bytes written |
| `seconds` | Wall time |
| `errors` | One line per failed request (empty on success) |

### Request log

#### `log`

An Arrow table with one row per successful request since the last `save_log`. Requests that raise, including failed writes, aren't logged.

| Column | Type | |
| --- | --- | --- |
| `requested_at` | timestamp, UTC | When the request started |
| `kind` | string | `query`, `scan`, `count`, or `write` |
| `namespace` | string | |
| `run_id`, `query_id` | string | Whatever you passed |
| `request` | string | `rank_by`, `filters`, `top_k`, and `aggregate_by` as JSON. Attribute lists are left out. |
| `rows` | int64 | Rows returned, or written |
| `wall_ms` | float64 | Measured by the client from before it waits for a `max_concurrency` slot, so it includes queueing and retries. Use `server_ms` for latency. |
| `server_ms` | float64 | turbopuffer's `server_total_ms` |
| `billable_bytes_queried`, `billable_bytes_returned`, `billable_bytes_written` | int64 | From the response's `billing` |
| `cache_temperature`, `cache_hit_ratio` | string, float64 | From `performance` |
| `approx_namespace_size` | int64 | Approximate document count, from `performance` |
| `last_included_write_at` | string | The newest write the query saw. If two runs differ here, they didn't see the same data. |
| `embedding_tokens` | int64 | Tokens turbopuffer embedded for this request |

Columns turbopuffer didn't return are null.

#### `save_log(con, table="tpuf_requests")`

Appends the log to a DuckDB table, creating it if needed, clears the log, and returns the number of rows saved. Columns are matched by name, so an existing table can have extra columns or be missing some. If the insert fails, the log is kept.

## SQL functions

### `ta.register(con, client)`

Adds these to a DuckDB connection. Calling it again replaces them. Each returns `LIST(STRUCT(id VARCHAR, rank INTEGER, score DOUBLE))`.

| Function | `rank_by` it sends |
| --- | --- |
| `tpuf_search(namespace, rank_by_json, top_k, filters_json)` | the JSON you pass |
| `tpuf_bm25(namespace, attribute, query, top_k, filters_json)` | `[attribute, "BM25", query]` |
| `tpuf_ann(namespace, attribute, query, top_k, filters_json)` | `[attribute, "ANN", ["Embed", query]]` |

`filters_json` can be NULL. A NULL namespace, query, or top_k returns NULL without sending a request. Ids come back as text. JSON that doesn't parse raises an error that stops the query.

## Logging

### `ta.enable_logging(level=logging.INFO)`

Adds a stderr handler to the `tpuf_arrow` logger and sets its level. Calling it twice doesn't add a second handler. Without it, nothing prints unless your application has configured logging. The [guide](guide.md#logging) says what each level contains.

## Errors

| Exception | Raised when |
| --- | --- |
| `TpufArrowError` | Base class for the ones below |
| `ConfigError` | No API key, or `env_file` doesn't exist |
| `SchemaError` | A column or type can't map to or from turbopuffer. Also a `ValueError`. |
| `UpsertError` | Some write requests failed. `.stats` is a `WriteStats`. |

When you call `query`, `scan`, `count`, or the namespace methods, errors from turbopuffer (after the SDK's retries) are the SDK's own exceptions, all subclasses of `turbopuffer.APIError`: `AuthenticationError`, `NotFoundError`, `BadRequestError`, `RateLimitError`, and so on. Inside a SQL function, DuckDB wraps them in `duckdb.InvalidInputException`, with a message like "Python exception occurred while executing the UDF: NotFoundError: ...". In `upsert`, each failed write becomes a line in `UpsertError.stats.errors`. Bad arguments, like a negative `limit`, raise `ValueError`.

`ta.__version__` holds the installed version.
