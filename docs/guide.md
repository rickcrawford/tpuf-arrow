# Guide

The [reference](reference.md) lists every parameter, and [architecture](architecture.md) explains how it works underneath.

All examples assume:

```python
import duckdb
import tpuf_arrow as ta

tp = ta.connect()
con = duckdb.connect()
ta.register(con, tp)
```

## Connecting

`connect()` needs an API key and a region. Each one comes from the first place that has it:

1. the argument: `ta.connect(api_key=..., region="gcp-us-central1")`
2. the environment: `TURBOPUFFER_API_KEY`, `TURBOPUFFER_REGION`
3. a file you name: `ta.connect(env_file=".env")`

The region defaults to `aws-us-east-1`. With `env_file`, the file is read but never loaded into `os.environ`, and `connect()` never goes looking for a `.env` on its own. A missing key or a missing file raises `ConfigError`.

A `Client` is safe to share across threads. Close it when you're done, or use it as a context manager:

```python
with ta.connect() as tp:
    ...
```

## Searching from Python

`tp.query()` runs one ranked query and returns an Arrow table with `id`, `rank` (starting at 1), `score`, and then whatever attributes you asked for.

```python
hits = tp.query("products", ("title", "BM25", "red shoes"), top_k=10, include=["title", "brand"])
con.sql("SELECT * FROM hits WHERE brand IS NOT NULL")
```

`rank_by` and `filters` are turbopuffer's own expressions written as tuples or lists, exactly as the [query docs](https://turbopuffer.com/docs/query) show them:

```python
("title", "BM25", "red shoes")                       # full-text search
("text", "ANN", ("Embed", "red shoes"))              # turbopuffer embeds the query, then searches
("vector", "ANN", [0.1, 0.2, ...])                   # your own query vector
("And", (("brand", "Eq", "Nike"), ("color", "In", ["Red", "Black"])))   # a filter
```

What `score` means depends on `rank_by`. For BM25 it's a relevance score and higher is better. For vector search it's a distance and lower is better.

`include=True` returns every attribute except vectors. Name a vector attribute in a list if you want it back.

### Hybrid search and reranking

`tp.multi_query()` sends several queries in one request, all against the same snapshot of the namespace. With `rerank_by=("RRF", {})`, turbopuffer fuses them by reciprocal rank and you get back a list holding one table:

```python
q = "red shoes"
(hits,) = tp.multi_query(
    "products",
    [{"rank_by": ("title", "BM25", q), "top_k": 50}, {"rank_by": ("text", "ANN", ("Embed", q)), "top_k": 50}],
    rerank_by=("RRF", {}),
    limit=10,
    include=["title"],
)
```

Without `rerank_by`, the list has one table per query, in order. Each query is a dict of turbopuffer's query parameters, except that `include=` picks the attributes for all of them. A multi-query takes one `max_concurrency` slot per query.

`rerank_by` on `query()` hands the hits to one of turbopuffer's hosted [rerankers](https://turbopuffer.com/docs/reranking), and `score` becomes the model's. Filtering on a list of ids reranks a fixed candidate set, which is how you compare rerankers on the same input:

```python
model = {"model": "voyage/rerank-3", "attribute": "text", "query": q}
reranked = tp.query("products", ("id", "asc"), top_k=len(ids), filters=("id", "In", ids), rerank_by=model)
```

turbopuffer only reranks by an attribute the request includes, so tpuf-arrow asks for it. It's in the result only if it's in `include`. `rerank_by` works on `multi_query()` too, where the model reranks the union of the queries' hits. Hosted models have to be enabled for your org.

## Searching from SQL

`ta.register(con, tp)` adds three functions to that connection. Each returns a list of `{id, rank, score}` structs, and `unnest(..., recursive := true)` turns the list into rows.

```sql
SELECT unnest(tpuf_bm25('products', 'title', 'red shoes', 10, NULL), recursive := true);
SELECT unnest(tpuf_ann('products', 'text', 'red shoes', 10, NULL), recursive := true);
SELECT unnest(tpuf_search('products', '["title","BM25","red shoes"]', 10, NULL), recursive := true);
```

The arguments are positional: namespace, attribute, query text, top_k, filters. Use `tpuf_search` when you need a `rank_by` the other two don't build. It takes the expression as JSON text.

To search once per row, put the call in a `LATERAL` subquery:

```sql
SELECT q.query_id, h.rank, h.id
FROM queries q,
     LATERAL (SELECT unnest(tpuf_ann('products', 'text', q.text, 10, NULL), recursive := true)) h;
```

Hits only carry ids, so the rest comes from a join, either to your own table or to a scan of the namespace:

```sql
SELECT h.rank, p.title, p.brand
FROM (SELECT unnest(tpuf_bm25('products', 'title', 'red shoes', 10, NULL), recursive := true)) h
JOIN products p USING (id)
ORDER BY h.rank;
```

Filters are JSON too, and `json_array` builds them without any string quoting:

```sql
tpuf_ann('products', 'text', q.text, 10, json_array('brand', 'Eq', 'Nike'))
```

If the namespace, query, or top_k is NULL, the result is NULL and no request goes out. DuckDB passes the function up to 2,048 rows at a time. Identical searches within that batch are sent once, and the rest run in parallel, up to the client's `max_concurrency`.

Ids always come back as text (`VARCHAR`), even when the namespace uses integer ids. Cast if you join to an integer column: `JOIN products p ON p.id = h.id::BIGINT`.

## Reading a whole namespace

turbopuffer has no export endpoint, so `tp.scan()` pages through a namespace in id order, 10,000 rows per request. It returns a lazy `pyarrow.RecordBatchReader`, which DuckDB can query by name:

```python
rows = tp.scan("products", filters=("brand", "Eq", "Garmin"), columns=["title", "brand"])
con.sql("SELECT brand, count(*) FROM rows GROUP BY brand")
```

Pass `filters` and `columns` to `scan()` itself. They're sent to turbopuffer, so only the rows and attributes you asked for come back. A `WHERE` clause in SQL on top of `rows` runs in DuckDB after every page has been downloaded:

```python
tp.scan("products", filters=("brand", "Eq", "Garmin"))     # turbopuffer filters, few pages
rows = tp.scan("products")                                 # every page, then DuckDB filters
con.sql("SELECT * FROM rows WHERE brand = 'Garmin'")
```

A reader can be read once. To query the rows more than once, copy them into a table:

```python
tp.copy_to(con, "products", "garmin", filters=("brand", "Eq", "Garmin"))   # returns the row count
con.sql("SELECT * FROM garmin")
```

`copy_to` replaces the table if it exists. For a Parquet export, see [`examples/04_export.py`](../examples/04_export.py).

`tp.count("products", filters=...)` asks turbopuffer for a count without scanning.

A scan isn't a snapshot. A row written while it runs can show up in the results or not, depending on whether the scan has passed its id yet.

## Writing

`tp.upsert()` takes anything that implements the Arrow stream protocol: a DuckDB relation, or a pyarrow table or reader.

```python
rel = con.sql("SELECT sku::VARCHAR AS id, title, price FROM 'catalog.parquet'")
stats = tp.upsert("products", rel)
print(stats.rows, stats.requests, stats.billable_bytes)
```

The id column is `id` unless you pass `id_column="sku"`. Ids must be strings or non-negative integers.

### Types

turbopuffer infers an attribute's type from the first values it sees, and the type can't change after that. The inference isn't always right. A DOUBLE column whose first value is `2.0` can become an `int`, and the next write with `2.5` in it fails. So `upsert` sends a type for every attribute the namespace doesn't have yet, based on the Arrow type:

| DuckDB type | turbopuffer type |
| --- | --- |
| VARCHAR, ENUM | `string` |
| BIGINT and other signed integers, HUGEINT | `int` |
| UBIGINT and other unsigned integers | `uint` |
| DOUBLE, FLOAT, DECIMAL with a scale above 0 | `float` |
| BOOLEAN | `bool` |
| TIMESTAMP, TIMESTAMPTZ, DATE | `datetime` (sent as UTC) |
| VARCHAR[], BIGINT[], BOOLEAN[], ... | `[]string`, `[]int`, `[]bool`, ... |
| DOUBLE[], FLOAT[] | your call (see below) |
| BLOB, INTERVAL, STRUCT, MAP | not stored; raises `SchemaError` |

Attributes the namespace already has are left out of the request, so a plain write never resets their full-text or embedding settings.

### Vectors

A list of floats could be a vector you want to search or a plain array of numbers, so `upsert` won't guess. Say which in `schema=`:

```python
tp.upsert("docs", rel, schema={"embedding": {"type": "[1024]f32", "ann": True}},
          distance_metric="cosine_distance")
```

The one exception is a column named `vector`, turbopuffer's default vector attribute. turbopuffer types it from the data, and the first write that includes it needs `distance_metric=`.

### Full-text search and embeddings

Settings for new attributes also go in `schema=`, and turbopuffer's [schema docs](https://turbopuffer.com/docs/schema) list them all. This is how the sample loader sets up BM25 on two fields and has turbopuffer embed a third:

```python
english = {"language": "english", "stemming": True}
tp.upsert("products", rel, distance_metric="cosine_distance", schema={
    "title": {"type": "string", "full_text_search": english},
    "body": {"type": "string", "full_text_search": english},
    "text": {"type": "string", "embed": "voyage/voyage-4-lite", "filterable": False},
})
```

You don't need an embedding provider's API key for this. turbopuffer computes the embeddings and bills the tokens.

### Batching

Each request is sized to about 4 MB of JSON (`batch_bytes`), because turbopuffer's batch discount applies per request and reaches 50% at about 3.2 MB. Small input batches get merged until they reach that size. If the namespace embeds any attribute, requests are capped at 256 rows, which is turbopuffer's limit for those writes. `max_rows` lowers the cap further. Up to `workers` requests (default 4) are in flight at once.

### Values that change on the way in

- NaN and infinity become null, since JSON can't carry them.
- Naive timestamps are treated as UTC, and timezone-aware ones are converted to UTC.
- Nanosecond timestamps are truncated to microseconds.
- Whole-number decimals become ints and the rest become floats.

### When a write fails

If any request fails, `upsert` raises `UpsertError` after the requests already in flight have finished. `e.stats` says how many rows made it, and `e.stats.errors` has one line per failed request, with the first and last id it carried. A 4xx response (other than 429) stops new requests from going out, since the next batch would most likely fail the same way. Upserts are keyed on id, so after fixing the problem you can re-run the whole thing.

```python
try:
    tp.upsert("products", rel)
except ta.UpsertError as e:
    print(e.stats.rows, "rows written")
    for line in e.stats.errors:
        print(line)
```

## Costs and the request log

Every request that succeeds is added to `tp.log`, an Arrow table with one row per request: kind, namespace, wall time, server time, billed bytes, cache temperature, and more (the [reference](reference.md#request-log) has every column). `save_log()` appends it to a DuckDB table and clears it, so a database file can keep history across runs:

```python
con = duckdb.connect("lab.duckdb")
...
tp.save_log(con)        # appends to tpuf_requests
```

```sql
SELECT kind, count(*) AS requests,
       sum(billable_bytes_queried) / 1e15 AS dollars_queried,    -- $1 per PB at list price
       median(server_ms) AS p50_ms
FROM tpuf_requests GROUP BY kind;
```

Pass `run_id=` to `query`, `multi_query`, `scan`, `count`, or `upsert` to tag the requests, and `query_id=` to `query` or `multi_query`, so you can group the log by experiment or by query later. If you keep the hits in a table with the same two columns, a join puts each query's server time and cost next to its hits:

```sql
SELECT h.*, l.server_ms, l.billable_bytes_queried
FROM hits h JOIN tpuf_requests l ON l.run_id = h.run_id AND l.query_id = h.query_id;
```

turbopuffer bills every query for at least 1.28 GB queried, even when the namespace is smaller, so on a small namespace each search costs about the same. A scan is one billed query per page.

## Logging

The package logs to the `tpuf_arrow` logger and prints nothing until you configure it. In a script or notebook, `ta.enable_logging()` sends INFO and above to stderr. In an application, configure the `tpuf_arrow` logger along with the rest of your logging.

| Level | What you get |
| --- | --- |
| DEBUG | One line per request, including `rank_by` and filters. That means your users' search text ends up in the log. |
| INFO | One line when a scan, upsert, or `save_log` finishes |
| WARNING | Each failed write request |

The API key is never logged. Be careful with `TURBOPUFFER_LOG=debug`, which turns on the turbopuffer SDK's own logging. It logs full request bodies, including every row you upsert.

## Concurrency

An unpinned turbopuffer namespace runs 16 queries at a time. BM25 and ANN queries use one of those slots each, exact kNN uses two, and aggregations like `count()` use four. The client lets at most `max_concurrency` requests (default 8) run at once across all its threads, and counts a `count()` as four of them and a `multi_query()` as one per query. That leaves room for other clients on the same namespace. If you get 429s anyway, the SDK retries them with backoff, up to `max_retries` (default 6). If you still get `RateLimitError` after the retries, lower `max_concurrency`:

```python
tp = ta.connect(max_concurrency=4)
```
