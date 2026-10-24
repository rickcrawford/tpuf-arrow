"""A turbopuffer client that speaks Arrow, so DuckDB can query what it returns.

DuckDB's Python API can't define table functions, so this module hands DuckDB
Arrow tables and record batch readers, which it queries by variable name.
Filters and columns are arguments here. A WHERE clause in DuckDB on top of a
scan filters again after the rows arrive; it isn't pushed down.
"""

import json
import os
import threading
import time
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

import duckdb
import pyarrow as pa
from dotenv import dotenv_values
from turbopuffer import APIStatusError, NotFoundError, RateLimitError, Turbopuffer

from tpuf_arrow._logging import logger
from tpuf_arrow.errors import ConfigError, SchemaError, UpsertError
from tpuf_arrow.types import arrow_schema, check_id, coerce_rows, is_vector, write_columns, write_schema

# turbopuffer caps limit.total at 10,000, and its export guide pages by id at that size.
MAX_PAGE = 10_000

# Writes that carry an attribute turbopuffer embeds are capped at 256 rows.
EMBED_BATCH_ROWS = 256

# The batch discount reaches 50% at about 3.2 MB per request, so 4 MB earns all of it.
DEFAULT_BATCH_BYTES = 4_000_000

# An unpinned namespace runs 16 queries at once. BM25 and ANN queries take one
# slot each, exact kNN takes 2, and aggregations take 4. Eight in flight
# leaves room for another client on the same namespace.
DEFAULT_CONCURRENCY = 8
_AGGREGATE_SLOTS = 4

DEFAULT_REGION = "aws-us-east-1"
RESULT_COLUMNS = ("id", "rank", "score")

LOG_SCHEMA = pa.schema(
    [
        ("requested_at", pa.timestamp("us", tz="UTC")),
        ("kind", pa.string()),  # query, multi_query, scan, count, write
        ("namespace", pa.string()),
        ("run_id", pa.string()),
        ("query_id", pa.string()),
        ("request", pa.string()),  # the request's parameters as JSON, without attribute lists
        ("rows", pa.int64()),
        ("wall_ms", pa.float64()),
        ("server_ms", pa.float64()),
        ("query_execution_ms", pa.float64()),
        ("embedding_ms", pa.float64()),
        ("billable_bytes_queried", pa.int64()),
        ("billable_bytes_returned", pa.int64()),
        ("billable_bytes_written", pa.int64()),
        ("cache_temperature", pa.string()),
        ("cache_hit_ratio", pa.float64()),
        ("approx_namespace_size", pa.int64()),
        ("exhaustive_search_count", pa.int64()),
        ("last_included_write_at", pa.string()),
        ("embedding_tokens", pa.int64()),
    ]
)


@dataclass
class WriteStats:
    """What an upsert wrote. On failure, UpsertError.stats carries one of these."""

    rows: int = 0
    requests: int = 0
    billable_bytes: int = 0
    seconds: float = 0.0
    errors: list[str] = field(default_factory=list)


def connect(
    api_key: str | None = None,
    region: str | None = None,
    *,
    env_file: str | os.PathLike[str] | None = None,
    max_concurrency: int = DEFAULT_CONCURRENCY,
    max_retries: int = 6,
) -> "Client":
    """Open a client.

    Each setting comes from the argument, then the environment
    (TURBOPUFFER_API_KEY, TURBOPUFFER_REGION), then env_file if you pass one.
    The file is read, never loaded into os.environ, and nothing is read from
    a .env you didn't name.
    """
    if env_file is not None and not Path(env_file).is_file():
        raise ConfigError(f"env_file {str(env_file)!r} doesn't exist")
    file = {k: v for k, v in dotenv_values(env_file).items() if v} if env_file is not None else {}
    api_key = api_key or os.environ.get("TURBOPUFFER_API_KEY") or file.get("TURBOPUFFER_API_KEY")
    if not api_key:
        raise ConfigError(
            "No turbopuffer API key. Pass api_key=, set TURBOPUFFER_API_KEY, "
            "or pass env_file= pointing at a file that sets it."
        )
    region = (
        region or os.environ.get("TURBOPUFFER_REGION") or file.get("TURBOPUFFER_REGION") or DEFAULT_REGION
    )
    # The SDK retries 429s and 5xx responses with backoff. turbopuffer holds
    # the 17th concurrent query for up to 800 ms before it sends a 429.
    tpuf = Turbopuffer(api_key=api_key, region=region, max_retries=max_retries)
    return Client(tpuf, max_concurrency=max_concurrency)


class _Slots:
    """A counting semaphore that can take several permits at once without deadlocking.

    Taking permits one at a time, two callers that each need four could end up
    holding three apiece and wait forever. The lock makes each multi-permit
    take all-or-nothing with respect to other takers.
    """

    def __init__(self, n: int):
        self.n = n
        self._sem = threading.Semaphore(n)
        self._taking = threading.Lock()

    @contextmanager
    def take(self, k: int = 1) -> Iterator[None]:
        k = min(k, self.n)
        with self._taking:
            for _ in range(k):
                self._sem.acquire()
        try:
            yield
        finally:
            for _ in range(k):
                self._sem.release()


class Client:
    """Reads and writes one turbopuffer account's namespaces as Arrow. Safe to share across threads.

    Use connect() to make one. Close it when you're done, or use it as a
    context manager.
    """

    def __init__(self, tpuf: Turbopuffer, *, max_concurrency: int = DEFAULT_CONCURRENCY):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        self.tpuf = tpuf
        self.max_concurrency = max_concurrency
        # Every request this client makes takes slots from one semaphore, so
        # threads calling it at once can't push a namespace past its limit.
        self._slots = _Slots(max_concurrency)
        self._types: dict[str, dict[str, str]] = {}
        self._log: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"<tpuf_arrow.Client region={self.tpuf.region!r}>"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the HTTP connections. The request log stays readable."""
        self.tpuf.close()

    # Reads -----------------------------------------------------------------

    def query(
        self,
        namespace: str,
        rank_by: Any,
        *,
        top_k: int = 10,
        filters: Any = None,
        include: list[str] | bool | None = None,
        rerank_by: Any = None,
        run_id: str | None = None,
        query_id: str | None = None,
    ) -> pa.Table:
        """One ranked query, as an Arrow table of id, rank, score, then the included attributes.

        score is turbopuffer's $dist: a BM25 score (higher is better) or a
        vector distance (lower is better), depending on rank_by. With
        rerank_by, a hosted model such as {"model": "voyage/rerank-3",
        "attribute": "text", "query": q} reorders the top_k hits, and score is
        the model's. The attribute it reads is requested for you, and returned
        only if it's in include.

        include=True means every attribute except vectors. An attribute named
        rank or score can't be included, since those names hold the ranking;
        rename it, or use scan() to read it.
        """
        columns = self._result_columns(namespace, include)
        resp = self._query(
            "query",
            namespace,
            run_id=run_id,
            query_id=query_id,
            rank_by=rank_by,
            top_k=top_k,
            filters=filters,
            include_attributes=_with_rerank_attribute(columns, rerank_by) or None,
            rerank_by=rerank_by,
        )
        return self._hits(namespace, resp.rows, columns)

    def multi_query(
        self,
        namespace: str,
        queries: list[dict[str, Any]],
        *,
        rerank_by: Any = None,
        limit: int | None = None,
        include: list[str] | bool | None = None,
        run_id: str | None = None,
        query_id: str | None = None,
    ) -> list[pa.Table]:
        """Several queries in one request, against one snapshot of the namespace.

        Each query is a dict of query parameters: rank_by, top_k, filters, and
        so on. Without rerank_by you get one table per query, in order. With
        rerank_by, turbopuffer merges the results and you get a list of one
        table: ("RRF", {}) fuses them with reciprocal rank fusion, and a
        hosted model reranks their union. limit caps the merged results, so
        it needs rerank_by.

        Tables have the same columns as query(), and include picks the
        attributes for every query, so the queries can't set
        include_attributes or exclude_attributes themselves. Each query takes
        one max_concurrency slot, and aggregations aren't supported; use
        count() for those.
        """
        if not queries:
            raise ValueError("multi_query needs at least one query")
        if len(queries) > self.max_concurrency:
            raise ValueError(
                f"multi_query with {len(queries)} queries needs that many slots, "
                f"and max_concurrency is {self.max_concurrency}"
            )
        for q in queries:
            if bad := sorted({"include_attributes", "exclude_attributes", "aggregate_by"} & q.keys()):
                hint = "use count()" if bad == ["aggregate_by"] else "pass include= to multi_query"
                raise ValueError(f"multi_query queries can't set {', '.join(bad)}; {hint}")
        if limit is not None and rerank_by is None:
            raise ValueError("limit caps the merged results, so it needs rerank_by")
        columns = self._result_columns(namespace, include)
        sent = _with_rerank_attribute(columns, rerank_by)
        queries = [{**q, "include_attributes": sent} if sent else dict(q) for q in queries]
        resp = self._query(
            "multi_query",
            namespace,
            run_id=run_id,
            query_id=query_id,
            slots=len(queries),
            queries=queries,
            rerank_by=rerank_by,
            limit=limit,
        )
        return [self._hits(namespace, r.rows, columns) for r in resp.results or []]

    def scan(
        self,
        namespace: str,
        *,
        filters: Any = None,
        columns: list[str] | bool | None = True,
        page_size: int = MAX_PAGE,
        limit: int | None = None,
        run_id: str | None = None,
    ) -> pa.RecordBatchReader:
        """Every row, or every row matching filters, paged in id order.

        turbopuffer has no export endpoint. Its export guide pages like this:
        rank by id, filter on id > the last id seen, 10,000 rows a page. Each
        page is a billed query. Rows written during the scan can show up in
        it, so it isn't a snapshot.

        The reader is lazy and can be read once. Register it with DuckDB and
        query it, or call read_all() first if you need to read it twice.
        """
        if not 0 < page_size <= MAX_PAGE:
            raise ValueError(f"page_size must be between 1 and {MAX_PAGE}")
        if limit is not None and limit < 0:
            raise ValueError("limit can't be negative")
        cols = self._columns(namespace, columns)
        schema = arrow_schema(self.attr_types(namespace), cols)

        def pages() -> Iterator[pa.RecordBatch]:
            last, seen, started = None, 0, time.perf_counter()
            while limit is None or seen < limit:
                size = page_size if limit is None else min(page_size, limit - seen)
                after = ("id", "Gt", last) if last is not None else None
                resp = self._query(
                    "scan",
                    namespace,
                    run_id=run_id,
                    rank_by=("id", "asc"),
                    top_k=size,
                    filters=_and(filters, after),
                    include_attributes=cols or None,
                )
                rows = [r.to_dict() for r in resp.rows or []]
                for row in rows:
                    row.pop("$dist", None)
                if rows:
                    yield pa.RecordBatch.from_pylist(coerce_rows(rows, schema), schema=schema)
                seen += len(rows)
                if len(rows) < size:
                    break
                last = rows[-1]["id"]
            logger.info("scan %s: %d rows in %.1fs", namespace, seen, time.perf_counter() - started)

        return pa.RecordBatchReader.from_batches(schema, pages())

    def count(self, namespace: str, *, filters: Any = None, run_id: str | None = None) -> int:
        """Rows matching filters, counted by turbopuffer instead of by scanning.

        An aggregation costs 4 of the namespace's 16 query slots, and
        turbopuffer's docs advise against them on latency-sensitive paths for
        namespaces over 1M documents.
        """
        resp = self._query(
            "count",
            namespace,
            run_id=run_id,
            slots=_AGGREGATE_SLOTS,
            aggregate_by={"n": ("Count",)},
            filters=filters,
        )
        return int(resp.aggregations["n"])

    def copy_to(self, con: duckdb.DuckDBPyConnection, namespace: str, table: str, **scan_args: Any) -> int:
        """Scan a namespace into a DuckDB table, replacing it, and return the row count.

        table may be schema-qualified ("main.products"). A name with a dot in
        it is always read as schema.table.
        """
        reader = self.scan(namespace, **scan_args)
        view = _temp_name("scan")
        con.register(view, reader)
        try:
            con.execute(f'CREATE OR REPLACE TABLE {_ident(table)} AS SELECT * FROM "{view}"')
        finally:
            con.unregister(view)
            reader.close()
        return con.execute(f"SELECT count(*) FROM {_ident(table)}").fetchall()[0][0]

    # Namespaces ------------------------------------------------------------

    def namespaces(self, prefix: str | None = None) -> pa.Table:
        """Namespace names in the account, optionally only those starting with prefix."""
        page = self.tpuf.namespaces(**_given(prefix=prefix, page_size=1000))
        return pa.table({"namespace": pa.array([n.id for n in page], pa.string())})

    def metadata(self, namespace: str) -> dict[str, Any]:
        """Row count, size, index status, timestamps, and attribute types for a namespace."""
        m = self.tpuf.namespace(namespace).metadata()
        return {
            "namespace": namespace,
            "approx_row_count": m.approx_row_count,
            "approx_logical_bytes": m.approx_logical_bytes,
            "index_status": getattr(m.index, "status", None),
            "created_at": m.created_at,
            "updated_at": m.updated_at,
            "last_write_at": getattr(m, "last_write_at", None),
            "schema": {k: v.type for k, v in (m.schema_ or {}).items()},
        }

    def attr_types(self, namespace: str, *, refresh: bool = False) -> dict[str, str]:
        """Attribute name to turbopuffer type ("string", "[1024]f16", ...), cached per namespace.

        upsert() clears the cache for the namespace it writes. Pass
        refresh=True if something else changed the schema.
        """
        if refresh or namespace not in self._types:
            schema = self.tpuf.namespace(namespace).schema()
            self._types[namespace] = {k: v.type for k, v in schema.items() if v.type}
        return self._types[namespace]

    def delete_namespace(self, namespace: str) -> bool:
        """Delete a namespace and everything in it. Returns False if it didn't exist."""
        self._types.pop(namespace, None)
        try:
            self.tpuf.namespace(namespace).delete_all()
        except NotFoundError:
            return False
        return True

    # Writes ----------------------------------------------------------------

    def upsert(
        self,
        namespace: str,
        data: Any,
        *,
        schema: dict[str, Any] | None = None,
        id_column: str = "id",
        batch_bytes: int = DEFAULT_BATCH_BYTES,
        max_rows: int | None = None,
        workers: int = 4,
        distance_metric: str | None = None,
        run_id: str | None = None,
    ) -> WriteStats:
        """Upsert rows from anything with __arrow_c_stream__: a DuckDB relation, an Arrow table or reader.

        Requests are sized by their JSON size, about batch_bytes each, because
        turbopuffer's batch discount is per request. If any attribute is
        embedded by turbopuffer, requests are capped at 256 rows.

        New attributes get turbopuffer types from their Arrow types. Existing
        attributes keep their settings. Float lists raise SchemaError unless
        schema= says whether each is a vector.

        If a request fails with a 4xx, no more are sent. Failures raise
        UpsertError once in-flight requests finish; rows in the requests that
        succeeded are written, and re-running is safe since upserts are keyed
        on id.
        """
        if batch_bytes < 1 or workers < 1 or (max_rows is not None and max_rows < 1):
            raise ValueError("batch_bytes, workers, and max_rows must be at least 1")
        reader = pa.RecordBatchReader.from_stream(data)
        try:
            return self._upsert(
                namespace,
                reader,
                schema=schema,
                id_column=id_column,
                batch_bytes=batch_bytes,
                max_rows=max_rows,
                workers=workers,
                distance_metric=distance_metric,
                run_id=run_id,
            )
        finally:
            # A DuckDB connection refuses new queries while a result stream is
            # open, so the reader is closed whether or not the upsert worked.
            reader.close()

    def _upsert(
        self,
        namespace: str,
        reader: pa.RecordBatchReader,
        *,
        schema: dict[str, Any] | None,
        id_column: str,
        batch_bytes: int,
        max_rows: int | None,
        workers: int,
        distance_metric: str | None,
        run_id: str | None,
    ) -> WriteStats:
        check_id(reader.schema, id_column)
        existing = self._existing_schema(namespace)
        cap = min(max_rows or MAX_PAGE, EMBED_BATCH_ROWS if _embeds(existing, schema) else MAX_PAGE)
        # Typing new attributes from Arrow stops turbopuffer guessing from the
        # first values it sees (a float column of 2.0s reads as int). Attributes
        # the namespace already has are left out, so a write can't reset their
        # full-text or embedding settings.
        known = set(existing) | set(schema or {})
        full_schema = {**write_schema(reader.schema, id_column=id_column, known=known), **(schema or {})}
        stats, started = WriteStats(), time.perf_counter()
        stop = threading.Event()

        def write(chunk: pa.Table) -> tuple[int, int]:
            if stop.is_set():
                return 0, 0
            t0 = time.perf_counter()
            try:
                with self._slots.take():
                    resp = self.tpuf.namespace(namespace).write(
                        upsert_columns=write_columns(chunk, id_column),  # type: ignore[arg-type]
                        **_given(schema=full_schema or None, distance_metric=distance_metric),
                    )
            except Exception as e:
                if _is_client_error(e):
                    stop.set()
                ids = chunk.column(id_column)
                raise _WriteFailed(f"ids {ids[0]} to {ids[-1]}: {type(e).__name__}: {e}") from e
            billed = getattr(resp.billing, "billable_logical_bytes_written", None) or 0
            self._record("write", namespace, resp, t0, chunk.num_rows, run_id=run_id)
            return chunk.num_rows, billed

        with ThreadPoolExecutor(max_workers=workers) as pool:
            inflight: set[Future[tuple[int, int]]] = set()
            for chunk in _rebatch(reader, batch_bytes, cap):
                if stop.is_set():
                    break
                if len(inflight) >= workers * 2:
                    done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    _collect(done, stats)
                inflight.add(pool.submit(write, chunk))
            _collect(inflight, stats)
        stats.seconds = time.perf_counter() - started
        self._types.pop(namespace, None)  # the write may have added attributes
        if stats.errors:
            how = "stopped after a client error" if stop.is_set() else "finished"
            for line in stats.errors:
                logger.warning("upsert %s: request failed, %s", namespace, line)
            raise UpsertError(
                f"upsert to {namespace!r} {how}: {len(stats.errors)} request(s) failed, "
                f"{stats.rows:,} rows written. First failure: {stats.errors[0]}",
                stats,
            )
        logger.info(
            "upsert %s: %d rows in %d requests, %.1fs", namespace, stats.rows, stats.requests, stats.seconds
        )
        return stats

    # Request log -----------------------------------------------------------

    @property
    def log(self) -> pa.Table:
        """One row per request this client has made since the last save_log()."""
        with self._lock:
            return pa.Table.from_pylist(list(self._log), schema=LOG_SCHEMA)

    def save_log(self, con: duckdb.DuckDBPyConnection, table: str = "tpuf_requests") -> int:
        """Append the request log to a DuckDB table, creating it if needed, then clear the log.

        Columns are matched by name. An existing table can have extra columns,
        or fewer (the log's other columns are dropped), but it needs at least
        one of the log's. If the insert fails, the log is kept.
        """
        with self._lock:
            rows = list(self._log)
        tbl = pa.Table.from_pylist(rows, schema=LOG_SCHEMA)
        view = _temp_name("log")
        con.register(view, tbl)
        try:
            con.execute(f'CREATE TABLE IF NOT EXISTS {_ident(table)} AS SELECT * FROM "{view}" LIMIT 0')
            have = {d[0] for d in con.execute(f"SELECT * FROM {_ident(table)} LIMIT 0").description}
            cols = [c for c in LOG_SCHEMA.names if c in have]
            if not cols:
                raise SchemaError(f"table {table!r} has none of the request log's columns")
            select = ", ".join(f'"{c}"' for c in cols)
            con.execute(f'INSERT INTO {_ident(table)} BY NAME SELECT {select} FROM "{view}"')
        finally:
            con.unregister(view)
        with self._lock:
            del self._log[: len(rows)]  # rows logged during the insert stay for next time
        logger.info("save_log: %d requests to %s", len(rows), table)
        return len(rows)

    # Internals -------------------------------------------------------------

    def _query(
        self,
        kind: str,
        namespace: str,
        *,
        run_id: str | None = None,
        query_id: str | None = None,
        slots: int = 1,
        **params: Any,
    ) -> Any:
        params = _given(**params)
        t0 = time.perf_counter()
        ns = self.tpuf.namespace(namespace)
        with self._slots.take(slots):
            if kind == "multi_query":
                resp = ns.multi_query(**params)
                rows = sum(len(r.rows or []) for r in resp.results or [])
            else:
                resp = ns.query(**params)
                rows = len(resp.rows or [])
        request = {k: v for k, v in params.items() if k != "include_attributes"}
        if "queries" in request:
            request["queries"] = [
                {k: v for k, v in q.items() if k != "include_attributes"} for q in request["queries"]
            ]
        self._record(
            kind,
            namespace,
            resp,
            t0,
            rows,
            run_id=run_id,
            query_id=query_id,
            request=json.dumps(request, default=str),
        )
        return resp

    def _record(
        self,
        kind: str,
        namespace: str,
        resp: Any,
        t0: float,
        rows: int,
        *,
        run_id: str | None = None,
        query_id: str | None = None,
        request: str | None = None,
    ) -> None:
        perf, bill = getattr(resp, "performance", None), getattr(resp, "billing", None)
        row = {
            "requested_at": datetime.now(UTC),
            "kind": kind,
            "namespace": namespace,
            "run_id": run_id,
            "query_id": query_id,
            "request": request,
            "rows": rows,
            "wall_ms": (time.perf_counter() - t0) * 1000,
            "server_ms": getattr(perf, "server_total_ms", None),
            "query_execution_ms": getattr(perf, "query_execution_ms", None),
            "embedding_ms": getattr(perf, "embedding_ms", None),
            "billable_bytes_queried": getattr(bill, "billable_logical_bytes_queried", None),
            "billable_bytes_returned": getattr(bill, "billable_logical_bytes_returned", None),
            "billable_bytes_written": getattr(bill, "billable_logical_bytes_written", None),
            "cache_temperature": getattr(perf, "cache_temperature", None),
            "cache_hit_ratio": getattr(perf, "cache_hit_ratio", None),
            "approx_namespace_size": getattr(perf, "approx_namespace_size", None),
            "exhaustive_search_count": getattr(perf, "exhaustive_search_count", None),
            "last_included_write_at": getattr(perf, "last_included_write_at", None),
            "embedding_tokens": getattr(perf, "embedding_tokens", None),
        }
        logger.debug(
            "%s %s: %d rows, %.0f ms, %s billed bytes, request=%s",
            kind,
            namespace,
            rows,
            row["wall_ms"],
            row["billable_bytes_queried"] or row["billable_bytes_written"],
            request,
        )
        with self._lock:
            self._log.append(row)

    def _result_columns(self, namespace: str, include: list[str] | bool | None) -> list[str]:
        columns = self._columns(namespace, include)
        if clash := sorted(set(columns) & {"rank", "score"}):
            raise SchemaError(
                f"attribute {clash[0]!r} would overwrite the {clash[0]!r} column query() "
                f"returns. Leave it out of include=, or read it with scan()."
            )
        return columns

    def _hits(self, namespace: str, rows: Any, columns: list[str]) -> pa.Table:
        """Response rows as id, rank, score, then the included attributes."""
        rows = [r.to_dict() for r in rows or []]
        for rank, row in enumerate(rows, 1):
            row["rank"] = rank
            row["score"] = row.pop("$dist", None)
        base = arrow_schema(self.attr_types(namespace), columns)
        schema = pa.schema(
            [
                base.field("id"),
                ("rank", pa.int32()),
                ("score", pa.float64()),
                *[f for f in base if f.name != "id"],
            ]
        )
        return pa.Table.from_pylist(coerce_rows(rows, schema), schema=schema)

    def _columns(self, namespace: str, include: list[str] | bool | None) -> list[str]:
        if include is True:
            return [k for k, t in self.attr_types(namespace).items() if k != "id" and not is_vector(t)]
        if isinstance(include, str):
            raise TypeError("include and columns take a list of names, or True")
        return [c for c in include or [] if c != "id"]

    def _existing_schema(self, namespace: str) -> dict[str, Any]:
        """The namespace's schema from metadata (which, unlike /schema, keeps embed settings)."""
        try:
            return self.tpuf.namespace(namespace).metadata().schema_ or {}
        except NotFoundError:
            return {}


class _WriteFailed(Exception):
    """A write request failed; the message names the ids it carried."""


def _is_client_error(e: Exception) -> bool:
    """A 4xx other than 429: sending the next batch would fail the same way."""
    return isinstance(e, APIStatusError) and 400 <= e.status_code < 500 and not isinstance(e, RateLimitError)


def _embeds(existing: dict[str, Any], schema: dict[str, Any] | None) -> bool:
    """Does a write to this namespace carry an attribute turbopuffer embeds?"""
    new = any(isinstance(s, dict) and s.get("embed") for s in (schema or {}).values())
    return new or any(getattr(s, "embed", None) for s in existing.values())


def _with_rerank_attribute(columns: list[str], rerank_by: Any) -> list[str]:
    """turbopuffer only reranks by an attribute the request includes, so add it."""
    attr = rerank_by.get("attribute") if isinstance(rerank_by, dict) else None
    return [*columns, attr] if attr and attr not in columns else columns


def _given(**kwargs: Any) -> dict[str, Any]:
    """Drop arguments left as None, so the SDK leaves them out of the request."""
    return {k: v for k, v in kwargs.items() if v is not None}


def _and(a: Any, b: Any) -> Any:
    if a is None or b is None:
        return a if b is None else b
    return ("And", (a, b))


def _ident(name: str) -> str:
    """Quote a table name, reading a dot as a schema qualifier (main.results)."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def _temp_name(kind: str) -> str:
    return f"_tpuf_{kind}_{threading.get_ident()}_{time.monotonic_ns()}"


def _json_bytes_per_row(batch: pa.RecordBatch, sample: int = 32) -> float:
    """Estimated JSON size of one row. Arrow sizes run several times smaller than the JSON
    turbopuffer receives (a float32 is 4 bytes in Arrow and about 20 as JSON text)."""
    head = batch.slice(0, sample).to_pylist()
    return max(1.0, len(json.dumps(head, default=str)) / len(head))


def _rebatch(reader: pa.RecordBatchReader, target_bytes: int, max_rows: int) -> Iterator[pa.Table]:
    """Regroup a stream of record batches into write requests of about target_bytes and at most max_rows.

    Small source batches are merged, so a table that arrives in 100-row
    pieces still goes out in requests big enough to earn the batch discount.
    """
    pending: list[pa.RecordBatch] = []
    rows, size = 0, 0.0
    for batch in reader:
        if batch.num_rows == 0:
            continue
        per_row = _json_bytes_per_row(batch)
        start = 0
        while start < batch.num_rows:
            room = min(max_rows - rows, max(1, int((target_bytes - size) / per_row)))
            piece = batch.slice(start, room)
            pending.append(piece)
            rows, size, start = rows + piece.num_rows, size + piece.num_rows * per_row, start + piece.num_rows
            if rows >= max_rows or size >= target_bytes:
                yield pa.Table.from_batches(pending, schema=reader.schema)
                pending, rows, size = [], 0, 0.0
    if pending:
        yield pa.Table.from_batches(pending, schema=reader.schema)


def _collect(futures: set[Future[tuple[int, int]]], stats: WriteStats) -> None:
    for f in futures:
        try:
            rows, billed = f.result()
        except _WriteFailed as e:
            stats.errors.append(str(e))
            continue
        if rows:
            stats.rows += rows
            stats.requests += 1
            stats.billable_bytes += billed
