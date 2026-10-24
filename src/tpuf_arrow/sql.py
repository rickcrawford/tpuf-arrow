"""SQL functions for searching turbopuffer from inside a DuckDB query.

register(con, client) adds three functions, each returning
LIST(STRUCT(id VARCHAR, rank INTEGER, score DOUBLE)):

  tpuf_search(namespace, rank_by_json, top_k, filters_json)
  tpuf_bm25(namespace, attribute, query, top_k, filters_json)
  tpuf_ann(namespace, attribute, query, top_k, filters_json)   turbopuffer embeds the query

filters_json can be NULL. Unnest the list to get one row per hit:

  SELECT q.query_id, h.*
  FROM queries q, LATERAL (SELECT unnest(tpuf_bm25('products', 'title', q.text, 10, NULL),
                                         recursive := true)) h

DuckDB hands the function up to 2,048 rows at a time. Identical searches in
a batch are sent once, the rest run on a thread pool, and the client's
semaphore caps how many are in flight.
"""

import contextlib
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb
import pyarrow as pa
from duckdb.func import ARROW, SPECIAL
from duckdb.sqltypes import INTEGER, VARCHAR

from tpuf_arrow.client import Client

HITS = pa.list_(pa.struct([("id", pa.string()), ("rank", pa.int32()), ("score", pa.float64())]))

_MACROS = {
    "tpuf_bm25": "json_array(attr, 'BM25', q)",
    "tpuf_ann": "json_array(attr, 'ANN', json_array('Embed', q))",
}


def register(con: duckdb.DuckDBPyConnection, client: Client) -> None:
    """Add tpuf_search, tpuf_bm25, and tpuf_ann to a connection, searching through client.

    Calling it again replaces the functions, so you can switch clients or
    re-run a notebook cell.
    """

    def search(
        namespaces: pa.ChunkedArray,
        rank_bys: pa.ChunkedArray,
        top_ks: pa.ChunkedArray,
        filters: pa.ChunkedArray,
    ) -> pa.Array:
        args = list(
            zip(
                namespaces.to_pylist(),
                rank_bys.to_pylist(),
                top_ks.to_pylist(),
                filters.to_pylist(),
                strict=True,
            )
        )
        # A join can repeat the same search many times in one batch, so each
        # distinct search is sent once.
        unique = list(dict.fromkeys(a for a in args if None not in a[:3]))
        workers = max(1, min(len(unique), client.max_concurrency))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            hits = dict(zip(unique, pool.map(lambda a: _search(client, *a), unique), strict=True))
        return pa.array([hits.get(a) for a in args], type=HITS)

    with contextlib.suppress(duckdb.Error):  # not registered yet
        con.remove_function("tpuf_search")
    hits_type = duckdb.list_type(duckdb.struct_type({"id": VARCHAR, "rank": INTEGER, "score": "DOUBLE"}))
    con.create_function(
        "tpuf_search",
        search,
        [VARCHAR, VARCHAR, INTEGER, VARCHAR],
        hits_type,
        type=ARROW,
        null_handling=SPECIAL,
        side_effects=True,
    )
    for name, rank_by in _MACROS.items():
        # A NULL query gives NULL hits and sends no request.
        con.execute(f"""
            CREATE OR REPLACE TEMP MACRO {name}(ns, attr, q, k, filters) AS
                tpuf_search(ns, CASE WHEN q IS NOT NULL THEN {rank_by}::VARCHAR END, k, filters::VARCHAR)
        """)


def _search(
    client: Client, namespace: str, rank_by: str, top_k: int, filters: str | None
) -> list[dict[str, Any]]:
    try:
        rank = json.loads(rank_by)
        where = json.loads(filters) if filters is not None else None
    except json.JSONDecodeError as e:
        raise ValueError(f"tpuf_search: rank_by and filters must be JSON ({e})") from None
    hits = client.query(namespace, rank, top_k=top_k, filters=where).to_pylist()
    return [{"id": str(h["id"]), "rank": h["rank"], "score": h["score"]} for h in hits]
