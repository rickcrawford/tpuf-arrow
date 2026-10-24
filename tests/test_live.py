"""Live tests against turbopuffer, using the sample data in data/.

The session loads the 843 sample products into a fresh namespace and deletes
it at the end. Each query bills turbopuffer's 1.28 GB minimum, about a
millionth of a dollar, so the whole module costs well under a cent.
"""

import datetime as dt

import pyarrow as pa
import pytest
from turbopuffer import AuthenticationError, BadRequestError, PermissionDeniedError

import tpuf_arrow as ta

pytestmark = pytest.mark.live


def ndcg10(con, run: str) -> float:
    """Mean nDCG@10 over the sample queries, with 2^grade - 1 gain and unjudged hits as 0."""
    return con.execute(f"""
        WITH dcg AS (
            SELECT r.query_id, sum((pow(2, coalesce(j.grade, 0)) - 1) / log2(r.rank + 1)) AS dcg
            FROM {run} r LEFT JOIN judgments j ON j.query_id = r.query_id AND j.doc_id = r.id
            WHERE r.rank <= 10 GROUP BY 1),
        ideal AS (
            SELECT query_id, sum((pow(2, grade) - 1) / log2(pos + 1)) AS idcg
            FROM (SELECT query_id, grade, row_number() OVER (PARTITION BY query_id ORDER BY grade DESC) pos
                  FROM judgments)
            WHERE pos <= 10 GROUP BY 1)
        SELECT avg(coalesce(dcg, 0) / idcg) FROM ideal LEFT JOIN dcg USING (query_id)
    """).fetchall()[0][0]


def test_load_counts_match(live, sample_ns):
    assert live.count(sample_ns) == 843
    assert live.namespaces(prefix=sample_ns).column("namespace").to_pylist() == [sample_ns]


def test_scan_matches_the_parquet_file(live, sample_ns, sample_con):
    # 100-row pages make this nine requests, so paging is exercised for real.
    scanned = live.scan(sample_ns, columns=["title", "body", "brand", "color"], page_size=100)
    sample_con.register("scanned", scanned)
    sample_con.execute("CREATE TABLE got AS SELECT * FROM scanned")
    missing = sample_con.execute(
        "SELECT count(*) FROM (SELECT id, title, body, brand, color FROM products EXCEPT SELECT * FROM got)"
    ).fetchall()[0][0]
    extra = sample_con.execute(
        "SELECT count(*) FROM (SELECT * FROM got EXCEPT SELECT id, title, body, brand, color FROM products)"
    ).fetchall()[0][0]
    assert (missing, extra) == (0, 0)
    assert sample_con.execute("SELECT count(*), count(DISTINCT id) FROM got").fetchall() == [(843, 843)]


def test_scan_filter_is_pushed_down(live, sample_ns, sample_con):
    ((brand, n),) = sample_con.execute(
        "SELECT brand, count(*) FROM products WHERE brand IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1"
    ).fetchall()
    before = live.log.num_rows
    got = live.scan(sample_ns, filters=("brand", "Eq", brand), columns=["brand"]).read_all()
    assert got.num_rows == n
    assert set(got.column("brand").to_pylist()) == {brand}
    log = live.log.slice(before).to_pylist()
    assert len(log) == 1
    assert log[0]["rows"] == n


def test_copy_to_duckdb(live, sample_ns, sample_con):
    assert live.copy_to(sample_con, sample_ns, "tp_products", columns=["brand"]) == 843


def test_bm25_and_ann_find_graded_products(live, sample_ns, sample_con):
    ta.register(sample_con, live)
    for name, fn, attr in [("bm25_run", "tpuf_bm25", "title"), ("ann_run", "tpuf_ann", "text")]:
        sample_con.execute(f"""
            CREATE TABLE {name} AS
            SELECT q.query_id, h.id, h.rank, h.score
            FROM queries q, LATERAL (SELECT unnest({fn}('{sample_ns}', '{attr}', q.text, 10, NULL),
                                                    recursive := true)) h
        """)
    # ANN always fills top_k. BM25 returns only products that match a query
    # term, and "makeup eraser" matches 9 titles.
    counts = "SELECT count(DISTINCT query_id), count(*) FROM {}"
    assert sample_con.execute(counts.format("ann_run")).fetchall() == [(24, 240)]
    ((queries, hits),) = sample_con.execute(counts.format("bm25_run")).fetchall()
    assert queries == 24
    assert 200 <= hits <= 240
    # On 2026-10-24 this sample scored 0.67 (BM25 on title) and 0.76 (ANN).
    # The floors catch a broken pipeline, not a small quality change.
    assert ndcg10(sample_con, "bm25_run") > 0.5
    assert ndcg10(sample_con, "ann_run") > 0.5


def test_udf_and_client_agree(live, sample_ns, sample_con):
    ta.register(sample_con, live)
    text = sample_con.execute("SELECT text FROM queries ORDER BY query_id LIMIT 1").fetchall()[0][0]
    direct = live.query(sample_ns, ("title", "BM25", text), top_k=10).column("id").to_pylist()
    via_sql = [
        r[0]
        for r in sample_con.execute(
            "SELECT unnest(tpuf_bm25(?, 'title', ?, 10, NULL)).id", [sample_ns, text]
        ).fetchall()
    ]
    assert via_sql == direct
    assert len(direct) == 10


def test_filtered_search_respects_the_filter(live, sample_ns, sample_con):
    # RuenTech makes four of the sample's watch bands; unfiltered, other brands rank too.
    ta.register(sample_con, live)
    search = """
        SELECT p.brand
        FROM (SELECT unnest(tpuf_bm25(?, 'body', 'watch band', 20, {}), recursive := true)) h
        JOIN products p USING (id)
    """
    filtered = sample_con.execute(
        search.format("json_array('brand', 'Eq', 'RuenTech')"), [sample_ns]
    ).fetchall()
    unfiltered = sample_con.execute(search.format("NULL"), [sample_ns]).fetchall()
    assert len(filtered) >= 4
    assert set(filtered) == {("RuenTech",)}
    assert len(set(unfiltered)) > 1


def test_query_includes_attributes(live, sample_ns):
    t = live.query(sample_ns, ("title", "BM25", "dehumidifier"), top_k=3, include=True)
    assert t.schema.names[:3] == ["id", "rank", "score"]
    assert {"title", "body", "brand", "color", "text"} <= set(t.schema.names)
    assert "embed_text" not in t.schema.names  # vectors stay out unless asked for
    assert t.num_rows == 3
    assert all("ehumidifier" in s.lower() for s in t.column("title").to_pylist())


def test_hybrid_multi_query_fuses_bm25_and_ann(live, sample_ns):
    q = "running shoes"
    bm25 = {"rank_by": ("title", "BM25", q), "top_k": 20}
    ann = {"rank_by": ("text", "ANN", ("Embed", q)), "top_k": 20}
    separate = live.multi_query(sample_ns, [bm25, ann])
    assert [t.num_rows for t in separate] == [20, 20]
    (fused,) = live.multi_query(sample_ns, [bm25, ann], rerank_by=("RRF", {}), limit=10, include=["title"])
    assert fused.schema.names == ["id", "rank", "score", "title"]
    assert fused.num_rows == 10
    pooled = set(separate[0].column("id").to_pylist()) | set(separate[1].column("id").to_pylist())
    assert set(fused.column("id").to_pylist()) <= pooled
    assert live.log.column("kind").to_pylist()[-1] == "multi_query"


def test_hosted_reranker_reorders_a_fixed_candidate_set(live, sample_ns):
    q = "running shoes"
    ids = live.query(sample_ns, ("text", "ANN", ("Embed", q)), top_k=10).column("id").to_pylist()
    model = {"model": "voyage/rerank-3-lite", "attribute": "text", "query": q}
    try:
        t = live.query(sample_ns, ("id", "asc"), top_k=10, filters=("id", "In", ids), rerank_by=model)
    except BadRequestError as e:
        if "not enabled" in str(e):
            pytest.skip("voyage/rerank-3-lite isn't enabled for this org")
        raise
    assert sorted(t.column("id").to_pylist()) == sorted(ids)
    scores = t.column("score").to_pylist()
    assert scores == sorted(scores, reverse=True)  # the model's relevance, best first


def test_request_log_records_billing(live, sample_ns, sample_con):
    live.query(sample_ns, ("title", "BM25", "hoodie"), top_k=5, run_id="live-log", query_id="q")
    live.save_log(sample_con)
    row = sample_con.execute("""
        SELECT rows, billable_bytes_queried, server_ms, last_included_write_at
        FROM tpuf_requests WHERE run_id = 'live-log'
    """).fetchall()
    assert len(row) == 1
    rows, billed, server_ms, write_at = row[0]
    assert rows == 5
    assert billed >= 1_280_000_000
    assert server_ms > 0
    assert write_at


def test_upsert_round_trips_types(live, scratch_ns):
    ns = scratch_ns("types")
    when = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    t = pa.table(
        {
            "id": ["a", "b"],
            "n": [1, -2],
            "u": pa.array([1, 2], pa.uint64()),
            "price": [2.0, 1.5],
            "ok": [True, False],
            "tags": [["x"], []],
            "seen": [when, when],
        }
    )
    stats = live.upsert(ns, t)
    assert stats.rows == 2
    assert stats.billable_bytes > 0
    assert live.attr_types(ns) == {
        "id": "string",
        "n": "int",
        "u": "uint",
        "price": "float",
        "ok": "bool",
        "tags": "[]string",
        "seen": "datetime",
    }
    got = sorted(live.scan(ns).read_all().to_pylist(), key=lambda r: r["id"])
    assert got[0] | {"seen": None} == {
        "id": "a",
        "n": 1,
        "u": 1,
        "price": 2.0,
        "ok": True,
        "tags": ["x"],
        "seen": None,
    }
    assert got[1]["price"] == 1.5
    assert got[1]["tags"] == []
    assert dt.datetime.fromisoformat(got[0]["seen"]) == when


def test_upsert_into_existing_namespace_keeps_full_text_search(live, scratch_ns):
    ns = scratch_ns("fts")
    live.upsert(
        ns,
        pa.table({"id": ["a"], "title": ["red running shoes"]}),
        schema={"title": {"type": "string", "full_text_search": True}},
    )
    live.upsert(ns, pa.table({"id": ["b"], "title": ["blue running shoes"], "size": [10]}))
    hits = live.query(ns, ("title", "BM25", "running"), top_k=10).column("id").to_pylist()
    assert sorted(hits) == ["a", "b"]


def test_bad_key_error_does_not_leak_the_key():
    secret = "tpuf_definitely_not_a_real_key_0123456789"
    tp = ta.connect(api_key=secret, max_retries=0)
    with pytest.raises((AuthenticationError, PermissionDeniedError)) as err:
        tp.count("anything")
    assert secret not in str(err.value)
    assert secret not in repr(err.value)


def test_edge_case_types_round_trip(live, scratch_ns, sample_con):
    ns = scratch_ns("edge")
    rel = sample_con.sql("""
        SELECT 'a' AS id, [true, false] AS flags, 'nan'::DOUBLE AS x,
               12345678901234567::HUGEINT AS big, 'red'::ENUM('red', 'blue') AS color
    """)
    live.upsert(ns, rel)
    assert live.attr_types(ns) == {
        "id": "string",
        "flags": "[]bool",
        "x": "float",
        "big": "int",
        "color": "string",
    }
    assert live.scan(ns).read_all().to_pylist() == [
        {"id": "a", "flags": [True, False], "x": None, "big": 12345678901234567, "color": "red"}
    ]


def test_float_lists_need_an_explicit_type(live, scratch_ns, sample_con):
    ns = scratch_ns("vec")
    rel = sample_con.sql("SELECT 'a' AS id, [0.1, 0.9]::FLOAT[] AS emb")
    with pytest.raises(ta.SchemaError, match="list of floats"):
        live.upsert(ns, rel)
    assert live.namespaces(prefix=ns).num_rows == 0  # nothing was written
    live.upsert(ns, rel, schema={"emb": {"type": "[2]f32", "ann": True}}, distance_metric="cosine_distance")
    hits = live.query(ns, ("emb", "ANN", [0.1, 0.9]), top_k=1)
    assert hits.column("id").to_pylist() == ["a"]
