"""Offline tests for the client, against the in-memory fake in conftest.py."""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest

import tpuf_arrow as ta
from tpuf_arrow import ConfigError, SchemaError, UpsertError
from tpuf_arrow.client import Client

TYPES = {"id": "string", "brand": "string", "price": "float", "vec": "[2]f32"}


def rows(n):
    return [
        {"id": f"p{i:04d}", "brand": "acme" if i % 3 == 0 else "other", "price": float(i), "vec": [0.0, 1.0]}
        for i in range(n)
    ]


def ids(n, prefix=""):
    return pa.table({"id": [f"{prefix}{i:06d}" for i in range(n)]})


# connect ---------------------------------------------------------------------


@pytest.fixture
def no_env(monkeypatch, tmp_path):
    monkeypatch.delenv("TURBOPUFFER_API_KEY", raising=False)
    monkeypatch.delenv("TURBOPUFFER_REGION", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_connect_without_key_fails_clearly(no_env):
    with pytest.raises(ConfigError, match="No turbopuffer API key"):
        ta.connect()


def test_connect_ignores_a_dotenv_it_was_not_given(no_env):
    (no_env / ".env").write_text("TURBOPUFFER_API_KEY=tpuf_from_cwd\n")
    with pytest.raises(ConfigError):
        ta.connect()


def test_connect_reads_env_file_without_touching_os_environ(no_env):
    env = no_env / "creds.env"
    env.write_text("TURBOPUFFER_API_KEY=tpuf_from_file\nTURBOPUFFER_REGION=gcp-us-central1\n")
    with ta.connect(env_file=env) as tp:
        assert tp.tpuf.api_key == "tpuf_from_file"
        assert tp.tpuf.region == "gcp-us-central1"
    assert "TURBOPUFFER_API_KEY" not in os.environ


def test_connect_precedence_is_argument_then_environment_then_file(no_env, monkeypatch):
    env = no_env / "creds.env"
    env.write_text("TURBOPUFFER_API_KEY=tpuf_file\n")
    monkeypatch.setenv("TURBOPUFFER_API_KEY", "tpuf_env")
    assert ta.connect(env_file=env).tpuf.api_key == "tpuf_env"
    assert ta.connect("tpuf_arg", env_file=env).tpuf.api_key == "tpuf_arg"
    assert ta.connect("tpuf_arg").tpuf.region == "aws-us-east-1"


def test_connect_with_a_missing_env_file_says_so(no_env):
    with pytest.raises(ConfigError, match="doesn't exist"):
        ta.connect(env_file=no_env / "nope.env")


def test_client_closes_as_a_context_manager(fake):
    with Client(fake) as c:
        assert not fake.closed
    assert fake.closed
    assert repr(c) == "<tpuf_arrow.Client region='fake-region'>"


def test_max_concurrency_must_be_positive(fake):
    with pytest.raises(ValueError, match="max_concurrency"):
        Client(fake, max_concurrency=0)


# scan ------------------------------------------------------------------------


def test_scan_pages_in_id_order_without_gaps(fake, client):
    fake.seed("ns", rows(25), TYPES)
    table = client.scan("ns", page_size=10).read_all()
    assert table.column("id").to_pylist() == [f"p{i:04d}" for i in range(25)]
    assert [c["top_k"] for c in fake.calls] == [10, 10, 10]  # 10, 10, then a short page of 5
    assert fake.calls[1]["filters"] == ("id", "Gt", "p0009")


def test_scan_pages_integer_ids_numerically(fake, client):
    fake.seed("ns", [{"id": i * 7} for i in range(23)], {"id": "uint"})
    got = client.scan("ns", page_size=5).read_all()
    assert got.column("id").to_pylist() == [i * 7 for i in range(23)]
    assert got.schema.field("id").type == pa.uint64()
    assert fake.calls[1]["filters"] == ("id", "Gt", 28)


def test_scan_stops_after_a_full_last_page(fake, client):
    fake.seed("ns", rows(20), TYPES)
    assert client.scan("ns", page_size=10).read_all().num_rows == 20
    assert len(fake.calls) == 3  # the third page comes back empty, and that ends it


def test_scan_combines_caller_filters_with_the_cursor(fake, client):
    fake.seed("ns", rows(30), TYPES)
    t = client.scan("ns", filters=("brand", "Eq", "acme"), page_size=4).read_all()
    assert t.column("brand").to_pylist() == ["acme"] * 10
    assert fake.calls[1]["filters"] == ("And", (("brand", "Eq", "acme"), ("id", "Gt", "p0009")))


def test_scan_columns_default_to_everything_but_vectors(fake, client):
    fake.seed("ns", rows(3), TYPES)
    t = client.scan("ns").read_all()
    assert t.schema.names == ["id", "brand", "price"]
    assert t.schema.field("price").type == pa.float64()


def test_scan_reads_a_vector_when_asked(fake, client):
    fake.seed("ns", rows(2), TYPES)
    t = client.scan("ns", columns=["vec"]).read_all()
    assert t.schema.field("vec").type == pa.list_(pa.float32(), 2)


@pytest.mark.parametrize(("limit", "expected", "top_ks"), [(12, 12, [10, 2]), (0, 0, []), (10, 10, [10])])
def test_scan_limit(fake, client, limit, expected, top_ks):
    fake.seed("ns", rows(25), TYPES)
    assert client.scan("ns", page_size=10, limit=limit).read_all().num_rows == expected
    assert [c["top_k"] for c in fake.calls] == top_ks


def test_scan_empty_namespace_keeps_schema(fake, client):
    fake.seed("ns", [], TYPES)
    t = client.scan("ns", columns=["price"]).read_all()
    assert t.num_rows == 0
    assert t.schema.names == ["id", "price"]


@pytest.mark.parametrize("kwargs", [{"page_size": 0}, {"page_size": 10_001}, {"limit": -1}])
def test_scan_rejects_bad_arguments(fake, client, kwargs):
    fake.seed("ns", [], TYPES)
    with pytest.raises(ValueError, match=r"page_size|limit"):
        client.scan("ns", **kwargs)


def test_scan_columns_must_be_a_list(fake, client):
    fake.seed("ns", [], TYPES)
    with pytest.raises(TypeError):
        client.scan("ns", columns="brand")


def test_scan_reader_is_lazy(fake, client):
    fake.seed("ns", rows(5), TYPES)
    reader = client.scan("ns")
    assert fake.calls == []  # nothing is fetched until DuckDB reads it
    reader.read_all()
    assert len(fake.calls) == 1


def test_duckdb_reads_a_scan(fake, client, con):
    fake.seed("ns", rows(25), TYPES)
    con.register("products", client.scan("ns", page_size=7))
    got = con.sql("SELECT brand, count(*) n, sum(price) FROM products GROUP BY 1 ORDER BY 1").fetchall()
    assert got == [("acme", 9, 108.0), ("other", 16, 192.0)]


# query and count -------------------------------------------------------------


def test_query_returns_rank_and_score_then_attributes(fake, client):
    fake.seed("ns", rows(5), TYPES)
    t = client.query("ns", ("brand", "BM25", "acme"), top_k=3, include=["price"])
    assert t.schema.names == ["id", "rank", "score", "price"]
    assert t.column("rank").to_pylist() == [1, 2, 3]
    assert t.column("score").to_pylist() == [1.0, 0.5, pytest.approx(1 / 3)]
    assert t.column("price").to_pylist() == [0.0, 1.0, 2.0]


@pytest.mark.parametrize("include", [True, ["score"], ["rank", "price"]])
def test_query_refuses_attributes_that_would_overwrite_rank_or_score(fake, client, include):
    fake.seed(
        "ns",
        [{"id": "a", "score": 4.5, "rank": 7, "price": 1.0}],
        {"id": "string", "score": "float", "rank": "int", "price": "float"},
    )
    with pytest.raises(SchemaError, match="would overwrite"):
        client.query("ns", ("title", "BM25", "x"), include=include)
    assert client.scan("ns").read_all().to_pylist() == [{"id": "a", "score": 4.5, "rank": 7, "price": 1.0}]


def test_query_with_no_hits_is_an_empty_typed_table(fake, client):
    fake.seed("ns", rows(5), TYPES)
    t = client.query("ns", ("brand", "BM25", "x"), filters=("brand", "Eq", "nobody"), include=True)
    assert t.num_rows == 0
    assert t.schema.names == ["id", "rank", "score", "brand", "price"]


def test_query_passes_rerank_by_and_takes_the_models_order(fake, client):
    fake.seed("ns", rows(5), TYPES)
    model = {"model": "voyage/rerank-3", "attribute": "brand", "query": "acme"}
    t = client.query("ns", ("brand", "BM25", "acme"), top_k=3, rerank_by=model)
    assert fake.calls[-1]["rerank_by"] == model
    assert fake.calls[-1]["include_attributes"] == ["brand"]  # turbopuffer needs it to rerank
    assert t.schema.names == ["id", "rank", "score"]  # but it wasn't asked for
    assert t.column("id").to_pylist() == ["p0002", "p0001", "p0000"]
    assert t.column("rank").to_pylist() == [1, 2, 3]


def test_multi_query_without_rerank_by_returns_a_table_per_query(fake, client):
    fake.seed("ns", rows(5), TYPES)
    a, b = client.multi_query(
        "ns",
        [{"rank_by": ("brand", "BM25", "x"), "top_k": 2}, {"rank_by": ("id", "asc"), "top_k": 3}],
        include=["price"],
    )
    assert a.schema.names == b.schema.names == ["id", "rank", "score", "price"]
    assert (a.num_rows, b.num_rows) == (2, 3)
    assert all(q["include_attributes"] == ["price"] for q in fake.calls[-1]["queries"])


def test_multi_query_with_rrf_returns_one_fused_table(fake, client):
    fake.seed("ns", rows(5), TYPES)
    queries = [
        {"rank_by": ("brand", "BM25", "x"), "top_k": 4},
        {"rank_by": ("price", "ANN", [0.1]), "top_k": 4},
    ]
    (fused,) = client.multi_query("ns", queries, rerank_by=("RRF", {}), limit=3, run_id="r", query_id="q")
    assert fused.num_rows == 3
    assert fused.column("rank").to_pylist() == [1, 2, 3]
    log = client.log.to_pylist()[-1]
    assert (log["kind"], log["rows"], log["run_id"], log["query_id"]) == ("multi_query", 3, "r", "q")
    assert '"rerank_by": ["RRF", {}]' in log["request"]


def test_multi_query_with_a_model_reranks_the_union_and_includes_its_attribute(fake, client):
    fake.seed("ns", rows(6), TYPES)
    model = {"model": "voyage/rerank-3", "attribute": "brand", "query": "x"}
    queries = [{"rank_by": ("id", "asc"), "top_k": 3}, {"rank_by": ("price", "ANN", [0.1]), "top_k": 4}]
    (merged,) = client.multi_query("ns", queries, rerank_by=model, include=["price"])
    assert merged.schema.names == ["id", "rank", "score", "price"]
    assert merged.num_rows == 4  # the union of both queries' hits
    assert [q["include_attributes"] for q in fake.calls[-1]["queries"]] == [["price", "brand"]] * 2
    assert "include_attributes" not in queries[0]  # the caller's dicts aren't changed


@pytest.mark.parametrize(
    ("queries", "kwargs", "match"),
    [
        ([], {}, "at least one"),
        ([{"rank_by": ("id", "asc")}] * 5, {}, "max_concurrency is 4"),
        ([{"rank_by": ("id", "asc"), "include_attributes": True}], {}, "include_attributes; pass include="),
        ([{"rank_by": ("id", "asc"), "exclude_attributes": ["brand"]}], {}, "exclude_attributes"),
        ([{"aggregate_by": {"n": ("Count",)}}], {}, "use count"),
        ([{"rank_by": ("id", "asc")}], {"limit": 3}, "needs rerank_by"),
    ],
)
def test_multi_query_rejects_what_it_cant_return(fake, client, queries, kwargs, match):
    fake.seed("ns", rows(2), TYPES)
    with pytest.raises(ValueError, match=match):
        client.multi_query("ns", queries, **kwargs)
    assert fake.calls == []


def test_multi_query_takes_a_slot_per_query(fake):
    fake.seed("ns", rows(3), TYPES)
    fake.delay = 0.05
    client = Client(fake, max_concurrency=4)
    two = [{"rank_by": ("id", "asc")}, {"rank_by": ("id", "asc")}]
    with ThreadPoolExecutor(6) as pool:
        list(pool.map(lambda _: client.multi_query("ns", two), range(6)))
    assert fake.max_in_flight <= 2  # each multi_query holds 2 of the 4 slots


def test_count(fake, client):
    fake.seed("ns", rows(30), TYPES)
    assert client.count("ns") == 30
    assert client.count("ns", filters=("brand", "Eq", "acme")) == 10
    assert fake.calls[0]["aggregate_by"] == {"n": ("Count",)}


# concurrency -----------------------------------------------------------------


def test_threads_sharing_a_client_stay_under_max_concurrency(fake):
    fake.seed("ns", rows(3), TYPES)
    fake.delay = 0.02
    client = Client(fake, max_concurrency=3)
    with ThreadPoolExecutor(12) as pool:
        list(pool.map(lambda i: client.query("ns", ("brand", "BM25", str(i))), range(36)))
    assert fake.max_in_flight == 3  # the cap held, and requests did overlap up to it


def test_counts_take_four_slots(fake):
    fake.seed("ns", rows(3), TYPES)
    fake.delay = 0.02
    client = Client(fake, max_concurrency=8)
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda _: client.count("ns"), range(16)))
    assert fake.max_in_flight == 2  # 8 slots / 4 per aggregation


def test_mixed_counts_and_queries_do_not_deadlock(fake):
    fake.seed("ns", rows(3), TYPES)
    fake.delay = 0.005
    client = Client(fake, max_concurrency=5)

    def work(i):
        return client.count("ns") if i % 2 else client.query("ns", ("id", "asc")).num_rows

    done = threading.Event()

    def run():
        with ThreadPoolExecutor(10) as pool:
            list(pool.map(work, range(60)))
        done.set()

    threading.Thread(target=run, daemon=True).start()
    assert done.wait(timeout=10), "requests deadlocked"
    assert fake.max_in_flight <= 5


# upsert ----------------------------------------------------------------------


def test_upsert_from_a_duckdb_relation(fake, client, con):
    rel = con.sql("SELECT 'd' || i AS doc_id, i::DOUBLE AS price, ['a', 'b'] AS tags FROM range(10) r(i)")
    stats = client.upsert("new", rel, id_column="doc_id")
    assert (stats.rows, stats.requests, stats.errors) == (10, 1, [])
    assert sorted(fake.data["new"]["rows"]) == sorted(f"d{i}" for i in range(10))
    assert fake.writes[0]["schema"] == {"price": {"type": "float"}, "tags": {"type": "[]string"}}
    assert con.sql("SELECT 1").fetchall() == [(1,)]  # the connection is free afterward


@pytest.mark.parametrize(
    ("sql", "error"),
    [
        ("SELECT 1 AS doc", SchemaError),  # no id column
        ("SELECT 'a' AS id, [1.0, 2.0] AS emb", SchemaError),  # a decimal list it won't guess about
        ("SELECT 'a' AS id, [1.0, 2.0]::FLOAT[] AS emb", SchemaError),  # a float list
    ],
)
def test_a_failed_upsert_leaves_the_duckdb_connection_usable(fake, client, con, sql, error):
    with pytest.raises(error):
        client.upsert("ns", con.sql(sql))
    assert fake.writes == []  # refused before anything was sent
    assert con.sql("SELECT 42").fetchall() == [(42,)]


def test_a_turbopuffer_error_mid_upsert_leaves_the_connection_usable(fake, client, con):
    fake.fail_writes.append(lambda cols: True)
    with pytest.raises(UpsertError):
        client.upsert("ns", con.sql("SELECT i::VARCHAR AS id FROM range(10) r(i)"))
    assert con.sql("SELECT 42").fetchall() == [(42,)]


def test_upsert_does_not_retype_existing_attributes(fake, client):
    fake.seed("ns", [], {"id": "string", "title": "string"})
    client.upsert("ns", pa.table({"id": ["a"], "title": ["t"], "n": [1]}))
    assert fake.writes[0]["schema"] == {"n": {"type": "int"}}


def test_upsert_explicit_schema_wins(fake, client):
    client.upsert(
        "ns",
        pa.table({"id": ["a"], "n": [1], "emb": [[0.5, 0.5]]}),
        schema={"n": {"type": "float"}, "emb": {"type": "[2]f32", "ann": True}},
    )
    assert fake.writes[0]["schema"] == {"n": {"type": "float"}, "emb": {"type": "[2]f32", "ann": True}}


def test_upsert_sends_values_strict_json_accepts(fake, client, con):
    rel = con.sql("""SELECT 'x' AS id, 1.5::DECIMAL(4,2) AS d, 12345678901234567890::HUGEINT AS h,
                            'nan'::DOUBLE AS f, TIMESTAMPTZ '2026-01-01 00:00:00+00' AS t,
                            'red'::ENUM('red', 'blue') AS e""")
    client.upsert("ns", rel)
    assert fake.data["ns"]["rows"]["x"] == {
        "id": "x",
        "d": 1.5,
        "h": 12345678901234567890,
        "f": None,
        "t": "2026-01-01T00:00:00+00:00",
        "e": "red",
    }
    assert fake.writes[0]["schema"] == {
        "d": {"type": "float"},
        "h": {"type": "int"},
        "f": {"type": "float"},
        "t": {"type": "datetime"},
        "e": {"type": "string"},
    }


def test_upsert_caps_batches_at_256_rows_when_embedding(fake, client):
    t = pa.table({"id": [str(i) for i in range(600)], "text": ["x"] * 600})
    client.upsert("ns", t, schema={"text": {"type": "string", "embed": "voyage/voyage-4-lite"}})
    assert sorted(w["rows"] for w in fake.writes) == [88, 256, 256]


def test_upsert_caps_batches_when_the_namespace_already_embeds(fake, client):
    fake.seed("ns", [], {"id": "string", "text": "string"})
    fake.data["ns"]["schema"]["text"].embed = "voyage/voyage-4-lite"
    client.upsert("ns", pa.table({"id": [str(i) for i in range(300)], "text": ["x"] * 300}))
    assert sorted(w["rows"] for w in fake.writes) == [44, 256]


def test_max_rows_cannot_raise_the_embedding_cap(fake, client):
    t = pa.table({"id": [str(i) for i in range(300)], "text": ["x"] * 300})
    client.upsert("ns", t, max_rows=300, schema={"text": {"type": "string", "embed": "voyage/voyage-4-lite"}})
    assert max(w["rows"] for w in fake.writes) == 256


def test_upsert_merges_small_source_batches(fake, client):
    batches = [
        pa.record_batch({"id": [f"{j}-{i}" for i in range(100)], "body": ["y" * 200] * 100})
        for j in range(50)
    ]
    stats = client.upsert("ns", pa.Table.from_batches(batches))
    assert stats.rows == 5000
    assert stats.requests == 1


def test_upsert_sizes_requests_by_their_json(fake, client):
    # Vectors are 4 bytes a float in Arrow and about 20 as JSON text.
    t = pa.table(
        {
            "id": [f"{i:06d}" for i in range(2000)],
            "vector": pa.array([[0.123456] * 256] * 2000, pa.list_(pa.float32(), 256)),
        }
    )
    client.upsert("ns", t, batch_bytes=1_000_000, distance_metric="cosine_distance")
    sizes = [w["bytes"] for w in fake.writes]
    assert len(sizes) > 2
    assert max(sizes) < 1_300_000


def test_upsert_stops_after_a_client_error_and_reports_what_failed(fake, client):
    fake.fail_writes.append(lambda cols: cols["id"][0] == "000020")
    with pytest.raises(UpsertError, match="stopped after a client error") as err:
        client.upsert("ns", ids(100), max_rows=10, workers=1)
    stats = err.value.stats
    assert stats.rows == 20
    assert stats.requests == 2
    assert stats.errors == ["ids 000020 to 000029: BadRequestError: fake: refused"]
    assert len(fake.writes) == 3  # nothing was sent after the 400


def test_upsert_keeps_going_after_a_transient_error(fake, client, monkeypatch):
    real = type(fake.namespace("ns")).write
    seen = []

    def flaky(self, **kw):
        seen.append(1)
        if len(seen) == 2:
            raise ConnectionError("boom")
        return real(self, **kw)

    monkeypatch.setattr(type(fake.namespace("ns")), "write", flaky)
    with pytest.raises(UpsertError, match="finished: 1 request") as err:
        client.upsert("ns", ids(30), max_rows=10, workers=1)
    assert err.value.stats.rows == 20
    assert len(fake.data["ns"]["rows"]) == 20
    assert "ConnectionError: boom" in err.value.stats.errors[0]


def test_upsert_writes_stay_under_max_concurrency(fake):
    fake.delay = 0.02
    client = Client(fake, max_concurrency=2)
    client.upsert("ns", ids(200), max_rows=10, workers=6)
    assert fake.max_in_flight == 2


def test_upsert_does_not_read_far_ahead_of_its_writes(fake):
    fake.delay = 0.01
    client = Client(fake, max_concurrency=8)
    pulled = []
    written = []
    real = type(fake.namespace("ns")).write

    def counting(self, **kw):
        written.append(len(pulled) - len(written))
        return real(self, **kw)

    type(fake.namespace("ns")).write = counting
    try:

        def source():
            for j in range(40):
                pulled.append(j)
                yield pa.record_batch({"id": [f"{j}-{i}" for i in range(10)]})

        reader = pa.RecordBatchReader.from_batches(pa.schema([("id", pa.string())]), source())
        client.upsert("ns", reader, max_rows=10, workers=2)
    finally:
        type(fake.namespace("ns")).write = real
    assert len(pulled) == 40
    assert max(written) <= 2 * 2 + 1


@pytest.mark.parametrize("kwargs", [{"batch_bytes": 0}, {"workers": 0}, {"max_rows": 0}])
def test_upsert_rejects_bad_arguments(client, kwargs):
    with pytest.raises(ValueError, match="at least 1"):
        client.upsert("ns", ids(1), **kwargs)


# request log ------------------------------------------------------------------


def test_requests_are_logged_and_saved(fake, client, con):
    fake.seed("ns", rows(15), TYPES)
    client.scan("ns", page_size=10).read_all()
    client.query("ns", ("brand", "BM25", "acme"), top_k=2, run_id="r1", query_id="q1")
    assert client.log.num_rows == 3
    assert client.save_log(con) == 3
    assert client.log.num_rows == 0
    got = con.execute(
        "SELECT kind, rows, run_id, query_id, billable_bytes_queried FROM tpuf_requests ORDER BY requested_at"
    ).fetchall()
    assert got == [
        ("scan", 10, None, None, 1_280_000_000),
        ("scan", 5, None, None, 1_280_000_000),
        ("query", 2, "r1", "q1", 1_280_000_000),
    ]
    client.count("ns")
    assert client.save_log(con) == 1  # appends to the existing table
    assert con.execute("SELECT count(*) FROM tpuf_requests").fetchall() == [(4,)]


def test_save_log_keeps_the_log_when_the_insert_fails(fake, client, con):
    fake.seed("ns", rows(1), TYPES)
    client.count("ns")
    con.execute("CREATE TABLE tpuf_requests (unrelated INTEGER NOT NULL)")
    with pytest.raises(SchemaError, match="none of the request log's columns"):
        client.save_log(con)
    con.execute("CREATE TABLE strict (kind VARCHAR, rows INTEGER CHECK (rows > 100))")
    with pytest.raises(Exception, match="CHECK"):
        client.save_log(con, "strict")
    assert client.log.num_rows == 1


def test_save_log_matches_columns_by_name(fake, client, con):
    fake.seed("ns", rows(1), TYPES)
    client.count("ns", run_id="r9")
    con.execute("CREATE TABLE audit (note VARCHAR, run_id VARCHAR, kind VARCHAR)")
    client.save_log(con, "audit")
    assert con.execute("SELECT note, run_id, kind FROM audit").fetchall() == [(None, "r9", "count")]


def test_logged_request_is_json_and_skips_attribute_lists(fake, client):
    fake.seed("ns", rows(1), TYPES)
    client.query("ns", ("brand", "BM25", "acme"), filters=("price", "Gte", 1), include=True)
    assert client.log.column("request")[0].as_py() == (
        '{"rank_by": ["brand", "BM25", "acme"], "top_k": 10, "filters": ["price", "Gte", 1]}'
    )
    client.multi_query("ns", [{"rank_by": ("id", "asc")}], include=True)
    assert client.log.column("request")[1].as_py() == '{"queries": [{"rank_by": ["id", "asc"]}]}'


def test_log_never_contains_the_api_key(no_env):
    tp = ta.connect("tpuf_secret_value_123")
    tp.tpuf = __import__("conftest").FakeTurbopuffer()
    tp.tpuf.seed("ns", rows(2), TYPES)
    tp.query("ns", ("brand", "BM25", "x"))
    assert "secret" not in str(tp.log.to_pylist())
    assert "secret" not in repr(tp)


def test_enable_logging_adds_one_handler(fake, client, capsys):
    logger = logging.getLogger("tpuf_arrow")
    before = list(logger.handlers)
    try:
        ta.enable_logging()
        ta.enable_logging("DEBUG")
        assert len(logger.handlers) == len(before) + 1
        assert logger.level == logging.DEBUG
        fake.seed("ns", rows(2), TYPES)
        client.query("ns", ("brand", "BM25", "x"))
        assert "query ns: 2 rows" in capsys.readouterr().err
    finally:
        logger.handlers[:] = before
        logger.setLevel(logging.NOTSET)


# DuckDB helpers and namespaces ----------------------------------------------------


def test_copy_to(fake, client, con):
    fake.seed("ns", rows(12), TYPES)
    assert client.copy_to(con, "ns", "main.products", page_size=5) == 12
    assert con.execute("SELECT count(*) FROM main.products").fetchall() == [(12,)]
    assert client.copy_to(con, "ns", "products", filters=("brand", "Eq", "acme")) == 4  # replaces


def test_table_names_are_quoted(fake, client, con):
    fake.seed("ns", rows(3), TYPES)
    name = 'we"ird; DROP TABLE x; --'
    assert client.copy_to(con, "ns", name) == 3
    client.save_log(con, 'log "table"')
    assert con.execute('SELECT count(*) FROM "log ""table"""').fetchall() == [(1,)]


def test_namespaces_and_metadata(fake, client):
    fake.seed("a-1", rows(2), TYPES)
    fake.seed("b-1", rows(3), TYPES)
    assert client.namespaces().column("namespace").to_pylist() == ["a-1", "b-1"]
    assert client.namespaces(prefix="b").column("namespace").to_pylist() == ["b-1"]
    m = client.metadata("b-1")
    assert m["approx_row_count"] == 3
    assert m["schema"]["vec"] == "[2]f32"


def test_delete_namespace_is_idempotent(fake, client):
    fake.seed("ns", rows(1), TYPES)
    assert client.delete_namespace("ns") is True
    assert client.delete_namespace("ns") is False


def test_attr_types_are_cached_until_a_write(fake, client):
    fake.seed("ns", rows(1), TYPES)
    client.attr_types("ns")
    fake.data["ns"]["schema"]["extra"] = type(fake.data["ns"]["schema"]["id"])(type="int", embed=None)
    assert "extra" not in client.attr_types("ns")
    assert "extra" in client.attr_types("ns", refresh=True)
    client.upsert("ns", pa.table({"id": ["z"]}))
    assert client.attr_types("ns")["extra"] == "int"
