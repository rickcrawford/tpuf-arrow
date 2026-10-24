"""Offline tests for the SQL functions that register() adds."""

import json

import duckdb
import pytest

import tpuf_arrow as ta

TYPES = {"id": "string", "brand": "string"}


@pytest.fixture
def sql(fake, client, con):
    fake.seed("ns", [{"id": f"p{i}", "brand": "acme" if i % 2 else "other"} for i in range(6)], TYPES)
    ta.register(con, client)
    return con


def test_tpuf_search_returns_ranked_hits(sql):
    got = sql.sql("""
        SELECT unnest(tpuf_search('ns', '["brand", "BM25", "acme"]', 3, NULL), recursive := true)""")
    assert got.fetchall() == [("p0", 1, 1.0), ("p1", 2, 0.5), ("p2", 3, pytest.approx(1 / 3))]


def test_bm25_and_ann_macros_build_rank_by(sql, fake):
    sql.sql("SELECT tpuf_bm25('ns', 'title', 'red shoes', 5, NULL)").fetchall()
    sql.sql("SELECT tpuf_ann('ns', 'text', 'red shoes', 5, NULL)").fetchall()
    assert fake.calls[0]["rank_by"] == ["title", "BM25", "red shoes"]
    assert fake.calls[1]["rank_by"] == ["text", "ANN", ["Embed", "red shoes"]]
    assert fake.calls[0]["top_k"] == 5


def test_filters_pass_through_as_json(sql, fake):
    hits = sql.sql("""SELECT unnest(tpuf_bm25('ns', 'brand', 'x', 10, json_array('brand', 'Eq', 'acme')))
                      .id""").fetchall()
    assert hits == [("p1",), ("p3",), ("p5",)]
    assert fake.calls[0]["filters"] == ["brand", "Eq", "acme"]


def test_one_request_per_distinct_search_in_a_batch(sql, fake):
    # 300 rows, 3 distinct query strings
    sql.sql("""SELECT tpuf_bm25('ns', 'brand', 'q' || (i % 3), 2, NULL) FROM range(300) r(i)""").fetchall()
    assert len(fake.calls) == 3


def test_null_inputs_give_null_hits(sql, fake):
    got = sql.sql("SELECT tpuf_bm25('ns', 'brand', NULL, 2, NULL) IS NULL").fetchall()
    assert got == [(True,)]
    assert fake.calls == []


def test_lateral_join_over_a_query_table(sql):
    sql.execute("CREATE TABLE qs AS SELECT * FROM (VALUES ('q1', 'acme'), ('q2', 'other')) t(query_id, text)")
    got = sql.sql("""
        SELECT q.query_id, h.id, h.rank
        FROM qs q, LATERAL (SELECT unnest(tpuf_bm25('ns', 'brand', q.text, 2, NULL), recursive := true)) h
        ORDER BY 1, 3
    """).fetchall()
    assert got == [("q1", "p0", 1), ("q1", "p1", 2), ("q2", "p0", 1), ("q2", "p1", 2)]


def test_searches_run_in_parallel_up_to_the_client_limit(fake, con):
    fake.seed("ns", [{"id": "a"}], {"id": "string"})
    fake.delay = 0.01
    client = ta.Client(fake, max_concurrency=3)
    ta.register(con, client)
    con.sql("SELECT tpuf_bm25('ns', 'id', 'q' || i, 1, NULL) FROM range(60) r(i)").fetchall()
    assert len(fake.calls) == 60
    assert fake.max_in_flight == 3


def test_register_twice_replaces_the_functions(fake, con):
    fake.seed("ns", [{"id": "a"}], {"id": "string"})
    first, second = ta.Client(fake), ta.Client(fake)
    ta.register(con, first)
    ta.register(con, second)
    con.sql("SELECT tpuf_bm25('ns', 'id', 'x', 1, NULL)").fetchall()
    assert first.log.num_rows == 0
    assert second.log.num_rows == 1


def test_integer_ids_come_back_as_text(fake, con):
    fake.seed("ns", [{"id": 7}, {"id": 11}], {"id": "uint"})
    ta.register(con, ta.Client(fake))
    got = con.sql("SELECT unnest(tpuf_search('ns', '[\"id\", \"asc\"]', 5, NULL)).id").fetchall()
    assert got == [("7",), ("11",)]


def test_bad_json_is_a_clear_error(sql):
    with pytest.raises(duckdb.Error, match="must be JSON"):
        sql.sql("SELECT tpuf_search('ns', 'not json', 3, NULL)").fetchall()


def test_udf_searches_are_logged(sql, client):
    sql.sql("SELECT tpuf_bm25('ns', 'brand', 'acme', 2, NULL)").fetchall()
    log = client.log.to_pylist()
    assert [r["kind"] for r in log] == ["query"]
    assert json.loads(log[0]["request"])["rank_by"] == ["brand", "BM25", "acme"]
