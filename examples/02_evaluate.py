"""Score two retrievers against human relevance grades with nDCG@10, all in SQL.

Runs every sample query through BM25 and through vector search, joins the
hits to the human grades in data/judgments.parquet, and prints nDCG@10 per
retriever with a 95% bootstrap interval.

  uv run examples/02_evaluate.py
"""

import duckdb

import tpuf_arrow as ta

from _common import DATA, client, parser

args = parser(__doc__).parse_args()
ns = args.namespace

with client() as tp, duckdb.connect() as con:
    ta.register(con, tp)
    for t in ("queries", "judgments"):
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM '{DATA / f'{t}.parquet'}'")

    con.execute(f"""
        CREATE TABLE runs AS
        SELECT 'bm25 (title)' AS run, q.query_id, h.*
        FROM queries q,
             LATERAL (SELECT unnest(tpuf_bm25('{ns}', 'title', q.text, 10, NULL), recursive := true)) h
        UNION ALL
        SELECT 'ann (text)', q.query_id, h.*
        FROM queries q,
             LATERAL (SELECT unnest(tpuf_ann('{ns}', 'text', q.text, 10, NULL), recursive := true)) h
    """)

    # nDCG@10 per query, with 2^grade - 1 gain. Unjudged hits count as 0.
    con.execute("""
        CREATE TABLE per_query AS
        WITH dcg AS (
            SELECT run, r.query_id, sum((pow(2, coalesce(j.grade, 0)) - 1) / log2(r.rank + 1)) AS dcg
            FROM runs r LEFT JOIN judgments j ON j.query_id = r.query_id AND j.doc_id = r.id
            GROUP BY ALL),
        ideal AS (
            SELECT query_id, sum((pow(2, grade) - 1) / log2(pos + 1)) AS idcg
            FROM (SELECT query_id, grade,
                         row_number() OVER (PARTITION BY query_id ORDER BY grade DESC) AS pos
                  FROM judgments)
            WHERE pos <= 10 GROUP BY ALL)
        SELECT run, query_id, coalesce(dcg, 0) / idcg AS ndcg
        FROM ideal CROSS JOIN (SELECT DISTINCT run FROM runs)
        LEFT JOIN dcg USING (run, query_id)
    """)

    # A bootstrap over queries: 1,000 resamples, each the same size as the query set.
    con.sql("""
        WITH q AS (SELECT run, ndcg, row_number() OVER (PARTITION BY run ORDER BY query_id) - 1 AS i,
                          count(*) OVER (PARTITION BY run) AS n FROM per_query),
        draws AS (SELECT b, run, floor(random() * n)::INT AS i
                  FROM range(1000) t(b), (SELECT DISTINCT run, n FROM q), range(n)),
        means AS (SELECT b, run, avg(ndcg) AS m FROM draws JOIN q USING (run, i) GROUP BY ALL)
        SELECT run, round((SELECT avg(ndcg) FROM per_query p WHERE p.run = means.run), 3) AS ndcg_at_10,
               round(quantile_cont(m, 0.025), 3) AS ci_low, round(quantile_cont(m, 0.975), 3) AS ci_high
        FROM means GROUP BY run ORDER BY ndcg_at_10 DESC
    """).show()
