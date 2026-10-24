"""The sample data in data/ hangs together, and every example has a test (offline)."""

from conftest import ROOT, SAMPLE_SQL


def test_sample_sizes(sample_con):
    counts = [
        sample_con.execute(f"SELECT count(*) FROM {t}").fetchall()[0][0]
        for t in ("products", "queries", "judgments")
    ]
    assert counts == [843, 24, 543]


def test_every_graded_product_is_in_the_corpus(sample_con):
    assert sample_con.execute(
        "SELECT count(*) FROM judgments WHERE doc_id NOT IN (SELECT id FROM products)"
    ).fetchall() == [(0,)]


def test_every_query_has_three_exact_products(sample_con):
    assert sample_con.execute("""
        SELECT count(*) FROM queries q
        WHERE (SELECT count(*) FROM judgments j WHERE j.query_id = q.query_id AND j.grade = 3) < 3
    """).fetchall() == [(0,)]


def test_ids_are_unique_and_text_is_never_empty(sample_con):
    assert sample_con.execute(f"""
        SELECT count(*) = count(DISTINCT id), count(*) FILTER (WHERE text = '') FROM ({SAMPLE_SQL})
    """).fetchall() == [(True, 0)]


def test_examples_are_all_tested():
    # test_examples.py runs these live; this check runs offline so a new example can't slip in untested.
    tested = {"01_search.py", "02_evaluate.py", "03_load_your_data.py", "04_export.py", "05_cost_report.py"}
    assert {p.name for p in (ROOT / "examples").glob("[0-9]*.py")} == tested
