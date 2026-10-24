"""Run every example against the session's sample namespace, the way a reader would."""

import subprocess
import sys

import duckdb
import pytest

from conftest import ROOT

pytestmark = pytest.mark.live
EXAMPLES = ROOT / "examples"


def run(script: str, *args: str) -> str:
    done = subprocess.run(
        [sys.executable, str(EXAMPLES / script), *args],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout


def test_01_search(sample_ns):
    out = run("01_search.py", "--namespace", sample_ns)
    assert "Champion" in out
    assert "watch bands for garmin vivoactive 3" in out


def test_02_evaluate(sample_ns):
    out = run("02_evaluate.py", "--namespace", sample_ns)
    assert "ann (text)" in out
    assert "bm25 (title)" in out
    assert "ci_low" in out


def test_03_load_your_data(live, scratch_ns):
    ns = scratch_ns("example03")
    out = run("03_load_your_data.py", "--target", ns)
    assert f"{ns} now has 843 rows" in out


def test_04_export(sample_ns, tmp_path):
    out_file = tmp_path / "garmin.parquet"
    out = run("04_export.py", "--namespace", sample_ns, "--out", str(out_file), "--brand", "Garmin")
    assert "wrote 2 rows" in out
    with duckdb.connect() as con:
        assert con.execute("SELECT DISTINCT brand FROM read_parquet(?)", [str(out_file)]).fetchall() == [
            ("Garmin",)
        ]


def test_05_cost_report(sample_ns, tmp_path):
    db = tmp_path / "lab.duckdb"
    run("05_cost_report.py", "--namespace", sample_ns, "--db", str(db))
    run("05_cost_report.py", "--namespace", sample_ns, "--db", str(db))  # the history accumulates
    with duckdb.connect(str(db)) as con:
        assert con.execute("SELECT kind, count(*) FROM tpuf_requests GROUP BY 1 ORDER BY 1").fetchall() == [
            ("query", 48),
            ("scan", 2),
        ]
