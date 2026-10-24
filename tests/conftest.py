"""Fixtures: an in-memory fake of the turbopuffer SDK for offline tests, and a real
namespace loaded with the sample data for live ones.

Live tests need TURBOPUFFER_API_KEY, from the environment or the repo's .env,
and are skipped without it. Run only the offline tests with: uv run pytest -m "not live"
"""

import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import httpx
import pytest
from dotenv import dotenv_values
from turbopuffer import BadRequestError, NotFoundError

import tpuf_arrow as ta
from tpuf_arrow.client import Client

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
ENV_FILE = ROOT / ".env"

EMBED_MODEL = "voyage/voyage-4-lite"
SAMPLE_SCHEMA = {
    "title": {"type": "string", "full_text_search": {"language": "english", "stemming": True}},
    "body": {"type": "string", "full_text_search": {"language": "english", "stemming": True}},
    "text": {"type": "string", "embed": EMBED_MODEL, "filterable": False},
}
SAMPLE_SQL = f"""
    SELECT id, title, body, brand, color,
           concat_ws('. ', title, brand, color, left(body, 300)) AS text
    FROM '{DATA / "products.parquet"}'
"""


def status_error(cls: type, status: int, message: str) -> Exception:
    """An SDK APIStatusError subclass, built the way the SDK builds them."""
    request = httpx.Request("POST", "https://fake.turbopuffer.test")
    return cls(message, response=httpx.Response(status, request=request), body=None)


# Offline fake ---------------------------------------------------------------


class FakeRow(dict):
    def to_dict(self) -> dict:
        return dict(self)


def _matches(row: dict, f: Any) -> bool:
    if f is None:
        return True
    if f[0] == "And":
        return all(_matches(row, g) for g in f[1])
    attr, op, value = f
    v = row.get(attr)
    return {
        "Eq": lambda: v == value,
        "Gt": lambda: v is not None and v > value,
        "Gte": lambda: v is not None and v >= value,
        "In": lambda: v in value,
    }[op]()


class FakeNamespace:
    """Enough of the SDK's namespace API to exercise the driver.

    Where the driver's correctness depends on it, the fake behaves like the
    service: writes go through strict JSON (the SDK serializes with orjson,
    which rejects Decimal, bytes, and NaN), embedded writes over 256 rows get
    a 400, and an unknown namespace gets a 404. Queries rank by id; any other
    rank_by gets a made-up score of 1/position.
    """

    def __init__(self, store: "FakeTurbopuffer", name: str):
        self.store, self.name = store, name

    @property
    def _ns(self) -> dict:
        if self.name not in self.store.data:
            raise status_error(NotFoundError, 404, f"namespace {self.name!r} was not found")
        return self.store.data[self.name]

    def _enter(self) -> None:
        with self.store.lock:
            self.store.in_flight += 1
            self.store.max_in_flight = max(self.store.max_in_flight, self.store.in_flight)
        time.sleep(self.store.delay)

    def _exit(self) -> None:
        with self.store.lock:
            self.store.in_flight -= 1

    def query(self, *, rank_by=None, top_k=10, filters=None, include_attributes=None, aggregate_by=None):
        self.store.calls.append(
            {
                "namespace": self.name,
                "rank_by": rank_by,
                "top_k": top_k,
                "filters": filters,
                "include_attributes": include_attributes,
                "aggregate_by": aggregate_by,
            }
        )
        self._enter()
        try:
            rows = sorted(
                (r for r in self._ns["rows"].values() if _matches(r, filters)), key=lambda r: r["id"]
            )
            if aggregate_by:
                return _resp([], aggregations={"n": len(rows)})
            by_id = rank_by == ("id", "asc")
            keep = include_attributes or []
            return _resp(
                [
                    FakeRow(
                        {
                            "id": r["id"],
                            "$dist": 0.0 if by_id else 1.0 / (i + 1),
                            **{k: r.get(k) for k in keep},
                        }
                    )
                    for i, r in enumerate(rows[:top_k])
                ]
            )
        finally:
            self._exit()

    def write(self, *, upsert_columns, schema=None, distance_metric=None):
        body = json.dumps({"upsert_columns": upsert_columns, "schema": schema}, allow_nan=False)
        self.store.writes.append(
            {"namespace": self.name, "rows": len(upsert_columns["id"]), "schema": schema, "bytes": len(body)}
        )
        self._enter()
        try:
            for fail in self.store.fail_writes:
                if fail(upsert_columns):
                    raise status_error(BadRequestError, 400, "fake: refused")
            ns = self.store.data.setdefault(self.name, {"rows": {}, "schema": {}})
            for k, v in (schema or {}).items():
                ns["schema"][k] = SimpleNamespace(type=v.get("type"), embed=v.get("embed"))
            if len(upsert_columns["id"]) > 256 and any(s.embed for s in ns["schema"].values()):
                raise status_error(BadRequestError, 400, "refusing to embed a large batch of documents")
            names = list(upsert_columns)
            for values in zip(*upsert_columns.values(), strict=True):
                row = dict(zip(names, values, strict=True))
                ns["rows"][row["id"]] = row
            return SimpleNamespace(
                billing=SimpleNamespace(billable_logical_bytes_written=len(body)), performance=None, rows=None
            )
        finally:
            self._exit()

    def schema(self):
        return {k: SimpleNamespace(type=v.type) for k, v in self._ns["schema"].items()}

    def metadata(self):
        return SimpleNamespace(
            schema_=self._ns["schema"],
            approx_row_count=len(self._ns["rows"]),
            approx_logical_bytes=0,
            index=SimpleNamespace(status="up-to-date"),
            created_at=None,
            updated_at=None,
            last_write_at=None,
        )

    def delete_all(self):
        _ = self._ns  # raises NotFoundError, like the real API
        del self.store.data[self.name]


def _resp(rows, aggregations=None):
    return SimpleNamespace(
        rows=rows,
        aggregations=aggregations,
        performance=SimpleNamespace(
            server_total_ms=1.0,
            cache_temperature="hot",
            cache_hit_ratio=1.0,
            approx_namespace_size=len(rows),
            last_included_write_at="t0",
            embedding_tokens=None,
        ),
        billing=SimpleNamespace(
            billable_logical_bytes_queried=1_280_000_000, billable_logical_bytes_returned=10 * len(rows)
        ),
    )


class FakeTurbopuffer:
    region = "fake-region"

    def __init__(self):
        self.data: dict[str, dict] = {}
        self.calls: list[dict] = []
        self.writes: list[dict] = []
        self.fail_writes: list = []  # predicates on upsert_columns; a match gets a 400
        self.delay = 0.0  # seconds each request takes, so concurrency shows up in tests
        self.lock = threading.Lock()
        self.in_flight = self.max_in_flight = 0
        self.closed = False

    def namespace(self, name):
        return FakeNamespace(self, name)

    def namespaces(self, prefix=None, page_size=None):
        return [SimpleNamespace(id=n) for n in sorted(self.data) if not prefix or n.startswith(prefix)]

    def close(self):
        self.closed = True

    def seed(self, name: str, rows: list[dict], types: dict[str, str]):
        self.data[name] = {
            "rows": {r["id"]: r for r in rows},
            "schema": {k: SimpleNamespace(type=t, embed=None) for k, t in types.items()},
        }


@pytest.fixture
def fake():
    return FakeTurbopuffer()


@pytest.fixture
def client(fake):
    return Client(fake, max_concurrency=4)


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


# Live -----------------------------------------------------------------------


def _api_key() -> str | None:
    if key := os.environ.get("TURBOPUFFER_API_KEY"):
        return key
    return dotenv_values(ENV_FILE).get("TURBOPUFFER_API_KEY") if ENV_FILE.is_file() else None


def pytest_collection_modifyitems(config, items):
    if _api_key():
        return
    skip = pytest.mark.skip(reason="TURBOPUFFER_API_KEY not set")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def live():
    with ta.connect(env_file=ENV_FILE if ENV_FILE.is_file() else None) as client:
        yield client


@pytest.fixture(scope="session")
def scratch_ns(live):
    """Makes namespace names that get deleted at the end of the session, whatever was written to them."""
    names = []

    def make(tag: str) -> str:
        names.append(f"tpuf-arrow-test-{tag}-{uuid.uuid4().hex[:8]}")
        return names[-1]

    yield make
    for n in names:
        live.delete_namespace(n)


@pytest.fixture(scope="session")
def sample_ns(live, scratch_ns):
    """The 843 sample products, loaded once per session, with BM25 on title and body
    and turbopuffer embedding `text` (about 85k embedding tokens)."""
    ns = scratch_ns("sample")
    c = duckdb.connect()
    try:
        stats = live.upsert(ns, c.sql(SAMPLE_SQL), schema=SAMPLE_SCHEMA, distance_metric="cosine_distance")
    finally:
        c.close()
    assert stats.rows == 843
    return ns


@pytest.fixture
def sample_con():
    """A DuckDB connection with the sample tables loaded."""
    c = duckdb.connect()
    for t in ("products", "queries", "judgments"):
        c.execute(f"CREATE TABLE {t} AS SELECT * FROM '{DATA / f'{t}.parquet'}'")
    yield c
    c.close()
