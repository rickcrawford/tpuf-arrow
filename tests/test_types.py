import datetime as dt
import decimal

import pyarrow as pa
import pytest

from tpuf_arrow.errors import SchemaError
from tpuf_arrow.types import (
    arrow_schema,
    arrow_type,
    check_id,
    coerce_rows,
    is_vector,
    to_tpuf_value,
    tpuf_type,
    write_columns,
    write_schema,
)


@pytest.mark.parametrize(
    ("tpuf", "arrow"),
    [
        ("string", pa.string()),
        ("uuid", pa.string()),
        ("int", pa.int64()),
        ("uint", pa.uint64()),
        ("float", pa.float64()),
        ("bool", pa.bool_()),
        ("datetime", pa.string()),
        ("[]string", pa.list_(pa.string())),
        ("[]int", pa.list_(pa.int64())),
        ("[]bool", pa.list_(pa.bool_())),
        ("[1024]f16", pa.list_(pa.float32(), 1024)),
        ("[8]f32", pa.list_(pa.float32(), 8)),
        ("{}f16", pa.string()),  # sparse vectors come back as JSON text
        ("[][8]f32", pa.string()),  # so do multi-vectors
        ("something-new", pa.string()),
    ],
)
def test_arrow_type(tpuf, arrow):
    assert arrow_type(tpuf) == arrow


@pytest.mark.parametrize(
    ("tpuf", "vector"),
    [
        ("[8]f32", True),
        ("[1024]f16", True),
        ("[][8]f32", True),
        ("{}f16", True),
        ("[]float", False),
        ("string", False),
    ],
)
def test_is_vector(tpuf, vector):
    assert is_vector(tpuf) is vector


def test_arrow_schema_puts_id_first_and_keeps_order():
    s = arrow_schema({"id": "uint", "b": "int", "a": "string"}, ["b", "a", "id"])
    assert s.names == ["id", "b", "a"]
    assert s.field("id").type == pa.uint64()


def test_arrow_schema_defaults_unknown_columns_to_string():
    assert arrow_schema({}, ["mystery"]).field("mystery").type == pa.string()


@pytest.mark.parametrize(
    ("arrow", "tpuf"),
    [
        (pa.string(), "string"),
        (pa.large_string(), "string"),
        (pa.dictionary(pa.int8(), pa.string()), "string"),  # DuckDB ENUM
        (pa.bool_(), "bool"),
        (pa.int32(), "int"),
        (pa.uint16(), "uint"),
        (pa.float32(), "float"),
        (pa.decimal128(10, 2), "float"),
        (pa.decimal128(38, 0), "int"),  # DuckDB HUGEINT and sum() results
        (pa.timestamp("us", tz="UTC"), "datetime"),
        (pa.date32(), "datetime"),
        (pa.list_(pa.string()), "[]string"),
        (pa.list_(pa.int64()), "[]int"),
        (pa.list_(pa.bool_()), "[]bool"),
        (pa.list_(pa.float32()), None),  # vector or []float? the caller decides
        (pa.list_(pa.float32(), 4), None),
        (pa.struct([("a", pa.int8())]), None),
        (pa.binary(), None),
        (pa.month_day_nano_interval(), None),
    ],
)
def test_tpuf_type(arrow, tpuf):
    assert tpuf_type(arrow) == tpuf


def test_write_schema_types_new_columns_and_skips_known_ones():
    s = pa.schema([("id", pa.string()), ("price", pa.float64()), ("title", pa.string())])
    assert write_schema(s, id_column="id", known={"title"}) == {"price": {"type": "float"}}


@pytest.mark.parametrize("vec", [pa.list_(pa.float32()), pa.list_(pa.float64(), 3)])
def test_write_schema_refuses_to_guess_about_float_lists(vec):
    s = pa.schema([("id", pa.string()), ("emb", vec)])
    with pytest.raises(SchemaError, match=r"emb.*list of floats.*\[\]float"):
        write_schema(s, id_column="id", known=set())
    assert write_schema(s, id_column="id", known={"emb"}) == {}


def test_write_schema_lets_the_default_vector_attribute_through():
    s = pa.schema([("id", pa.string()), ("vector", pa.list_(pa.float32(), 3))])
    assert write_schema(s, id_column="id", known=set()) == {}


@pytest.mark.parametrize("bad", [pa.binary(), pa.struct([("a", pa.int8())]), pa.month_day_nano_interval()])
def test_write_schema_rejects_types_turbopuffer_cant_store(bad):
    with pytest.raises(SchemaError, match="can't store"):
        write_schema(pa.schema([("id", pa.string()), ("x", bad)]), id_column="id", known=set())


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (pa.schema([("x", pa.string())]), "no 'id' column"),
        (pa.schema([("id", pa.float64())]), "cast it to VARCHAR"),
        (pa.schema([("id", pa.decimal128(38, 0))]), "cast it to VARCHAR"),
        (pa.schema([("doc_id", pa.string()), ("id", pa.string())]), "has an 'id' column as well"),
    ],
)
def test_check_id(schema, message):
    with pytest.raises(SchemaError, match=message):
        check_id(schema, "doc_id" if "doc_id" in schema.names else "id")


def test_write_columns_renames_id_and_converts_values():
    t = pa.table(
        {
            "doc_id": pa.array(["a"]),
            "e": pa.array(["red"]).dictionary_encode(),
            "t": pa.array([1_767_225_600_123_456_789], pa.timestamp("ns", tz="UTC")),
            "d": pa.array([decimal.Decimal("1.50")], pa.decimal128(4, 2)),
            "h": pa.array([decimal.Decimal(2**62)], pa.decimal128(38, 0)),
            "f": pa.array([float("nan")]),
        }
    )
    cols = write_columns(t, "doc_id")
    assert cols == {
        "id": ["a"],
        "e": ["red"],
        "t": ["2026-01-01T00:00:00.123456+00:00"],
        "d": [1.5],
        "h": [2**62],
        "f": [None],
    }


def test_coerce_rows_fits_values_to_text_columns():
    schema = pa.schema([("id", pa.string()), ("n", pa.int64()), ("s", pa.string())])
    rows = coerce_rows([{"id": 7, "n": 1, "s": None}, {"id": "x", "n": 2, "s": {"k": 0.5}}], schema)
    assert rows == [{"id": "7", "n": 1, "s": None}, {"id": "x", "n": 2, "s": '{"k": 0.5}'}]
    pa.Table.from_pylist(rows, schema=schema)  # and they now fit


def test_to_tpuf_value():
    when = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC)
    assert to_tpuf_value(when) == "2026-01-02T03:04:05+00:00"
    assert to_tpuf_value(dt.date(2026, 1, 2)) == "2026-01-02"
    assert to_tpuf_value(decimal.Decimal("1.25")) == 1.25
    assert isinstance(to_tpuf_value(decimal.Decimal("1.25")), float)
    assert to_tpuf_value(decimal.Decimal("3")) == 3
    assert isinstance(to_tpuf_value(decimal.Decimal("3")), int)
    assert to_tpuf_value(decimal.Decimal("NaN")) is None
    assert to_tpuf_value([decimal.Decimal("1.5"), None, float("inf")]) == [1.5, None, None]
    assert to_tpuf_value("x") == "x"


def test_timestamps_go_out_in_utc():
    est = dt.timezone(dt.timedelta(hours=-5))
    assert to_tpuf_value(dt.datetime(2025, 12, 31, 19, tzinfo=est)) == "2026-01-01T00:00:00+00:00"
    assert to_tpuf_value(dt.datetime(2026, 1, 1)) == "2026-01-01T00:00:00+00:00"
