"""Map turbopuffer attribute types to Arrow, and Arrow data to what turbopuffer accepts."""

import datetime as dt
import decimal
import json
import math
import re
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from tpuf_arrow.errors import SchemaError

_SCALARS = {
    "string": pa.string(),
    "uuid": pa.string(),
    "int": pa.int64(),
    "uint": pa.uint64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "datetime": pa.string(),  # turbopuffer returns ISO 8601 text; cast in SQL if you need a TIMESTAMP
}

# Dense vectors look like "[1024]f16" or "[8]f32".
_VECTOR = re.compile(r"^\[(\d+)\](f16|f32)$")

# Element types turbopuffer allows in an array attribute ("[]string", "[]bool", ...).
_ARRAY_ELEMENTS = {"string", "int", "uint", "float", "datetime", "bool", "uuid"}


def arrow_type(tpuf_type: str) -> pa.DataType:
    """Arrow type for a turbopuffer type string such as "string", "[]int", or "[1024]f16".

    Types this package doesn't map, sparse ({}f16) and multi-vector ([][N]f32)
    among them, come back as JSON text so one odd attribute can't fail a read.
    """
    if tpuf_type in _SCALARS:
        return _SCALARS[tpuf_type]
    if m := _VECTOR.match(tpuf_type):
        return pa.list_(pa.float32(), int(m.group(1)))
    if tpuf_type.startswith("[]") and tpuf_type[2:] in _ARRAY_ELEMENTS:
        return pa.list_(arrow_type(tpuf_type[2:]))
    return pa.string()


def is_vector(tpuf_type: str) -> bool:
    """Dense, multi-vector, or sparse: the types left out of include=True because they're big."""
    return bool(_VECTOR.match(tpuf_type)) or tpuf_type.startswith(("[][", "{}"))


def arrow_schema(attr_types: dict[str, str], columns: list[str]) -> pa.Schema:
    """Schema for an id column plus the requested attributes, in the order asked for."""
    fields = [pa.field("id", arrow_type(attr_types.get("id", "string")))]
    fields += [pa.field(c, arrow_type(attr_types.get(c, "string"))) for c in columns if c != "id"]
    return pa.schema(fields)


def coerce_rows(rows: list[dict[str, Any]], schema: pa.Schema) -> list[dict[str, Any]]:
    """Make values fit the schema's text columns: lists and maps as JSON, anything else via str().

    This matters only for attributes read as text that turbopuffer returns
    as something else, and it keeps one odd row from failing a whole page.
    """
    text = {f.name for f in schema if pa.types.is_string(f.type)}
    for row in rows:
        for name in text & row.keys():
            v = row[name]
            if isinstance(v, (dict, list)):
                row[name] = json.dumps(v)
            elif v is not None and not isinstance(v, str):
                row[name] = str(v)
    return rows


def _base(t: pa.DataType) -> pa.DataType:
    """The value type of a dictionary-encoded column (DuckDB sends ENUMs this way)."""
    return t.value_type if pa.types.is_dictionary(t) else t


def _is_float_list(t: pa.DataType) -> bool:
    is_list = pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t)
    # Decimals count: DuckDB types the literal [1.0, 2.0] as DECIMAL(2,1)[].
    return is_list and (pa.types.is_floating(t.value_type) or pa.types.is_decimal(t.value_type))


def tpuf_type(t: pa.DataType) -> str | None:
    """turbopuffer type for an Arrow type, or None when there's no safe automatic choice.

    Float lists get None on purpose. One could be a vector or a plain
    []float, and turbopuffer can't change the type once it's written.
    """
    t = _base(t)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_unsigned_integer(t):
        return "uint"
    if pa.types.is_integer(t):
        return "int"
    if pa.types.is_decimal(t):
        # DuckDB's HUGEINT, and sum() over integers, arrive as DECIMAL(38, 0).
        return "int" if t.scale == 0 else "float"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return "datetime"
    if (pa.types.is_list(t) or pa.types.is_large_list(t)) and not _is_float_list(t):
        inner = tpuf_type(t.value_type)
        return f"[]{inner}" if inner in _ARRAY_ELEMENTS else None
    return None


def write_schema(schema: pa.Schema, *, id_column: str, known: set[str]) -> dict[str, dict]:
    """turbopuffer schema entries for the attributes in an Arrow schema that aren't in known.

    known holds the attributes the namespace already has plus the ones the
    caller typed in schema=. Those are left alone. Raises SchemaError for a
    float list (see tpuf_type) and for types turbopuffer can't store, before
    anything is sent.
    """
    check_id(schema, id_column)
    out = {}
    for f in schema:
        if f.name == id_column or f.name in known:
            continue
        if _is_float_list(f.type):
            if f.name == "vector":  # turbopuffer's default vector attribute, typed by distance_metric
                continue
            size = f.type.list_size if pa.types.is_fixed_size_list(f.type) else "N"
            raise SchemaError(
                f"column {f.name!r} is a list of floats, so pass its type in schema=: "
                f'{{"{f.name}": {{"type": "[{size}]f32", "ann": true}}}} for a vector, or '
                f'{{"{f.name}": {{"type": "[]float"}}}} for a plain array. turbopuffer can\'t '
                f"change an attribute's type after the first write."
            )
        t = tpuf_type(f.type)
        if t is None:
            raise SchemaError(
                f"column {f.name!r} has Arrow type {f.type}, which turbopuffer can't store. "
                f"Cast it in SQL (to VARCHAR, say), or pass its type in schema=."
            )
        out[f.name] = {"type": t}
    return out


def check_id(schema: pa.Schema, id_column: str) -> None:
    """Raise SchemaError unless schema has a usable id column, and no second one named "id"."""
    if id_column not in schema.names:
        raise SchemaError(f"no {id_column!r} column; pass id_column= to say which column is the id")
    if id_column != "id" and "id" in schema.names:
        raise SchemaError(
            f"the data has an 'id' column as well as id_column={id_column!r}, and turbopuffer "
            f"would keep only one of them. Drop or rename 'id' first."
        )
    t = _base(schema.field(id_column).type)
    if not (pa.types.is_integer(t) or pa.types.is_string(t) or pa.types.is_large_string(t)):
        raise SchemaError(
            f"id column {id_column!r} has type {t}. turbopuffer ids are strings, unsigned "
            f"integers, or UUIDs, so cast it to VARCHAR or UBIGINT."
        )


def write_columns(data: pa.Table, id_column: str) -> dict[str, list]:
    """Column lists for a turbopuffer upsert_columns request, with the id column renamed to "id"."""
    cols = {}
    for name, col in zip(data.schema.names, data.columns, strict=True):
        if pa.types.is_dictionary(col.type):
            col = col.cast(col.type.value_type)
        if pa.types.is_timestamp(col.type) and col.type.unit == "ns":
            # Python datetimes stop at microseconds, so nanoseconds are truncated.
            col = pc.cast(col, pa.timestamp("us", col.type.tz), safe=False)
        cols["id" if name == id_column else name] = [to_tpuf_value(v) for v in col.to_pylist()]
    return cols


def to_tpuf_value(v: Any) -> Any:
    """A Python value from Arrow, made JSON-friendly for a turbopuffer write.

    NaN and infinity become null, since JSON can't carry them. Whole-number
    decimals become ints, other decimals become floats.
    """
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, dt.datetime):
        # Naive timestamps are taken as UTC; aware ones are converted to it.
        v = v.replace(tzinfo=dt.UTC) if v.tzinfo is None else v.astimezone(dt.UTC)
        return v.isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        if not v.is_finite():
            return None
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, list):
        return [to_tpuf_value(x) for x in v]
    return v
