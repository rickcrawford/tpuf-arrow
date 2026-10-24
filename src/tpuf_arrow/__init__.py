"""Query turbopuffer namespaces from DuckDB through Arrow."""

from importlib.metadata import PackageNotFoundError, version

from tpuf_arrow._logging import enable_logging
from tpuf_arrow.client import Client, WriteStats, connect
from tpuf_arrow.errors import ConfigError, SchemaError, TpufArrowError, UpsertError
from tpuf_arrow.sql import register

try:
    __version__ = version("tpuf-arrow")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"

__all__ = [
    "Client",
    "ConfigError",
    "SchemaError",
    "TpufArrowError",
    "UpsertError",
    "WriteStats",
    "__version__",
    "connect",
    "enable_logging",
    "register",
]
