"""Exceptions raised by tpuf_arrow.

Errors from turbopuffer itself (bad requests, auth, rate limits after the
SDK's retries) come through as the SDK's own exceptions, all subclasses of
turbopuffer.APIError. Inside a SQL function DuckDB wraps them in
duckdb.InvalidInputException, and upsert() reports failed writes as
UpsertError. These cover what this package checks before or after
talking to turbopuffer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tpuf_arrow.client import WriteStats


class TpufArrowError(Exception):
    """Base class for every error this package raises."""


class ConfigError(TpufArrowError):
    """Missing or invalid configuration, such as no API key."""


class SchemaError(TpufArrowError, ValueError):
    """Data whose columns or types can't be mapped to or from turbopuffer."""


class UpsertError(TpufArrowError):
    """Some write requests failed. Rows in the requests that succeeded were written.

    stats holds the totals for what succeeded, and stats.errors one line per
    failed request, naming the first and last id in it. Upserts are keyed on
    id, so re-running the whole upsert is safe.
    """

    def __init__(self, message: str, stats: WriteStats):
        super().__init__(message)
        self.stats = stats
