"""
PostgreSQL access (local PostgreSQL or AWS Aurora).

Uses psycopg 3, the same driver as the workshop's backend example
(backend/_examples/python-service/postgres_service.py). Tables are read with
their PostgreSQL types preserved, so Bronze keeps the source schema exactly.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
import pyarrow as pa
from psycopg import sql

from common.config import Settings
from common.logger import get_logger
from common.retry import with_retry

log = get_logger("postgres")

TRANSIENT_PG_ERRORS: tuple[type[BaseException], ...] = (psycopg.OperationalError,)

# PostgreSQL type OID -> Arrow type. Anything unmapped is read as text.
_OID_TO_ARROW: dict[int, pa.DataType] = {
    16: pa.bool_(),
    20: pa.int64(),
    21: pa.int16(),
    23: pa.int32(),
    700: pa.float32(),
    701: pa.float64(),
    25: pa.string(),
    1042: pa.string(),
    1043: pa.string(),
    1082: pa.date32(),
    1114: pa.timestamp("us"),
    1184: pa.timestamp("us", tz="UTC"),
}
_NUMERIC_OID = 1700


def _arrow_type(column: psycopg.Column) -> pa.DataType:
    """Map a result column to an Arrow type, keeping NUMERIC precision."""
    if column.type_code == _NUMERIC_OID:
        if column.precision is not None and column.scale is not None:
            return pa.decimal128(column.precision, column.scale)
        return pa.decimal128(38, 10)
    return _OID_TO_ARROW.get(column.type_code, pa.string())


class Postgres:
    """Connection factory with retry for the PostgreSQL source."""

    def __init__(self, settings: Settings) -> None:
        """Store connection settings."""
        self._settings = settings.postgres
        self._attempts = settings.retry_attempts

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        """Open a connection (retried on transient failures) and close it afterwards."""

        @with_retry(TRANSIENT_PG_ERRORS, attempts=self._attempts)
        def _open() -> psycopg.Connection:
            return psycopg.connect(self._settings.conninfo())

        conn = _open()
        try:
            yield conn
        finally:
            conn.close()

    def read_table(self, table: str, order_by: str | None = None) -> pa.Table:
        """
        Read a whole table into Arrow with source types preserved.

        Args:
            table: Table name (quoted safely).
            order_by: Optional column for deterministic row order.
        """
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))
        if order_by:
            query = sql.SQL("{} ORDER BY {}").format(query, sql.Identifier(order_by))

        @with_retry(TRANSIENT_PG_ERRORS, attempts=self._attempts)
        def _read() -> pa.Table:
            with self.connect() as conn, conn.cursor() as cur:
                cur.execute(query)
                columns = cur.description or []
                rows = cur.fetchall()
            schema = pa.schema([pa.field(c.name, _arrow_type(c)) for c in columns])
            data = {c.name: [row[i] for row in rows] for i, c in enumerate(columns)}
            return pa.Table.from_pydict(data, schema=schema)

        return _read()

    def row_count(self, table: str) -> int:
        """Return SELECT COUNT(*) for a table."""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
            return int(cur.fetchone()[0])
