"""
Silver layer invariants - data-engineer.md section 6, Silver checklist.

Run on every Silver output before it is written; a violation fails the step.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class SilverCheckError(RuntimeError):
    """A Silver output violated a checklist rule."""


def assert_silver_invariants(df: DataFrame, entity: str, primary_key: str, required: list[str]) -> None:
    """
    Enforce the Silver checklist:
      * zero nulls in the (non-nullable) primary key and required columns;
      * no duplicate primary keys;
      * dates/timestamps parsed (required date columns are non-null and typed).
    """
    total = df.count()
    null_counts = df.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in [primary_key, *required]]
    ).first().asDict()
    bad = {c: n for c, n in null_counts.items() if n}
    if bad:
        raise SilverCheckError(f"{entity}: nulls in required columns {bad}")

    distinct_keys = df.select(primary_key).distinct().count()
    if distinct_keys != total:
        raise SilverCheckError(f"{entity}: {total - distinct_keys} duplicate {primary_key} values")

    for name, dtype in df.dtypes:
        if name in required and name.endswith(("_date", "_ts")) and dtype not in {"date", "timestamp"}:
            raise SilverCheckError(f"{entity}: {name} is {dtype}, expected date/timestamp")
