"""
Apply Pydantic contracts to records, in plain Python or inside Spark.

`validate_dataframe` runs the contract on Spark executors via mapInArrow,
so validation scales with the cluster instead of collecting rows to the
driver. Every row comes back with two extra columns:

    _is_valid          boolean
    _rejection_reason  string, e.g. "rating: Input should be less than or equal to 5"
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator

import pyarrow as pa
from pydantic import BaseModel, ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

VALID_COL = "_is_valid"
REASON_COL = "_rejection_reason"


def format_errors(exc: ValidationError) -> str:
    """Turn a ValidationError into a compact 'field: message; ...' string."""
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "record"
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts)


def validate_record(model: type[BaseModel], record: dict[str, Any]) -> str | None:
    """Return None if the record satisfies the contract, else the reason."""
    try:
        model.model_validate(record)
        return None
    except ValidationError as exc:
        return format_errors(exc)


def validate_records(
    model: type[BaseModel], records: Iterable[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records into (valid, rejected); rejected rows carry REASON_COL."""
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for record in records:
        reason = validate_record(model, record)
        if reason is None:
            valid.append(record)
        else:
            rejected.append({**record, REASON_COL: reason})
    return valid, rejected


def validate_dataframe(
    df: DataFrame,
    model: type[BaseModel],
    fields: list[str] | None = None,
) -> DataFrame:
    """
    Validate every row of a Spark DataFrame against a contract.

    Args:
        df: Input rows.
        model: Pydantic contract.
        fields: Columns passed to the contract (default: the model's fields);
            other columns such as lineage metadata are carried through untouched.
    """
    contract_fields = fields or list(model.model_fields.keys())
    out_schema = StructType(
        df.schema.fields
        + [StructField(VALID_COL, BooleanType(), False), StructField(REASON_COL, StringType(), True)]
    )

    def _validate(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batches:
            rows = batch.to_pylist()
            reasons = [validate_record(model, {k: row.get(k) for k in contract_fields}) for row in rows]
            yield pa.RecordBatch.from_arrays(
                batch.columns
                + [pa.array([r is None for r in reasons], pa.bool_()), pa.array(reasons, pa.string())],
                names=batch.schema.names + [VALID_COL, REASON_COL],
            )

    return df.mapInArrow(_validate, out_schema)
