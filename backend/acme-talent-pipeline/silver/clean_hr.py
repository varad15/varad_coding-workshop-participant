"""
Bronze -> Silver for the PostgreSQL HR tables.

For each table (parents before children, schema.sql order):
  1. read the latest Bronze snapshot;
  2. validate every row against its Pydantic contract (contracts/hr.py);
  3. drop exact duplicate rows; quarantine primary keys with conflicting values;
  4. quarantine rows whose foreign keys have no valid parent in Silver;
  5. check Silver invariants, write silver/hr/<table>/ and quarantine/<table>/.

Source -> target column mapping: columns keep their source names and types;
lineage columns _batch_id, _ingested_at, _source_table are carried from Bronze.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bronze.ingest_postgres import BRONZE_HR_PREFIX
from common.lineage import RunContext
from common.logger import get_logger
from common.spark import to_arrow, to_spark
from common.storage import DataLake
from contracts.hr import HR_FOREIGN_KEYS, HR_TABLES
from contracts.validation import REASON_COL, VALID_COL, validate_dataframe
from silver.checks import assert_silver_invariants
from silver.quarantine import to_quarantine_rows, write_quarantine

log = get_logger("silver.hr")

SILVER_HR_PREFIX = "silver/hr"
LINEAGE_COLS = ["_source_table", "_batch_id", "_ingested_at"]


def read_latest_snapshot(spark: SparkSession, lake: DataLake, table: str) -> DataFrame:
    """Return the most recent Bronze snapshot of a table."""
    df = to_spark(spark, lake.read_table(f"{BRONZE_HR_PREFIX}/{table}"))
    latest = df.agg(F.max("_ingested_at")).first()[0]
    return df.filter(F.col("_ingested_at") == F.lit(latest))


def split_duplicates(df: DataFrame, pk: str, fields: list[str]) -> tuple[DataFrame, DataFrame, int]:
    """
    Remove duplicates on the primary key.

    Returns:
        (clean rows, rejected rows with REASON_COL, number of exact duplicates dropped)
    """
    before = df.count()
    deduped = df.dropDuplicates(fields)
    exact_dupes = before - deduped.count()

    per_key = Window.partitionBy(pk)
    flagged = deduped.withColumn("_pk_count", F.count(F.lit(1)).over(per_key))
    conflicting = flagged.filter(F.col("_pk_count") > 1).withColumn(
        REASON_COL, F.concat(F.lit(f"duplicate {pk} with conflicting values: "), F.col(pk).cast("string"))
    )
    return flagged.filter(F.col("_pk_count") == 1).drop("_pk_count"), conflicting.drop("_pk_count"), exact_dupes


def split_orphans(df: DataFrame, table: str, parents: dict[str, DataFrame]) -> tuple[DataFrame, DataFrame]:
    """Separate rows whose foreign keys are missing from the parent Silver tables."""
    rejected: list[DataFrame] = []
    for column, parent, parent_key in HR_FOREIGN_KEYS.get(table, []):
        keys = parents[parent].select(F.col(parent_key).alias("_fk")).distinct()
        orphans = df.join(keys, df[column] == keys["_fk"], "left_anti").withColumn(
            REASON_COL, F.concat(F.lit(f"foreign key {column}="), F.col(column).cast("string"), F.lit(f" not found in {parent}"))
        )
        rejected.append(orphans)
        df = df.join(keys, df[column] == keys["_fk"], "left_semi")
    if not rejected:
        return df, df.limit(0).withColumn(REASON_COL, F.lit(None).cast("string"))
    out = rejected[0]
    for extra in rejected[1:]:
        out = out.unionByName(extra)
    return df, out


def clean_hr_tables(spark: SparkSession, lake: DataLake, run: RunContext) -> dict[str, dict[str, object]]:
    """Run Bronze -> Silver for all HR tables; return per-table DQ stats."""
    silver: dict[str, DataFrame] = {}
    stats: dict[str, dict[str, object]] = {}

    for table, (model, pk) in HR_TABLES.items():
        fields = list(model.model_fields.keys())
        with run.step(
            f"silver.{table}", source=lake.uri(f"{BRONZE_HR_PREFIX}/{table}"), target=lake.uri(f"{SILVER_HR_PREFIX}/{table}"), critical=True
        ) as m:
            bronze = read_latest_snapshot(spark, lake, table).select(*fields, *LINEAGE_COLS).cache()
            m.rows_read = bronze.count()

            checked = validate_dataframe(bronze, model, fields).cache()
            invalid = checked.filter(~F.col(VALID_COL))
            valid = checked.filter(F.col(VALID_COL)).drop(VALID_COL, REASON_COL)

            deduped, conflicting, exact_dupes = split_duplicates(valid, pk, fields)
            clean, orphans = split_orphans(deduped, table, silver)
            clean = clean.cache()

            rejects = invalid.drop(VALID_COL).unionByName(conflicting).unionByName(orphans)
            q_rows = to_quarantine_rows(rejects, table, REASON_COL, run, record_fields=fields)
            m.rows_rejected = write_quarantine(lake, table, q_rows)
            m.rows_duplicate = exact_dupes
            m.rows_valid = clean.count()

            required = [f for f, info in model.model_fields.items() if info.is_required()]
            assert_silver_invariants(clean, table, pk, required)
            m.rows_written = lake.write_table(to_arrow(clean), f"{SILVER_HR_PREFIX}/{table}")

            reasons = {r["reason"]: r["count"] for r in q_rows.groupBy("reason").count().collect()}
            stats[table] = {
                "bronze_rows": m.rows_read,
                "silver_rows": m.rows_written,
                "quarantined_rows": m.rows_rejected,
                "duplicates_dropped": exact_dupes,
                "rejection_reasons": reasons,
            }
            silver[table] = clean
    return stats
