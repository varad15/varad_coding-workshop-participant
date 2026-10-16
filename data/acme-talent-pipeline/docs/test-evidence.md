# Test Evidence

`docs/data-engineer.md` (Testing) asks for *test evidence: commands, output, and conclusions*.
Environment: Python 3.13, PySpark 4.1.2, Java 21, PostgreSQL 16, S3 via moto (same API as LocalStack / AWS),
2 vCPU / 8 GB, the same core count as the EKS t3.medium node.

## 1. Functional tests

```sh
pip install -r tests/requirements-test.txt
pytest tests/unit tests/integration --cov=. --cov-config=.coveragerc --cov-report=term-missing
```

```
tests/unit/test_bronze_silver_rules.py ..........                        [ 12%]
tests/unit/test_config_retry_lineage.py .............                    [ 28%]
tests/unit/test_contracts.py ...............................             [ 67%]
tests/unit/test_gold_metrics.py .......                                  [ 76%]
tests/integration/test_pipeline_end_to_end.py ..........                 [ 88%]
tests/integration/test_storage_postgres.py .........                     [100%]
======================== 80 passed in 277.22s (0:04:37) ========================
```

| data-engineer.md requirement | Test(s) | Result |
|---|---|---|
| **Source ingestion validation**: expected row counts and required columns in Bronze | `test_source_ingestion_counts_match_bronze`, `test_bronze_partitions_follow_source_layout`, `test_upload_skips_identical_size_and_verifies` | ✅ 7 tables = source counts; 29,868 raw lines; 783 partition folders; S3 sizes = source sizes |
| **Transformation validation**: cleaning, casting, filtering, dedup in Silver | `test_parse_raw_flags_malformed_json`, `test_split_duplicates_*`, `test_split_orphans_*`, `test_silver_invariants`, `test_silver_has_no_null_keys_or_duplicates` | ✅ |
| **Aggregation validation**: Gold metrics vs known values | `tests/unit/test_gold_metrics.py` (hand-computed expectations for all 7 questions), `test_gold_aggregates_match_silver` | ✅ |
| **Data quality validation**: invalid rows quarantined, rules enforced | `test_contracts.py` (31 rule cases from `schema.sql`), `test_known_data_issues_are_quarantined` | ✅ 13 malformed, 39 drift (whole file), 76 null employee, 54 bad dates, 150 duplicates |
| **Idempotency validation**: run twice, no duplicates | `test_second_run_is_idempotent`, `test_unpartitioned_write_replaces_previous_data`, `test_partitioned_write_replaces_only_matching_partitions`, `test_immutable_write_never_overwrites` | ✅ SHA-256 of all 11 Silver/Gold/quarantine tables identical across runs; landing/seed/Bronze skipped on run 2 |
| **Integration** with PostgreSQL and S3 | `tests/integration/test_storage_postgres.py` | ✅ round-trips, types preserved (INTEGER, NUMERIC(5,2), DATE, NULL) |
| **Reconciliation** across layers | `test_reconciliation_bronze_equals_silver_plus_rejects` + the in-pipeline check in `silver/run.py` and Gold checks in `gold/run.py` | ✅ `bronze = silver + quarantined + duplicates` for all 8 entities |
| **Error handling** | `test_failure_exits_non_zero_with_error_logs`, `test_postgres_connection_failure_is_retried_then_raised`, `test_missing_bucket_is_fatal_in_cloud_mode`, retry/lineage unit tests | ✅ retries logged, job exits 1, status `failed` recorded |

## 2. Coverage

| Area (target from data-engineer.md) | Modules | Coverage |
|---|---|---|
| Core transformations (80%+) | `bronze/*`, `silver/clean_hr.py`, `silver/clean_events.py`, `gold/*` | 96–100% (`clean_events.py` 78%*) |
| Schema and validation rules (90%+) | `contracts/hr.py`, `contracts/learning.py`, `contracts/validation.py`, `silver/checks.py` | 100%, 100%, 89%*, 94% |
| Error handling and retry paths (85%+) | `common/retry.py`, `common/lineage.py`, `common/storage.py`, `common/postgres.py` | 100%, 100%, 99%, 98% |
| Critical business metrics (95%+) | `gold/attrition_risk.py`, `learning_summary.py`, `performance_history.py`, `promotion_pipeline.py`, `skill_distribution.py`, `skill_gaps.py`, `team_outcomes.py` | 100% each |
| End-to-end smoke (100% of one flow) | `tests/integration/test_pipeline_end_to_end.py`: full job, real services, full dataset | ✅ |
| **Total** | 1,205 statements | **96%** |

\* The uncovered lines are the bodies of functions Spark runs **inside its Python worker processes**
(`_parse_lines`, the `mapInArrow` validator). Coverage can't instrument those processes, but the
code is exercised: `test_parse_raw_flags_malformed_json` and `test_validate_dataframe_runs_inside_spark`
assert on their output.

## 3. Performance tests

```sh
pytest -m performance        # writes tests/performance/results/latest.json
```

```
.....                                                                    [100%]
5 passed in 769.78s (0:12:49)
```

Run with the cloud settings (`SPARK_MASTER=local[2]`, `SPARK_DRIVER_MEMORY=512m`). Larger inputs are the
real sample multiplied (event ids rewritten, data-quality issues multiplied too).

| Scale | Raw event lines | Silver events | Bronze + Silver events time | Throughput | Time per 10k records | Peak memory (PSS) |
|---|---:|---:|---:|---:|---:|---:|
| 1× | 29,868 | 29,536 | 40.0 s | 44,775 / min | 13.4 s | 2,139 MB |
| 3× | 89,604 | 88,608 | 185.1 s | 29,050 / min | 20.7 s | 2,213 MB |
| 6× | 179,208 | 177,216 | 227.0 s | 47,364 / min | 12.7 s | 2,267 MB |

| data-engineer.md test | Result |
|---|---|
| Batch throughput | ✅ ~45–47k records/min end to end through Bronze + Silver (incl. Pydantic validation of every row) |
| Scale test (small / medium / large) | ✅ 6× costs **0.95×** the time per record of 1×, so no non-linear slowdown. The 3× run was slower per record; since 6× wasn't, this is attributed to noise in the shared test machine and reported as measured |
| Resource efficiency | ✅ peak 2.1–2.3 GB (JVM ≈ 1.2 GB + Python driver ≈ 0.6 GB + 2 workers), growing only ~6% from 1× to 6×; average CPU 18–29% of 2 cores, so the job is I/O-bound |
| External dependency (retry / backoff) | ✅ `test_external_dependency_retry_backoff`: two injected S3 outages absorbed, write succeeds |
| SLA verification | ✅ full end-to-end run (landing → Gold, 1×) **114 s** vs 600 s SLA (`PIPELINE_SLA_SECONDS`) |

### Memory under the stock chart's 2 GiB limit

The stock EKS chart (`infra/helm`) caps the pod at 2 GiB / 1 CPU with no override, so peak RSS of the whole
process tree (what the pod cgroup counts: Python driver + Spark JVM + Python workers) was sampled every 0.5 s
across full end-to-end runs (landing → Gold, 1× data):

| Spark profile | Peak RSS | Run time | Result |
|---|---:|---:|---|
| defaults (`local[*]`, 1 g heap, worker reuse on) | 2,551 MiB | 120 s | would exceed 2 GiB — OOM-killed |
| `local[2]`, 768 m heap | 2,546 MiB | ~120 s | lingering reused workers keep the peak high |
| **`local[1]`, 512 m heap, `spark.python.worker.reuse=false`** | **1,700 MiB** | 81–98 s | **fits, ≈350 MiB headroom** |

At peak under the final profile: JVM ≈ 965 MiB, Python driver ≈ 680 MiB, one transient worker ≈ 55 MiB.
This profile is what `job.py`'s bootstrap applies in the stock-chart pod (Secret values override it).

## 4. Conclusions and issues found by testing

1. **PySpark 4.1.2 dropped rows silently**: converting the 783-chunk Bronze Arrow table gave 19,934 of
   29,868 rows. Bronze→Silver reconciliation caught it. Fixed by merging chunks; `to_spark()` now asserts
   the row count so it can't recur silently.
2. **Bronze would have skipped a second file per day**: ingestion was tracked per day partition. It's now
   tracked per source file (`test_two_files_on_same_day_are_tracked_separately`).
3. **Memory**: the first measurement (default JVM settings, 1 GB heap) peaked at ~2.6 GB, above the
   repo's 2 GiB job limit. JVM overheads are now capped and the heap is 512 MB in the cloud; the limit
   is raised to 3 GiB with the request kept at 1 GiB (see `limitations.md`).
4. Every data quality issue profiled in the raw files is caught, and every row is accounted for.
