# Assumptions, Trade-offs and Known Limitations

`docs/data-engineer.md` (Experience) asks for these to be *explicitly called out*.

## Business assumptions

| Area | Assumption | Why / how to change |
|---|---|---|
| Critical skill gap | `required_level − proficiency_level ≥ 2` | Not defined in the brief. One level short counts as "minor". Constant `CRITICAL_GAP` in `gold/skill_gaps.py` |
| Promotion candidate | High potential **and** Ready Now **and** latest rating ≥ 4 | Combines the talent review with demonstrated performance (9-box "Star"). `gold/promotion_pipeline.py` |
| Performance bands | 1–2 Low, 3 Moderate, 4–5 High | Standard 9-box banding on the latest review |
| Attrition risk | Rule weights: declined +2, latest ≤ 2 +2, negative comment +1, no learning in 180 days +1; ≥ 3 = High | Transparent, explainable rules instead of a trained model (no attrition outcomes in the data to train on). Shown **next to** HR's own flag, not replacing it |
| Negative comment | Keywords: *trended down, missed, struggled, needs support, follow-up plan* | Derived from the 9 distinct comments in the data |
| Course → competency | Curated mapping `reference/course_competency_map.csv` (42 courses → 40 competencies) | The learning platform has no competency id. The mapping is versioned, validated against `competencies`, and unmapped courses are logged as WARN |
| As-of date | Latest date found in the data, not today | Keeps Gold deterministic (same input gives the same output) |
| Invalid event dates | Quarantined, not inferred from the folder date | A fabricated timestamp would distort timelines |

## Data limitations

- **Two review periods only** (2024-H2, 2025-H1): a "trend" is a single period-over-period change.
- **One talent-review cycle** (2025-H1): potential, readiness and HR attrition risk have no history.
- **Projects link to teams, not employees**: the performance-vs-outcome analysis is team-level, and only 27 of 100 teams have projects (33 projects). The correlations are indicative, not causal.
- Synthetic data: e.g. some course events exist without an enrollment event; the pipeline reports what happened and doesn't enforce an event order.

## Technical trade-offs

| Decision | Trade-off |
|---|---|
| **S3 / PostgreSQL I/O via PyArrow + psycopg** instead of Spark's `s3a://` / JDBC connectors | No JARs downloaded from Maven at start-up (fragile on the VDI and in EKS pods; the repo's own skeleton shipped a mismatched Spark/Iceberg version). Data passes through the Spark driver, which is fine for this ~14 MB dataset. For large volumes, replace `common/storage.py` / `common/postgres.py` with `s3a://` and JDBC reads; nothing else changes |
| **Arrow → Spark conversion merges chunks and verifies row counts** | PySpark 4.1.2 silently dropped about 1/3 of rows from a 783-chunk Arrow table. Found by reconciliation, fixed in `common/spark.py`, guarded by a row-count assertion |
| **No Apache Iceberg** (configured in the repo skeleton) | Not required by data-engineer.md, and it needs a runtime Maven download. Parquet + Hive partitions + idempotent writes meet the requirements. Iceberg would add ACID MERGE and time travel |
| **Bronze snapshots per content hash** | Unchanged sources are never re-written (idempotent and immutable); every change creates a new snapshot (history kept, storage grows) |
| **Silver/Gold full rebuild each run** | Simple and idempotent at this size; for large data, rebuild only affected partitions |
| **Single Kubernetes Job, stages in sequence** | Separate `data/*` jobs would run concurrently with no ordering. An orchestrator (Airflow, Step Functions) is the next step for scheduling and retries per stage |
| Pipeline lives in `backend/` | Per the workshop organisers' instruction. `infra/locals.tf` routes `backend/*/job.py` folders to EKS instead of Lambda (a 128 MB Lambda cannot run Spark) |

## Cloud deployment caveats (to verify on the workshop AWS account)

- **S3 permissions of the EKS node role.** The job uses the pod's AWS credentials (EKS node role `coding-workshop-eks-<region>-<id>`, pre-created by the organisers). If it lacks access to `coding-workshop-datalake-<id>`, the job fails with *AccessDenied* (§7). Fix: grant the role access to the bucket, or pass the participant credentials from `ENVIRONMENT.config` as job secrets.
- **Job image** `jupyter/all-spark-notebook:latest` ships an older Spark; the job installs PySpark 4.1.2 with pip and ignores the image's `SPARK_HOME` (`common/spark.py`). The pod needs PyPI access (already assumed by the repo's Helm template).
- **Memory:** Spark's JVM plus the Python driver peak at about 2.1 GiB (PSS) on the sample data, even with a 512 MB heap and capped JVM overheads. That exceeds the repo's default 2 GiB job limit, so the limit is raised to **3 GiB** (request unchanged at 1 GiB, so it still schedules next to JupyterHub on one t3.medium). Running the job and a heavy notebook session at the same moment could pressure the 4 GiB node.
- **Redeploying while a Job still exists:** a Kubernetes Job's pod template is immutable. Wait for `ttlSecondsAfterFinished` (100 s) or run `kubectl delete job coding-workshop-acme-talent-pipeline` before `deploy-backend.sh`.
- Terraform/Helm changes were written for the repo's toolchain but couldn't be executed in the development sandbox. Run `terraform validate` on the VDI first.
