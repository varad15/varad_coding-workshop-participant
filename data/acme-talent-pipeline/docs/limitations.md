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
| Pipeline lives in `backend/`, `bin/` and `infra/` untouched | Per the workshop organisers' instruction. Dependencies are in `requirements-job.txt` so the stock scripts don't deploy the folder as a Lambda (a 128 MB Lambda cannot run Spark); the job is deployed to the stock EKS cluster by its own script |

## Cloud deployment caveats (to verify on the workshop AWS account)

- **First AWS run:** `deploy.sh` provisioned everything and the job started on EKS with the cloud settings; it then
  failed on the two issues below (credentials, image packages), which are fixed.
- **Network (Aurora from EKS):** the second cloud run timed out connecting to Aurora. Aurora's stock security
  group only admits itself, while EKS gives its worker nodes a separate cluster security group. Editing security
  group rules is denied to participants (`UnauthorizedOperation`; `deploy.sh` only warns), but `rds:ModifyDBCluster`
  is permitted: `deploy/open-aurora-network.sh` attaches the EKS cluster security group to the Aurora cluster —
  no rule is added or changed anywhere. Terraform resets Aurora's groups on every `./bin/deploy-backend.sh`, so the
  script must be re-run after each deploy (propagation ≈2–5 min; the Job's retries cover the gap). This is a manual
  change to organiser-managed infrastructure and is called out here deliberately.
- **AWS credentials in the pod.** On the first cloud run the pod could not obtain the EKS node role's credentials
  (`NoCredentialsError`), and pod-level IAM (IRSA) would need infra changes. `deploy.sh` therefore passes the
  participant's *temporary* credentials to the job through the Kubernetes Secret. They expire (the job needs only a
  few minutes) and every `deploy.sh` run refreshes them; `undeploy.sh` deletes them. In production the job would use
  its own IAM role (IRSA), never a person's credentials.
- **Image packages:** `jupyter/all-spark-notebook` ships libraries built for NumPy 1.x and an older Spark on
  `PYTHONPATH`/`SPARK_HOME`; the job installs its pinned dependencies into its own virtualenv (`/tmp/venv`) and points
  Spark's Python workers at it (`PYSPARK_PYTHON`).
- **Memory:** Spark's JVM plus the Python driver peak at about 2.1 GiB (PSS) on the sample data with a 512 MB heap,
  so `deploy/k8s/job.yaml` allows 3 GiB (the stock Helm job limit is 2 GiB).
- **Image:** `jupyter/all-spark-notebook:latest` (the workshop's job image: Python 3.11, Java 17). Every pinned
  dependency has a Python 3.11 wheel (checked); PySpark 4.1.2 needs Python ≥ 3.10 and Java 17+. The pod installs
  them from PyPI at start-up, as the stock Helm job does. Override with `JOB_IMAGE=...`.
- **Verified so far:** the full pipeline has run green on EKS via `deploy/deploy.sh`
  (run `20261015T110047Z-48df18`: 153 s, 27 steps, all reconciliations passed, `gold/_api` written and served by
  the insights-api Lambda). The stock-chart path below is verified locally (97 tests green from `data/`, full run
  under the 2 GiB profile); its first real EKS run is pending.

## Stock-chart deployment gaps (data/ job via ./bin/deploy-backend.sh)

The job now lives in `data/acme-talent-pipeline/` so the organisers' own deploy path provisions it
(`infra/locals.tf` → `infra/eks.tf` → `infra/helm/templates/python-job.yaml`). Gaps found in that path, and how
this project works inside them without touching `bin/` or `infra/`:

- **No environment is injected.** The guide (`docs/data-engineer.md`, "Database Environment Variables") says
  `POSTGRES_*`/`IS_LOCAL` are injected automatically in cloud mode, but the stock chart defines no `env` at all.
  Workaround: `deploy/cloud-prereqs.sh` writes the full job environment into the Kubernetes Secret
  `acme-talent-pipeline-secrets`, and `job.py`'s bootstrap reads it through the in-cluster API (a Role/RoleBinding
  grants the default ServiceAccount `get` on that one named Secret).
- **Only two files reach the pod** (`job.py`, `requirements.txt`, via ConfigMap). Workaround: the prereqs script
  uploads the pipeline source as `s3://<datalake>/_deploy/src.tar.gz`; the bootstrap downloads and re-executes it.
- **No AWS identity** (no ServiceAccount IAM, and IMDS hop limit 1 blocks the node role). Same temporary-credentials
  Secret as the fallback path; IRSA would be the production answer but needs infra changes.
- **2 GiB / 1 CPU, not overridable** from outside `infra/`. The bootstrap applies a constrained Spark profile
  (`local[1]`, 512 MB driver heap, `spark.python.worker.reuse=false`): measured peak falls from ≈2.5 GiB to
  ≈1.7 GiB (`docs/test-evidence.md`), at ≈1.2× the runtime.
- **The data-lake bucket is created by our scripts, not Terraform**, so `terraform destroy` will not remove it —
  delete `s3://coding-workshop-datalake-<app-id>` by hand when tearing down.
