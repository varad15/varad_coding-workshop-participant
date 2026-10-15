# ACME Inc. — Performance & Talent Management Data Pipeline

Batch ETL pipeline on a medallion data lake (Bronze → Silver → Gold). It ingests HR data from
**PostgreSQL** and learning-platform events from **S3**, validates them with **Pydantic** contracts,
transforms them with **PySpark**, and serves seven business answers to a **Jupyter** notebook.

Built from the workshop skeleton `data/_examples/python-job` and following `docs/data-engineer.md`.
Per the organisers' instruction it lives in `backend/` (see [docs/architecture.md](docs/architecture.md) for the
infra wiring that runs it on EKS instead of Lambda).

| Business question | Gold table |
|---|---|
| Performance ratings and review history per employee | `employee_performance_history` |
| Critical skill gaps | `employee_skill_gaps` |
| High-potential employees ready for promotion | `promotion_pipeline` |
| Training and development completed | `employee_course_progress`, `employee_learning_summary` |
| Attrition risk from performance trends | `attrition_risk` |
| Performance vs. team/project outcomes | `team_performance_outcomes` |
| Distribution of skills | `skill_distribution` |

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | diagram, module responsibilities, S3 layout, partitioning, idempotency, local vs cloud |
| [docs/source-to-target-mapping.md](docs/source-to-target-mapping.md) | every column from source to Gold: renamed / cast / derived / aggregated |
| [docs/data-quality-report.md](docs/data-quality-report.md) | issues found in the data and how each is handled |
| [docs/test-evidence.md](docs/test-evidence.md) | test commands, results, coverage, performance figures |
| [docs/limitations.md](docs/limitations.md) | assumptions, trade-offs, known limitations |
| [notebooks/acme_talent_insights.ipynb](notebooks/acme_talent_insights.ipynb) | the answers, with charts and narrative |

## Project structure

```
backend/
├── _source/                      sample dataset (CSVs, schema.sql, JSONL events) - not deployed
└── acme-talent-pipeline/
    ├── job.py                    entry point: landing -> seed -> bronze -> silver -> gold
    ├── requirements.txt
    ├── common/                   config, logging, retry, S3 + PostgreSQL adapters, Spark, lineage
    ├── contracts/                Pydantic schema contracts (mirror schema.sql) + Spark validator
    ├── seed/                     source-system bootstrap (S3 landing + PostgreSQL)
    ├── bronze/                   raw, immutable ingestion
    ├── silver/                   cleaning, validation, quarantine, DQ report
    ├── gold/                     one module per business question
    ├── reference/                course -> competency mapping (curated)
    ├── notebooks/                Jupyter analysis
    ├── tests/                    unit / integration / performance
    └── docs/
```

## Configuration

All configuration comes from environment variables (data-engineer.md §2). Defaults match the workshop VDI.

| Variable | Local default | Cloud |
|---|---|---|
| `IS_LOCAL` | `true` | `false` (injected) |
| `POSTGRES_HOST` / `PORT` / `NAME` / `USER` / `PASS` | `localhost` / `5432` / `postgres` / `postgres` / `postgres123` | Aurora (injected; password via Kubernetes Secret) |
| `AWS_ENDPOINT_URL_S3` | `http://s3.localhost.localstack.cloud:4566` | unset (real S3) |
| `DATALAKE_BUCKET` | `coding-workshop-datalake-<PARTICIPANT_ID>` | injected from Terraform |
| `AWS_REGION` | `us-east-1` | injected |
| optional | `RETRY_ATTEMPTS` (5), `LOG_LEVEL` (INFO), `SEED_SOURCES` (true), `SOURCE_DATA_DIR`, `SPARK_MASTER`, `SPARK_DRIVER_MEMORY` | |

## Run locally (VDI)

Prerequisites from the workshop setup (`bin/setup-environment.sh`): Python 3.13, Java 21, PostgreSQL, LocalStack.

```sh
cd ~/coding-workshop-participant
source ~/.bashrc
./bin/start-dev.sh                         # starts PostgreSQL + LocalStack (Ctrl+C after "LocalStack services verified" is fine)

cd backend/acme-talent-pipeline
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# LocalStack credentials (override the participant AWS keys from ENVIRONMENT.config)
export IS_LOCAL=true AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
       AWS_ENDPOINT_URL_S3=http://s3.localhost.localstack.cloud:4566

python job.py 2>&1 | tee /tmp/acme-talent-pipeline.log    # all stages
python job.py --stages silver,gold                        # selected stages
```

Exit code `0` = success, `1` = failure. Every log line is JSON; to see the step summary:

```sh
grep '"step_finished"' /tmp/acme-talent-pipeline.log | python3 -c "import sys,json; [print(d['step'], d['status'], d['rows_read'], '->', d['rows_written']) for d in map(json.loads, sys.stdin)]"
```

Inspect the lake:

```sh
aws --endpoint-url $AWS_ENDPOINT_URL_S3 s3 ls s3://coding-workshop-datalake-$PARTICIPANT_ID/ --recursive | head
aws --endpoint-url $AWS_ENDPOINT_URL_S3 s3 cp s3://coding-workshop-datalake-$PARTICIPANT_ID/_meta/dq_reports/latest/report.json -
```

### Notebook (local)

Run Jupyter from the pipeline's virtual environment. On the VDI the system Python's Pillow is broken, so the
default "Python 3" kernel cannot import matplotlib.

```sh
source .venv/bin/activate                   # the same venv the pipeline runs in
pip install notebook ipykernel matplotlib pillow
python -m ipykernel install --user --name acme-pipeline --display-name "ACME pipeline (.venv)"
# from a shell that has the exports above:
python -m jupyter notebook --port 8889
```

Open `notebooks/acme_talent_insights.ipynb`, choose **Kernel → Change kernel → ACME pipeline (.venv)**, then
**Run → Run All Cells**. The notebook reads only Gold tables (plus the DQ report) from S3, so run `python job.py` first.

## Tests

```sh
pip install -r tests/requirements-test.txt
pytest tests/unit                                       # fast, no services needed
pytest tests/integration                                # real PostgreSQL + S3 (moto starts automatically)
TEST_S3_ENDPOINT=$AWS_ENDPOINT_URL_S3 pytest tests/integration   # ...or against LocalStack
pytest -m performance                                   # throughput, scale, resources, retry, SLA (slow)
pytest tests/unit tests/integration --cov=. --cov-report=term-missing
```

Integration tests create and drop their own database (`acme_test`) and buckets. Results: [docs/test-evidence.md](docs/test-evidence.md).

## Deploy to AWS

```sh
cd ~/coding-workshop-participant
echo "export TF_VAR_aws_eks_enabled=true" >> ~/.bashrc && source ~/.bashrc
(cd infra && terraform init -backend=false >/dev/null && terraform validate)   # sanity check
./bin/deploy-backend.sh
```

`deploy-backend.sh` (Terraform) creates Aurora, the data lake bucket and EKS; syncs `backend/_source` to
`s3://coding-workshop-datalake-<id>/landing/`; then installs the job with Helm. Follow it with:

```sh
aws eks update-kubeconfig --name coding-workshop-$PARTICIPANT_ID
kubectl get jobs
kubectl logs -f job/coding-workshop-acme-talent-pipeline
```

To re-run after a code change: `kubectl delete job coding-workshop-acme-talent-pipeline --ignore-not-found && ./bin/deploy-backend.sh`.

### Notebook (cloud, JupyterHub on EKS)

```sh
kubectl port-forward svc/proxy-public 8080:80     # then open http://localhost:8080, log in with workshop credentials
```

Upload `notebooks/acme_talent_insights.ipynb` and, before the first cell, set:

```python
import os
os.environ.update(IS_LOCAL="false", DATALAKE_BUCKET="coding-workshop-datalake-<your-participant-id>", AWS_REGION="us-east-1")
```

Cloud caveats (S3 permissions of the EKS role, job memory, redeploys) are in [docs/limitations.md](docs/limitations.md).
