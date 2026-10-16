#!/usr/bin/env bash
# Deploy the ACME talent pipeline to the workshop's EKS cluster and run it.
#
# The workshop's own scripts provision the platform (unchanged bin/ and infra/):
#     export TF_VAR_aws_eks_enabled=true && ./bin/deploy-backend.sh     # Aurora + EKS
# This script deploys the workload onto that platform:
#     1. preflight: tools, fresh AWS credentials, Terraform state
#     2. read the EKS cluster and Aurora connection from the Terraform state
#     3. connect kubectl to the cluster, and allow its worker nodes to reach Aurora on 5432
#     4. create the data lake bucket (private, encrypted) if it does not exist
#     5. sync backend/_source to s3://<bucket>/landing/ (the S3 source system)
#     6. store the Aurora password (+ temporary AWS credentials) as a Kubernetes Secret
#     7. package this folder as a ConfigMap
#     8. (re)create the Kubernetes Job, stream its logs and report the result
#
# Usage (from anywhere, after `source ~/.bashrc && source ENVIRONMENT.config`):
#     backend/acme-talent-pipeline/deploy/deploy.sh
# Options (environment variables):
#     JOB_IMAGE=...           container image (default: the workshop's jupyter/all-spark-notebook:latest)
#     STAGES=seed,bronze,...  pipeline stages to run (default: seed,bronze,silver,gold)
#     FOLLOW_LOGS=false       start the job and return without waiting
#     WAIT_TIMEOUT_S=1800     how long to wait for the job to finish
#     USE_NODE_ROLE=true      do not pass your AWS credentials to the job (only if pods can use the node role)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$PIPELINE_DIR/../.." && pwd)"
SOURCE_DIR="$REPO_ROOT/backend/_source"
INFRA_DIR="$REPO_ROOT/infra"

export JOB_NAME="acme-talent-pipeline"
export NAMESPACE="${NAMESPACE:-default}"
export JOB_IMAGE="${JOB_IMAGE:-jupyter/all-spark-notebook:latest}"
export STAGES="${STAGES:-seed,bronze,silver,gold}"
FOLLOW_LOGS="${FOLLOW_LOGS:-true}"
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-1800}"
MAX_BUNDLE_BYTES=900000   # ConfigMaps are limited to 1 MiB

step() { printf '\n==> %s\n' "$*"; }
die() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# ------------------------------------------------------------------ 1. preflight
step "Preflight checks"
for cmd in aws kubectl terraform python3 tar; do
    command -v "$cmd" >/dev/null 2>&1 || die "'$cmd' is not installed or not on PATH"
done
[ -d "$SOURCE_DIR/learning_activity" ] || die "sample data not found at $SOURCE_DIR"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
[ -n "$AWS_REGION" ] || die "AWS_REGION is not set: run 'source ENVIRONMENT.config' from the repo root"
aws sts get-caller-identity >/dev/null 2>&1 \
    || die "AWS credentials are missing or expired: run './bin/setup-participant.sh && source ENVIRONMENT.config'"
echo "AWS credentials OK (region $AWS_REGION)"

# ------------------------------------------------------- 2. stack from Terraform
step "Reading the stack from Terraform state"
terraform -chdir="$INFRA_DIR" state pull > "$TMP_DIR/state.json" 2>"$TMP_DIR/tf.err" \
    || die "cannot read Terraform state ($(head -c 300 "$TMP_DIR/tf.err")). Run ./bin/deploy-backend.sh first"
eval "$(python3 "$SCRIPT_DIR/stack_from_state.py" < "$TMP_DIR/state.json")"
rm -f "$TMP_DIR/state.json"
echo "app id:       $APP_ID"
echo "EKS cluster:  $EKS_CLUSTER"
echo "Aurora:       $PG_HOST:$PG_PORT/$PG_NAME (user $PG_USER)"
echo "data lake:    s3://$DATALAKE_BUCKET"

# ------------------------------------------------------------ 3. kubectl context
step "Connecting kubectl to $EKS_CLUSTER"
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" >/dev/null
ready_nodes="$(kubectl get nodes --no-headers 2>/dev/null | awk '$2 == "Ready"' | wc -l || true)"
[ "$ready_nodes" -ge 1 ] || die "no Ready nodes in $EKS_CLUSTER (kubectl get nodes)"
echo "$ready_nodes node(s) Ready"

# EKS gives the worker nodes its own cluster security group; Aurora only accepts the
# workshop group. Allow PostgreSQL (tcp/5432) from the cluster group - nothing else.
step "Network: EKS worker nodes -> Aurora (tcp/5432)"
if [ -z "$EKS_CLUSTER_SG" ]; then
    EKS_CLUSTER_SG="$(aws eks describe-cluster --name "$EKS_CLUSTER" --region "$AWS_REGION" \
        --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)"
fi
[ -n "$EKS_CLUSTER_SG" ] && [ "$EKS_CLUSTER_SG" != "None" ] || die "could not find the EKS cluster security group"
[ -n "$AURORA_SG_IDS" ] || die "could not find Aurora's security group in the Terraform state"
for sg in $AURORA_SG_IDS; do
    # shellcheck disable=SC2016  # backticks are a JMESPath literal, not shell
    allowed="$(aws ec2 describe-security-groups --group-ids "$sg" --region "$AWS_REGION" --output text \
        --query 'SecurityGroups[0].IpPermissions[?FromPort==`5432`].UserIdGroupPairs[].GroupId' 2>/dev/null || true)"
    if printf '%s\n' "$allowed" | tr '\t' '\n' | grep -qx "$EKS_CLUSTER_SG"; then
        echo "$sg already allows 5432 from $EKS_CLUSTER_SG"
        continue
    fi
    rule='[{"IpProtocol":"tcp","FromPort":5432,"ToPort":5432,"UserIdGroupPairs":[{"GroupId":"'"$EKS_CLUSTER_SG"'","Description":"acme-talent-pipeline: EKS pods to Aurora"}]}]'
    if aws ec2 authorize-security-group-ingress --group-id "$sg" --region "$AWS_REGION" \
            --ip-permissions "$rule" >/dev/null 2>"$TMP_DIR/sg.err" \
        || grep -q 'InvalidPermission.Duplicate' "$TMP_DIR/sg.err"; then
        echo "$sg now allows 5432 from $EKS_CLUSTER_SG"
    else
        echo "WARNING: could not add the rule on $sg ($(grep -o 'UnauthorizedOperation\|AccessDenied[A-Za-z]*\|InvalidGroup[A-Za-z.]*' "$TMP_DIR/sg.err" | head -1))."
        echo "         Continuing in case Aurora already accepts the EKS nodes another way."
        echo "         If seed.postgres times out, ask the organisers to allow tcp/5432 from $EKS_CLUSTER_SG to $sg."
    fi
done

# -------------------------------------------------------------- 4. data lake bucket
step "Data lake bucket"
if aws s3api head-bucket --bucket "$DATALAKE_BUCKET" >/dev/null 2>&1; then
    echo "s3://$DATALAKE_BUCKET exists"
else
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$DATALAKE_BUCKET" >/dev/null
    else
        aws s3api create-bucket --bucket "$DATALAKE_BUCKET" \
            --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
    fi
    aws s3api put-public-access-block --bucket "$DATALAKE_BUCKET" --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
    aws s3api put-bucket-encryption --bucket "$DATALAKE_BUCKET" --server-side-encryption-configuration \
        '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
    echo "created s3://$DATALAKE_BUCKET (private, encrypted)"
fi

# ------------------------------------------------------------- 5. landing data
step "Syncing backend/_source to s3://$DATALAKE_BUCKET/landing/"
aws s3 sync "$SOURCE_DIR/learning_activity" "s3://$DATALAKE_BUCKET/landing/learning_activity/" \
    --exclude "*" --include "*.jsonl" --only-show-errors
aws s3 sync "$SOURCE_DIR" "s3://$DATALAKE_BUCKET/landing/postgres_seed/" \
    --exclude "*" --include "*.csv" --include "schema.sql" --exclude "learning_activity/*" --only-show-errors
local_files="$(find "$SOURCE_DIR/learning_activity" -name '*.jsonl' | wc -l)"
remote_files="$(aws s3 ls "s3://$DATALAKE_BUCKET/landing/learning_activity/" --recursive | grep -c '\.jsonl$' || true)"
[ "$local_files" -eq "$remote_files" ] || die "landing has $remote_files event files, expected $local_files"
echo "$remote_files event files + HR seed files in landing/"

# --------------------------------------------------------------- 6. job secrets
# Pods on the workshop's EKS nodes cannot obtain the node role's AWS credentials
# (first cloud run: NoCredentialsError), and IRSA would need infra changes. The job
# therefore gets the participant's temporary credentials - the same ones this script
# uses for the bucket - refreshed on every deploy. Set USE_NODE_ROLE=true to skip.
step "Kubernetes Secret ${JOB_NAME}-secrets"
umask 077
printf 'POSTGRES_PASS=%s\n' "$PG_PASS" > "$TMP_DIR/job.env"
if [ "${USE_NODE_ROLE:-false}" != "true" ]; then
    if ! aws configure export-credentials --format env-no-export > "$TMP_DIR/aws.env" 2>/dev/null; then
        : > "$TMP_DIR/aws.env"
        for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
            [ -n "${!var:-}" ] && printf '%s=%s\n' "$var" "${!var}" >> "$TMP_DIR/aws.env"
        done
    fi
    grep -q '^AWS_ACCESS_KEY_ID=' "$TMP_DIR/aws.env" \
        || die "could not read your AWS credentials: run 'source ENVIRONMENT.config' and retry"
    # keep the three credential lines, without any surrounding quotes
    grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' "$TMP_DIR/aws.env" \
        | sed -E "s/^([A-Z_]+)=[\"'](.*)[\"']$/\1=\2/" >> "$TMP_DIR/job.env"
    expiry="$(sed -n 's/^AWS_CREDENTIAL_EXPIRATION=//p' "$TMP_DIR/aws.env")"
    echo "job will use your temporary AWS credentials${expiry:+ (valid until $expiry)}"
fi
kubectl create secret generic "${JOB_NAME}-secrets" -n "$NAMESPACE" --from-env-file="$TMP_DIR/job.env" \
    --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f - >/dev/null
rm -f "$TMP_DIR/job.env" "$TMP_DIR/aws.env"
unset PG_PASS
kubectl delete secret "${JOB_NAME}-db" -n "$NAMESPACE" --ignore-not-found >/dev/null   # name used by the first version
echo "applied (values from Terraform state and your session, never stored in git)"

# ------------------------------------------------------------- 7. code bundle
step "Packaging the pipeline code"
tar -czf "$TMP_DIR/bundle.tar.gz" -C "$PIPELINE_DIR" \
    --exclude='./.venv' --exclude='./tests' --exclude='./notebooks' --exclude='./docs' --exclude='./deploy' \
    --exclude='__pycache__' --exclude='*.pyc' --exclude='.pytest_cache' --exclude='.coverage*' --exclude='htmlcov' \
    --exclude='./results' --exclude='.ipynb_checkpoints' .
bundle_bytes="$(wc -c < "$TMP_DIR/bundle.tar.gz")"
[ "$bundle_bytes" -le "$MAX_BUNDLE_BYTES" ] || die "code bundle is $bundle_bytes bytes (ConfigMap limit ~1 MiB)"
kubectl create configmap "${JOB_NAME}-code" -n "$NAMESPACE" --from-file=bundle.tar.gz="$TMP_DIR/bundle.tar.gz" \
    --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f - >/dev/null
echo "ConfigMap ${JOB_NAME}-code applied ($bundle_bytes bytes)"

# ------------------------------------------------------------------- 8. job
step "Starting Job $JOB_NAME (stages: $STAGES)"
kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < "$SCRIPT_DIR/k8s/job.yaml" > "$TMP_DIR/job.yaml"
kubectl apply -f "$TMP_DIR/job.yaml" >/dev/null
echo "job created"

if [ "$FOLLOW_LOGS" != "true" ]; then
    echo "Follow with: kubectl logs -f job/$JOB_NAME"
    exit 0
fi

step "Waiting for the pod to start"
pod=""
for _ in $(seq 1 120); do
    pod="$(kubectl get pods -n "$NAMESPACE" -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    phase="$( [ -n "$pod" ] && kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [ -n "$phase" ] && [ "$phase" != "Pending" ]; then break; fi
    sleep 5
done
if [ -z "$pod" ] || [ "${phase:-Pending}" = "Pending" ]; then
    [ -n "$pod" ] && kubectl describe pod "$pod" -n "$NAMESPACE" | sed -n '/^Events:/,$p'
    die "pod did not start within 10 minutes (see events above)"
fi
echo "pod $pod is $phase"

step "Pipeline logs"
kubectl logs -f "$pod" -n "$NAMESPACE" || true

waited=0
while :; do
    succeeded="$(kubectl get job "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    failed="$(kubectl get job "$JOB_NAME" -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
    [ "${succeeded:-0}" -ge 1 ] && break
    [ "${failed:-0}" -ge 1 ] && die "the pipeline job FAILED. Logs: kubectl logs job/$JOB_NAME"
    [ "$waited" -ge "$WAIT_TIMEOUT_S" ] && die "job still running after ${WAIT_TIMEOUT_S}s: kubectl logs -f job/$JOB_NAME"
    sleep 5
    waited=$((waited + 5))
done

step "SUCCESS: Gold tables in s3://$DATALAKE_BUCKET/gold/"
aws s3 ls "s3://$DATALAKE_BUCKET/gold/"
