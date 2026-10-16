#!/usr/bin/env bash
# Script: Stock-deployment prerequisites (run BEFORE ./bin/deploy-backend.sh)
#
# The organisers' deploy path provisions data/acme-talent-pipeline as an EKS
# Kubernetes Job (infra/locals.tf -> infra/eks.tf -> infra/helm), but the stock
# chart ships only job.py + requirements.txt and injects no environment, no AWS
# identity and no database settings. This script stages everything job.py's
# bootstrap mode needs, without touching bin/, infra/ or any organiser script:
#
#   1. landing data            -> s3://<datalake>/landing/...
#   2. source bundle           -> s3://<datalake>/_deploy/src.tar.gz
#   3. job environment         -> Kubernetes Secret acme-talent-pipeline-secrets
#   4. Secret read permission  -> Role + RoleBinding for the default ServiceAccount
#
# Usage:
#   source ENVIRONMENT.config            # repo root; refresh credentials first
#   ./data/acme-talent-pipeline/deploy/cloud-prereqs.sh
#   TF_VAR_aws_eks_enabled=true ./bin/deploy-backend.sh
#   ./data/acme-talent-pipeline/deploy/open-aurora-network.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd -P)"
PIPELINE_DIR="$(cd "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd "$PIPELINE_DIR/../.." >/dev/null 2>&1 && pwd -P)"
INFRA_DIR="$REPO_ROOT/infra"
SOURCE_DIR="$REPO_ROOT/backend/_source"
NAMESPACE="default"
JOB_NAME="acme-talent-pipeline"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

die()  { echo "ERROR: $*" >&2; exit 1; }
step() { echo ""; echo "==> $*"; }

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
echo "data lake:    s3://$DATALAKE_BUCKET"

# ------------------------------------------------------------ 3. kubectl context
step "Connecting kubectl to $EKS_CLUSTER"
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_REGION" >/dev/null
kubectl get nodes --no-headers >/dev/null || die "cannot reach the EKS cluster"

# ------------------------------------------------------------- 4. data lake sync
step "Data lake bucket + landing data"
aws s3api head-bucket --bucket "$DATALAKE_BUCKET" 2>/dev/null \
    || aws s3api create-bucket --bucket "$DATALAKE_BUCKET" \
         $( [ "$AWS_REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$AWS_REGION" ) >/dev/null
aws s3 sync "$SOURCE_DIR/learning_activity" "s3://$DATALAKE_BUCKET/landing/learning_activity/" \
    --exclude "*" --include "*.jsonl" --only-show-errors
aws s3 sync "$SOURCE_DIR" "s3://$DATALAKE_BUCKET/landing/postgres_seed/" \
    --exclude "*" --include "*.csv" --include "schema.sql" --only-show-errors
echo "landing/ synced"

# ------------------------------------------------------------- 5. source bundle
# The stock chart ships only job.py; the bootstrap downloads the rest from here.
step "Source bundle s3://$DATALAKE_BUCKET/_deploy/src.tar.gz"
tar -czf "$TMP_DIR/src.tar.gz" -C "$PIPELINE_DIR" \
    --exclude='tests' --exclude='deploy' --exclude='docs' \
    --exclude='__pycache__' --exclude='.pytest_cache' --exclude='*.ipynb' \
    .
aws s3 cp "$TMP_DIR/src.tar.gz" "s3://$DATALAKE_BUCKET/_deploy/src.tar.gz" --only-show-errors
echo "bundle uploaded ($(du -h "$TMP_DIR/src.tar.gz" | cut -f1))"

# --------------------------------------------------------------- 6. job secrets
# Pods on the workshop's EKS nodes cannot obtain the node role's AWS credentials
# (IMDS hop limit 1), and IRSA would need infra changes; the job gets the
# participant's temporary credentials, refreshed on every run of this script.
# The stock chart injects no env at all, so this Secret carries the full job
# environment and job.py's bootstrap exports it inside the pod.
step "Kubernetes Secret ${JOB_NAME}-secrets"
umask 077
{
    printf 'IS_LOCAL=false\n'
    printf 'APP_ID=%s\n'           "$APP_ID"
    printf 'AWS_REGION=%s\n'       "$AWS_REGION"
    printf 'DATALAKE_BUCKET=%s\n'  "$DATALAKE_BUCKET"
    printf 'POSTGRES_HOST=%s\n'    "$PG_HOST"
    printf 'POSTGRES_PORT=%s\n'    "$PG_PORT"
    printf 'POSTGRES_NAME=%s\n'    "$PG_NAME"
    printf 'POSTGRES_USER=%s\n'    "$PG_USER"
    printf 'POSTGRES_PASS=%s\n'    "$PG_PASS"
} > "$TMP_DIR/job.env"
if ! aws configure export-credentials --format env-no-export > "$TMP_DIR/aws.env" 2>/dev/null; then
    : > "$TMP_DIR/aws.env"
    for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
        [ -n "${!var:-}" ] && printf '%s=%s\n' "$var" "${!var}" >> "$TMP_DIR/aws.env"
    done
fi
grep -q '^AWS_ACCESS_KEY_ID=' "$TMP_DIR/aws.env" \
    || die "could not read your AWS credentials: run 'source ENVIRONMENT.config' and retry"
grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' "$TMP_DIR/aws.env" \
    | sed -E "s/^([A-Z_]+)=[\"'](.*)[\"']$/\1=\2/" >> "$TMP_DIR/job.env"
expiry="$(sed -n 's/^AWS_CREDENTIAL_EXPIRATION=//p' "$TMP_DIR/aws.env")"
kubectl create secret generic "${JOB_NAME}-secrets" -n "$NAMESPACE" --from-env-file="$TMP_DIR/job.env" \
    --dry-run=client -o yaml | kubectl apply --server-side --force-conflicts -f - >/dev/null
rm -f "$TMP_DIR/job.env" "$TMP_DIR/aws.env"
unset PG_PASS
echo "Secret written${expiry:+ (credentials valid until $expiry)}"

# ------------------------------------------- 7. let the pod read its own Secret
# The stock chart mounts the default ServiceAccount, which has no RBAC at all.
# Grant it read access to this one named Secret, nothing else.
step "RBAC: default ServiceAccount may read ${JOB_NAME}-secrets"
kubectl apply --server-side -f - >/dev/null <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${JOB_NAME}-secret-reader
  namespace: ${NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["${JOB_NAME}-secrets"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${JOB_NAME}-secret-reader
  namespace: ${NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: default
    namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: ${JOB_NAME}-secret-reader
  apiGroup: rbac.authorization.k8s.io
EOF
echo "Role + RoleBinding applied"

step "Done - next steps"
cat <<EOF
  1. TF_VAR_aws_eks_enabled=true ./bin/deploy-backend.sh     # stock deploy; (re)runs the job
  2. ./data/acme-talent-pipeline/deploy/open-aurora-network.sh   # terraform resets Aurora's SGs on every apply
  3. kubectl get jobs -n ${NAMESPACE} | grep ${JOB_NAME}
     kubectl logs -f -n ${NAMESPACE} -l job-name=\$(kubectl get jobs -n ${NAMESPACE} -o name | grep ${JOB_NAME} | cut -d/ -f2)
  (the Job retries up to 4 times, so a first attempt that starts before step 2
   finishes simply fails on the database connect and the retry succeeds)
EOF
