#!/usr/bin/env bash
# Remove the pipeline's Kubernetes resources (Job, code ConfigMap, DB Secret).
# The data lake bucket and its data are kept; delete them yourself if you need to:
#     aws s3 rb s3://<project>-datalake-<app-id> --force
set -euo pipefail

JOB_NAME="acme-talent-pipeline"
NAMESPACE="${NAMESPACE:-default}"

kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found
kubectl delete configmap "${JOB_NAME}-code" -n "$NAMESPACE" --ignore-not-found
kubectl delete secret "${JOB_NAME}-db" -n "$NAMESPACE" --ignore-not-found
echo "Removed the Kubernetes resources of $JOB_NAME (data lake bucket kept)."
