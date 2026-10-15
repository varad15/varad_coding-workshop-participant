#!/usr/bin/env bash
# Remove the pipeline's Kubernetes resources (Job, code ConfigMap, Secret).
# The data lake bucket and its data are kept; delete them yourself if you need to:
#     aws s3 rb s3://<project>-datalake-<app-id> --force
set -euo pipefail

JOB_NAME="acme-talent-pipeline"
NAMESPACE="${NAMESPACE:-default}"

kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found
kubectl delete configmap "${JOB_NAME}-code" -n "$NAMESPACE" --ignore-not-found
kubectl delete secret "${JOB_NAME}-secrets" "${JOB_NAME}-db" -n "$NAMESPACE" --ignore-not-found
echo "Removed the Kubernetes resources of $JOB_NAME (data lake bucket kept)."

# Remove the tcp/5432 rule deploy.sh added (EKS nodes -> Aurora). Best effort.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/../../../infra" && pwd)"
if stack="$(terraform -chdir="$INFRA_DIR" state pull 2>/dev/null | python3 "$SCRIPT_DIR/stack_from_state.py" 2>/dev/null)"; then
    eval "$stack"
    for sg in $AURORA_SG_IDS; do
        [ -n "$EKS_CLUSTER_SG" ] || break
        rule='[{"IpProtocol":"tcp","FromPort":5432,"ToPort":5432,"UserIdGroupPairs":[{"GroupId":"'"$EKS_CLUSTER_SG"'"}]}]'
        aws ec2 revoke-security-group-ingress --group-id "$sg" --ip-permissions "$rule" >/dev/null 2>&1 \
            && echo "Removed tcp/5432 from $EKS_CLUSTER_SG on $sg"
    done
fi
