#!/usr/bin/env bash
# Script: Attach the EKS cluster security group to Aurora (run AFTER every
#         ./bin/deploy-backend.sh - terraform resets Aurora's security groups).
#
# Why: Aurora's stock security group only admits traffic from itself, so pods
# on the EKS worker nodes time out on port 5432. The EKS cluster security
# group has a self-referencing allow-all rule covering the worker nodes, so
# attaching it to the Aurora cluster opens exactly the EKS->Aurora path.
# rds:ModifyDBCluster is permitted to participants; no security-group RULE is
# added or changed anywhere, and bin/ and infra/ stay untouched.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." >/dev/null 2>&1 && pwd -P)"
INFRA_DIR="$REPO_ROOT/infra"

die() { echo "ERROR: $*" >&2; exit 1; }

export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
[ -n "$AWS_REGION" ] || die "AWS_REGION is not set: run 'source ENVIRONMENT.config' from the repo root"

TMP_STATE="$(mktemp)"
trap 'rm -f "$TMP_STATE"' EXIT
terraform -chdir="$INFRA_DIR" state pull > "$TMP_STATE" 2>/dev/null \
    || die "cannot read Terraform state. Run ./bin/deploy-backend.sh first"
eval "$(python3 "$SCRIPT_DIR/stack_from_state.py" < "$TMP_STATE")"

[ -n "$PG_CLUSTER_ID" ]  || die "Aurora cluster id not found in Terraform state"
[ -n "$EKS_CLUSTER_SG" ] || die "EKS cluster security group not found in Terraform state"

current="$(aws rds describe-db-clusters --db-cluster-identifier "$PG_CLUSTER_ID" \
    --query 'DBClusters[0].VpcSecurityGroups[].VpcSecurityGroupId' --output text)"
case " $current " in
  *" $EKS_CLUSTER_SG "*)
    echo "already attached: $EKS_CLUSTER_SG is on $PG_CLUSTER_ID - nothing to do"
    exit 0 ;;
esac

echo "attaching $EKS_CLUSTER_SG to $PG_CLUSTER_ID (keeping: $current)"
# shellcheck disable=SC2086
aws rds modify-db-cluster \
    --db-cluster-identifier "$PG_CLUSTER_ID" \
    --vpc-security-group-ids $current "$EKS_CLUSTER_SG" \
    --apply-immediately >/dev/null
echo "done - propagation takes ~2-5 minutes; the pipeline Job's retries cover the gap"
