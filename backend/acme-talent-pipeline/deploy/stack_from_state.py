"""
Read the workshop stack (EKS cluster, Aurora, app id) from the stock Terraform state.

The workshop's infra/ outputs do not expose the Aurora endpoint or password, so the
deploy script reads them from `terraform state pull` (JSON on stdin) and this helper
prints them as shell `export` lines for `eval`. Values are shell-quoted; the password
is never printed anywhere else.

Usage:
    terraform -chdir=infra state pull | python3 stack_from_state.py
"""

from __future__ import annotations

import json
import shlex
import sys


class StackError(RuntimeError):
    """A resource the pipeline needs is missing from the Terraform state."""


def _attributes(state: dict, resource_type: str, name: str = "this") -> dict | None:
    for resource in state.get("resources", []):
        if resource.get("mode") == "managed" and resource.get("type") == resource_type and resource.get("name") == name:
            instances = resource.get("instances") or []
            if instances:
                return instances[0].get("attributes") or {}
    return None


def read_stack(state: dict) -> dict[str, str]:
    """Return the values the deploy script needs, or raise StackError."""
    website = _attributes(state, "aws_s3_bucket")
    if not website or "-website-" not in website.get("bucket", ""):
        raise StackError("website bucket not found in Terraform state: run ./bin/deploy-backend.sh first")
    project, app_id = website["bucket"].split("-website-", 1)

    eks = _attributes(state, "aws_eks_cluster")
    if not eks:
        raise StackError(
            "EKS cluster not found in Terraform state: export TF_VAR_aws_eks_enabled=true and run ./bin/deploy-backend.sh"
        )
    rds = _attributes(state, "aws_rds_cluster")
    if not rds:
        raise StackError("Aurora PostgreSQL not found in Terraform state: run ./bin/deploy-backend.sh (aws_postgres_enabled=true)")

    return {
        "APP_ID": app_id,
        "DATALAKE_BUCKET": f"{project}-datalake-{app_id}",
        "EKS_CLUSTER": eks["name"],
        "PG_HOST": rds["endpoint"],
        "PG_PORT": str(rds.get("port") or 5432),
        "PG_NAME": rds.get("database_name") or "postgres",
        "PG_USER": rds["master_username"],
        "PG_PASS": rds["master_password"],
    }


def main() -> int:
    try:
        stack = read_stack(json.load(sys.stdin))
    except (StackError, KeyError, ValueError) as exc:
        print(f"echo {shlex.quote(f'ERROR: {exc}')} >&2; exit 1")
        return 1
    for key, value in stack.items():
        print(f"export {key}={shlex.quote(value)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
