"""Unit tests: cloud deployment helpers in deploy/ (Terraform state reader, Job manifest)."""

from __future__ import annotations

import importlib.util
import os
import string
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

DEPLOY_DIR = Path(__file__).resolve().parents[2] / "deploy"


def _load_stack_module():
    spec = importlib.util.spec_from_file_location("stack_from_state", DEPLOY_DIR / "stack_from_state.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


stack = _load_stack_module()


def _resource(rtype: str, attributes: dict, name: str = "this") -> dict:
    return {"mode": "managed", "type": rtype, "name": name, "instances": [{"attributes": attributes}]}


FULL_STATE = {
    "resources": [
        _resource("aws_s3_bucket", {"bucket": "coding-workshop-website-2188ea91"}),
        _resource("aws_s3_bucket", {"bucket": "hot-reload"}, name="hot_reload"),
        _resource("aws_eks_cluster", {"name": "coding-workshop-2188ea91"}),
        _resource(
            "aws_rds_cluster",
            {"endpoint": "db.cluster-x.ap-south-1.rds.amazonaws.com", "port": 5432, "database_name": "postgres",
             "master_username": "postgres", "master_password": "brave-otter"},
        ),
    ]
}


def test_reads_cluster_database_and_bucket_from_state():
    s = stack.read_stack(FULL_STATE)
    assert s["APP_ID"] == "2188ea91"
    assert s["DATALAKE_BUCKET"] == "coding-workshop-datalake-2188ea91"
    assert s["EKS_CLUSTER"] == "coding-workshop-2188ea91"
    assert s["PG_HOST"].endswith("rds.amazonaws.com") and s["PG_PORT"] == "5432"
    assert s["PG_PASS"] == "brave-otter"


@pytest.mark.parametrize("missing, message", [("aws_eks_cluster", "TF_VAR_aws_eks_enabled"), ("aws_rds_cluster", "Aurora")])
def test_missing_platform_resource_is_a_clear_error(missing, message):
    state = {"resources": [r for r in FULL_STATE["resources"] if r["type"] != missing]}
    with pytest.raises(stack.StackError, match=message):
        stack.read_stack(state)


def test_cli_prints_shell_quoted_exports_and_fails_cleanly():
    import json

    ok = subprocess.run([sys.executable, str(DEPLOY_DIR / "stack_from_state.py")], input=json.dumps(FULL_STATE),
                        capture_output=True, text=True, check=False)
    assert ok.returncode == 0 and "export PG_PASS=brave-otter" in ok.stdout
    env = subprocess.run(["bash", "-c", 'eval "$(cat)"; echo "$DATALAKE_BUCKET"'], input=ok.stdout, capture_output=True, text=True)
    assert env.stdout.strip() == "coding-workshop-datalake-2188ea91"

    bad = subprocess.run([sys.executable, str(DEPLOY_DIR / "stack_from_state.py")], input='{"resources": []}',
                         capture_output=True, text=True, check=False)
    assert bad.returncode == 1 and bad.stdout.startswith("echo ") and "exit 1" in bad.stdout


def test_job_manifest_renders_to_a_valid_job():
    values = {"JOB_NAME": "acme-talent-pipeline", "NAMESPACE": "default", "JOB_IMAGE": "jupyter/all-spark-notebook:latest",
              "STAGES": "seed,bronze,silver,gold", "APP_ID": "2188ea91", "AWS_REGION": "ap-south-1",
              "DATALAKE_BUCKET": "coding-workshop-datalake-2188ea91", "PG_HOST": "db.example", "PG_PORT": "5432",
              "PG_NAME": "postgres", "PG_USER": "postgres"}
    rendered = string.Template((DEPLOY_DIR / "k8s" / "job.yaml").read_text()).substitute(values)
    job = yaml.safe_load(rendered)
    assert job["kind"] == "Job" and job["spec"]["backoffLimit"] == 0
    container = job["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["IS_LOCAL"] == "false" and env["DATALAKE_BUCKET"] == values["DATALAKE_BUCKET"]
    assert "POSTGRES_PASS" not in env                       # password only via the Secret
    assert container["envFrom"][0]["secretRef"]["name"] == "acme-talent-pipeline-db"
    assert container["resources"]["limits"]["memory"] == "3Gi"
    assert "python job.py --stages seed,bronze,silver,gold" in container["args"][0]


def test_deploy_scripts_are_executable():
    for name in ("deploy.sh", "undeploy.sh"):
        assert os.access(DEPLOY_DIR / name, os.X_OK), name
