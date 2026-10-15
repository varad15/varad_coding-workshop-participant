"""
insights-api: read-only Lambda serving the ACME talent dashboard.

Deployed automatically by the workshop's own ./bin/deploy-backend.sh (any
backend/<name>/ folder with requirements.txt becomes a Lambda with a function
URL, reachable through CloudFront at /api/insights-api/*).

It serves the chart-ready JSON files the data pipeline writes to
s3://coding-workshop-datalake-<APP_ID>/gold/_api/ (see
backend/acme-talent-pipeline/gold/api_export.py). No database access and no
heavy dependencies: boto3 ships with the Lambda runtime.

Routes (GET):
    /api/insights-api/            -> list of endpoints
    /api/insights-api/health      -> {"status": "ok"}
    /api/insights-api/<name>      -> gold/_api/<name>.json  (e.g. /summary)
"""

from __future__ import annotations

import json
import os
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

# Whitelist: only files the pipeline actually exports can be requested.
DATASETS = {
    "meta", "summary", "performance", "skills", "promotion",
    "learning", "attrition", "teams", "coverage", "extras",
}

HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "max-age=60",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
}


def bucket_name() -> str:
    """The data lake bucket; APP_ID is injected by the workshop Terraform."""
    explicit = os.environ.get("DATALAKE_BUCKET")
    if explicit:
        return explicit
    app_id = os.environ.get("APP_ID", "abcd1234")
    return f"coding-workshop-datalake-{app_id}"


@lru_cache(maxsize=1)
def s3_client():
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or None
    return boto3.client("s3", endpoint_url=endpoint)


def _response(status: int, body: dict | list) -> dict:
    return {"statusCode": status, "headers": dict(HEADERS), "body": json.dumps(body)}


def handler(event, context=None):
    """Lambda entry point (function URL / CloudFront, payload v2)."""
    method = (event.get("requestContext", {}).get("http", {}) or {}).get("method", "GET")
    if method == "OPTIONS":  # CORS preflight
        return _response(200, {})
    if method != "GET":
        return _response(405, {"error": "only GET is supported"})

    path = (event.get("rawPath") or "/").rstrip("/")
    name = path.split("/")[-1].lower() if path else ""

    if name in ("", "api", "insights-api"):
        return _response(200, {"service": "insights-api", "endpoints": sorted(DATASETS), "health": "/health"})
    if name == "health":
        return _response(200, {"status": "ok", "bucket": bucket_name()})
    if name not in DATASETS:
        return _response(404, {"error": f"unknown dataset '{name}'", "available": sorted(DATASETS)})

    try:
        obj = s3_client().get_object(Bucket=bucket_name(), Key=f"gold/_api/{name}.json")
        return {"statusCode": 200, "headers": dict(HEADERS), "body": obj["Body"].read().decode("utf-8")}
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return _response(404, {"error": f"'{name}' not generated yet - run the data pipeline first"})
        if code in ("AccessDenied", "403"):
            return _response(503, {"error": "the Lambda role has no access to the data lake bucket",
                                   "hint": "see docs/limitations.md (dashboard falls back to its bundled snapshot)"})
        return _response(502, {"error": f"S3 error {code or 'unknown'}"})
