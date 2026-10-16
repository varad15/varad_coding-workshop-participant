"""Unit tests: the insights-api Lambda (backend/insights-api/function.py)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

FUNCTION_PY = Path(__file__).resolve().parents[4] / "backend" / "insights-api" / "function.py"


@pytest.fixture()
def api(monkeypatch):
    spec = importlib.util.spec_from_file_location("insights_api_function", FUNCTION_PY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    monkeypatch.setenv("APP_ID", "2188ea91")
    module.s3_client.cache_clear()
    return module


class FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3:
    """Minimal stand-in for boto3's S3 client."""

    def __init__(self, objects: dict[str, bytes], error_code: str | None = None):
        self.objects, self.error_code = objects, error_code
        self.requested: list[tuple[str, str]] = []

    def get_object(self, Bucket: str, Key: str):
        from botocore.exceptions import ClientError

        self.requested.append((Bucket, Key))
        if self.error_code:
            raise ClientError({"Error": {"Code": self.error_code}}, "GetObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": FakeBody(self.objects[Key])}


def _event(path: str, method: str = "GET") -> dict:
    return {"rawPath": path, "requestContext": {"http": {"method": method}}}


def test_serves_dataset_from_the_participants_data_lake(api, monkeypatch):
    fake = FakeS3({"gold/_api/summary.json": b'{"kpis": {"employees": 2000}}'})
    monkeypatch.setattr(api, "s3_client", lambda: fake)
    resp = api.handler(_event("/api/insights-api/summary"))
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["kpis"]["employees"] == 2000
    assert fake.requested == [("coding-workshop-datalake-2188ea91", "gold/_api/summary.json")]
    assert resp["headers"]["Access-Control-Allow-Origin"] == "*"


def test_unknown_dataset_is_404_and_never_hits_s3(api, monkeypatch):
    fake = FakeS3({})
    monkeypatch.setattr(api, "s3_client", lambda: fake)
    resp = api.handler(_event("/api/insights-api/../etc/passwd"))
    assert resp["statusCode"] == 404 and fake.requested == []


def test_missing_export_explains_the_pipeline_must_run(api, monkeypatch):
    monkeypatch.setattr(api, "s3_client", lambda: FakeS3({}))
    resp = api.handler(_event("/api/insights-api/teams"))
    assert resp["statusCode"] == 404 and "pipeline" in json.loads(resp["body"])["error"]


def test_access_denied_maps_to_503_with_fallback_hint(api, monkeypatch):
    monkeypatch.setattr(api, "s3_client", lambda: FakeS3({}, error_code="AccessDenied"))
    resp = api.handler(_event("/api/insights-api/summary"))
    assert resp["statusCode"] == 503 and "snapshot" in json.loads(resp["body"])["hint"]


def test_index_health_and_method_handling(api, monkeypatch):
    monkeypatch.setattr(api, "s3_client", lambda: FakeS3({}))
    assert "endpoints" in json.loads(api.handler(_event("/api/insights-api/"))["body"])
    assert api.handler(_event("/health"))["statusCode"] == 200
    assert api.handler(_event("/summary", method="OPTIONS"))["statusCode"] == 200
    assert api.handler(_event("/summary", method="POST"))["statusCode"] == 405


def test_every_exported_dataset_is_whitelisted(api):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from gold import api_export  # noqa: PLC0415

    exported = {"meta", "summary", "performance", "skills", "promotion", "learning", "attrition", "teams", "coverage", "extras"}
    assert api.DATASETS == exported
    assert api_export.API_PREFIX == "gold/_api"
