"""
Runtime configuration, read only from environment variables.

The same code runs locally (VDI + LocalStack + local PostgreSQL) and in the
cloud (EKS + S3 + Aurora). Only environment variables differ between the two:

| Variable              | Local (IS_LOCAL=true)                  | Cloud (IS_LOCAL=false)        |
|-----------------------|----------------------------------------|-------------------------------|
| POSTGRES_HOST/PORT    | localhost / 5432                       | Aurora endpoint / 5432        |
| POSTGRES_NAME/USER/PASS | postgres / postgres / postgres123    | injected by Terraform         |
| AWS_ENDPOINT_URL_S3   | http://s3.localhost.localstack.cloud:4566 | (unset -> real AWS S3)     |
| DATALAKE_BUCKET       | coding-workshop-datalake-<id>          | coding-workshop-datalake-<id> |

Defaults mirror the workshop repo (bin/setup-environment.sh, infra/locals.tf).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

_TRUE_VALUES = {"1", "true", "yes", "y", "on"}

# Default location of the sample dataset inside the repository (backend/_source).
_DEFAULT_SOURCE_DIR = Path(__file__).resolve().parents[2] / "_source"


def _as_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean environment value such as 'true' / 'false'."""
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in _TRUE_VALUES


def _as_int(value: str | None, default: int) -> int:
    """Parse an integer environment value, falling back to a default."""
    if value is None or value.strip() == "":
        return default
    return int(value)


@dataclass(frozen=True)
class PostgresSettings:
    """Connection settings for the PostgreSQL source system."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    sslmode: str

    def conninfo(self) -> str:
        """Return a libpq connection string (password included, never log it)."""
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password} sslmode={self.sslmode} "
            f"connect_timeout=10"
        )


@dataclass(frozen=True)
class S3Settings:
    """Settings for the S3 data lake (real AWS S3 or LocalStack)."""

    bucket: str
    region: str
    endpoint_url: str | None

    @property
    def scheme(self) -> str:
        """URL scheme of the endpoint ('http' for LocalStack, 'https' for AWS)."""
        if self.endpoint_url and self.endpoint_url.startswith("http://"):
            return "http"
        return "https"


@dataclass(frozen=True)
class Settings:
    """All runtime settings for one pipeline run."""

    is_local: bool
    postgres: PostgresSettings
    s3: S3Settings
    source_dir: Path
    seed_sources: bool
    retry_attempts: int
    spark_master: str
    log_level: str

    def describe(self) -> dict[str, object]:
        """Return a log-safe summary of the settings (no secrets)."""
        return {
            "is_local": self.is_local,
            "postgres_host": self.postgres.host,
            "postgres_db": self.postgres.dbname,
            "postgres_sslmode": self.postgres.sslmode,
            "s3_bucket": self.s3.bucket,
            "s3_endpoint": self.s3.endpoint_url or "aws-default",
            "s3_region": self.s3.region,
            "source_dir": str(self.source_dir),
            "seed_sources": self.seed_sources,
            "spark_master": self.spark_master,
        }


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """
    Build Settings from environment variables.

    Args:
        env: Mapping to read from; defaults to os.environ (injectable for tests).

    Returns:
        A frozen Settings object.
    """
    env = os.environ if env is None else env
    is_local = _as_bool(env.get("IS_LOCAL"), default=True)

    participant = (
        env.get("APP_ID") or env.get("PARTICIPANT_ID") or env.get("TF_VAR_aws_app_code") or "abcd1234"
    )

    postgres = PostgresSettings(
        host=env.get("POSTGRES_HOST") or "localhost",
        port=_as_int(env.get("POSTGRES_PORT"), 5432),
        dbname=env.get("POSTGRES_NAME") or "postgres",
        user=env.get("POSTGRES_USER") or "postgres",
        password=env.get("POSTGRES_PASS") or "postgres123",
        # Aurora requires TLS; the local instance does not (see data-engineer.md tip).
        sslmode=env.get("POSTGRES_SSLMODE") or ("disable" if is_local else "require"),
    )

    endpoint = env.get("AWS_ENDPOINT_URL_S3") or env.get("AWS_ENDPOINT_URL") or None
    if endpoint is None and is_local:
        endpoint = "http://s3.localhost.localstack.cloud:4566"

    s3 = S3Settings(
        bucket=env.get("DATALAKE_BUCKET") or f"coding-workshop-datalake-{participant}",
        region=env.get("AWS_REGION") or env.get("APP_REGION") or "us-east-1",
        endpoint_url=endpoint,
    )

    return Settings(
        is_local=is_local,
        postgres=postgres,
        s3=s3,
        source_dir=Path(env.get("SOURCE_DATA_DIR") or _DEFAULT_SOURCE_DIR),
        seed_sources=_as_bool(env.get("SEED_SOURCES"), default=True),
        retry_attempts=_as_int(env.get("RETRY_ATTEMPTS"), 5),
        spark_master=env.get("SPARK_MASTER") or "local[*]",
        log_level=(env.get("LOG_LEVEL") or "INFO").upper(),
    )
