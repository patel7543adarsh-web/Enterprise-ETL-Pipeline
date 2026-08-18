"""
Enterprise ETL Pipeline Configuration Management.
Uses Pydantic Settings (v2) to validate and manage environment variables securely.
"""

from functools import lru_cache
from typing import Literal, Optional
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StripeSettings(BaseSettings):
    """Stripe API configuration settings."""
    model_config = SettingsConfigDict(env_prefix="STRIPE_", extra="ignore")

    api_key: SecretStr = Field(
        default=SecretStr("sk_test_placeholder_key"),
        description="Stripe secret API key"
    )
    base_url: str = Field(
        default="https://api.stripe.com/v1",
        description="Stripe API base URL"
    )
    page_limit: int = Field(
        default=100,
        ge=1,
        le=100,
        description="Page size limit (Stripe max is 100)"
    )
    rate_limit_rps: float = Field(
        default=25.0,
        gt=0,
        description="Allowed requests per second"
    )


class SalesforceSettings(BaseSettings):
    """Salesforce REST API configuration settings."""
    model_config = SettingsConfigDict(env_prefix="SALESFORCE_", extra="ignore")

    access_token: SecretStr = Field(
        default=SecretStr("placeholder_salesforce_token"),
        description="Salesforce OAuth Access Token"
    )
    instance_url: str = Field(
        default="https://your-org.my.salesforce.com",
        description="Salesforce instance URL"
    )
    api_version: str = Field(
        default="v59.0",
        description="Salesforce REST API version"
    )
    page_limit: int = Field(
        default=2000,
        ge=1,
        le=2000,
        description="Salesforce SOQL query batch size"
    )
    rate_limit_rps: float = Field(
        default=15.0,
        gt=0,
        description="Allowed requests per second"
    )

    @field_validator("instance_url")
    @classmethod
    def clean_instance_url(cls, v: str) -> str:
        return v.rstrip("/")


class StorageSettings(BaseSettings):
    """AWS S3 and Local Staging storage configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    storage_mode: Literal["s3", "local"] = Field(
        default="local",
        alias="STORAGE_MODE",
        description="Storage backend: 's3' or 'local'"
    )
    aws_access_key_id: Optional[SecretStr] = Field(
        default=None,
        alias="AWS_ACCESS_KEY_ID",
        description="AWS Access Key ID"
    )
    aws_secret_access_key: Optional[SecretStr] = Field(
        default=None,
        alias="AWS_SECRET_ACCESS_KEY",
        description="AWS Secret Access Key"
    )
    aws_region: str = Field(
        default="us-east-1",
        alias="AWS_REGION",
        description="AWS region name"
    )
    s3_bucket_name: str = Field(
        default="enterprise-etl-data-lake",
        alias="S3_BUCKET_NAME",
        description="Target S3 staging bucket"
    )
    s3_endpoint_url: Optional[str] = Field(
        default=None,
        alias="S3_ENDPOINT_URL",
        description="Custom S3 endpoint URL (for LocalStack / MinIO)"
    )
    local_storage_path: str = Field(
        default="./data_lake_staging",
        alias="LOCAL_STORAGE_PATH",
        description="Local directory path for raw data lake staging"
    )


class PipelineSettings(BaseSettings):
    """ETL Pipeline execution settings."""
    model_config = SettingsConfigDict(extra="ignore")

    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        alias="ENVIRONMENT"
    )
    log_level: str = Field(
        default="INFO",
        alias="LOG_LEVEL"
    )
    extraction_batch_size: int = Field(
        default=500,
        alias="EXTRACTION_BATCH_SIZE"
    )
    max_retries: int = Field(
        default=5,
        alias="MAX_RETRIES"
    )
    retry_initial_backoff: float = Field(
        default=1.0,
        alias="RETRY_INITIAL_BACKOFF"
    )
    retry_max_backoff: float = Field(
        default=60.0,
        alias="RETRY_MAX_BACKOFF"
    )


class AppSettings(BaseSettings):
    """Root Application Settings aggregating all sub-configurations."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    stripe: StripeSettings = Field(default_factory=StripeSettings)
    salesforce: SalesforceSettings = Field(default_factory=SalesforceSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)


@lru_cache()
def get_settings() -> AppSettings:
    """Returns a cached singleton instance of AppSettings."""
    return AppSettings()
