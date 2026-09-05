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


class WarehouseSettings(BaseSettings):
    """Data Warehouse connection and loading configuration."""
    model_config = SettingsConfigDict(env_prefix="DB_", extra="ignore", populate_by_name=True)

    db_type: Literal["postgres", "snowflake", "sqlite"] = Field(
        default="sqlite",
        description="Target warehouse dialect: 'postgres', 'snowflake', or 'sqlite'"
    )
    connection_url: Optional[SecretStr] = Field(
        default=None,
        description="Full SQLAlchemy database connection URL (overrides individual credentials if set)"
    )
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    user: str = Field(default="postgres", description="Database username")
    password: SecretStr = Field(default=SecretStr("postgres"), description="Database password")
    database: str = Field(default="enterprise_dw", description="Target database name")
    schema_name: str = Field(default="public", alias="DB_SCHEMA", description="Database schema name")
    sqlite_db_path: str = Field(
        default="./data_lake_staging/warehouse.db",
        description="Local SQLite DB file path when db_type is sqlite"
    )
    pool_size: int = Field(default=5, ge=1, le=50, description="SQLAlchemy connection pool size")
    max_overflow: int = Field(default=10, ge=0, description="SQLAlchemy max pool overflow")
    pool_timeout: int = Field(default=30, description="Connection pool timeout in seconds")
    echo_sql: bool = Field(default=False, description="Log raw SQL statements executed by SQLAlchemy")

    def get_connection_url(self) -> str:
        """Returns the resolved SQLAlchemy connection string."""
        if self.connection_url:
            return self.connection_url.get_secret_value()
        
        if self.db_type == "sqlite":
            return f"sqlite:///{self.sqlite_db_path}"
        elif self.db_type == "postgres":
            pwd = self.password.get_secret_value()
            return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == "snowflake":
            pwd = self.password.get_secret_value()
            return f"snowflake://{self.user}:{pwd}@{self.host}/{self.database}/{self.schema_name}"
        return f"sqlite:///{self.sqlite_db_path}"


class NotificationSettings(BaseSettings):
    """Alerting & notification settings for pipeline events."""
    model_config = SettingsConfigDict(env_prefix="ALERT_", extra="ignore")

    slack_enabled: bool = Field(default=False, description="Enable Slack webhook notifications")
    slack_webhook_url: Optional[SecretStr] = Field(default=None, description="Slack Incoming Webhook URL")
    slack_channel: str = Field(default="#data-engineering-alerts", description="Target Slack Channel")
    
    email_enabled: bool = Field(default=False, description="Enable SMTP Email alerts")
    smtp_host: str = Field(default="smtp.gmail.com", description="SMTP host server")
    smtp_port: int = Field(default=587, description="SMTP port")
    smtp_user: Optional[str] = Field(default=None, description="SMTP username / email")
    smtp_password: Optional[SecretStr] = Field(default=None, description="SMTP password / app key")
    email_from: str = Field(default="etl-alerts@example.com", description="Sender email address")
    email_to: str = Field(default="data-team@example.com", description="Recipient email address(es)")
    
    alert_on_success: bool = Field(default=True, description="Send notifications upon pipeline success")
    alert_on_failure: bool = Field(default=True, description="Send notifications upon pipeline failure")


class AirflowSettings(BaseSettings):
    """Airflow orchestration scheduling parameters."""
    model_config = SettingsConfigDict(env_prefix="AIRFLOW_", extra="ignore")

    dag_schedule_daily: str = Field(default="0 2 * * *", description="Cron schedule for Daily Full ETL DAG")
    dag_schedule_hourly: str = Field(default="0 * * * *", description="Cron schedule for Hourly Incremental DAG")
    retries: int = Field(default=2, description="Airflow task retries on failure")
    retry_delay_seconds: int = Field(default=300, description="Seconds between Airflow task retries")
    sla_minutes: int = Field(default=60, description="Task SLA threshold in minutes")


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
    warehouse: WarehouseSettings = Field(default_factory=WarehouseSettings)
    notification: NotificationSettings = Field(default_factory=NotificationSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)


@lru_cache()
def get_settings() -> AppSettings:
    """Returns a cached singleton instance of AppSettings."""
    return AppSettings()

