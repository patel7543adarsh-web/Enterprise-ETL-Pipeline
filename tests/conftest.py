"""
Pytest configuration, shared fixtures, and mock instances.
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import Generator
import pytest
from pydantic import SecretStr

from config.settings import AppSettings, PipelineSettings, SalesforceSettings, StorageSettings, StripeSettings
from storage.s3_client import S3DataLakeWriter
from storage.state_store import IncrementalStateStore


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Provides a clean temporary directory for tests and deletes it afterwards."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_settings(temp_dir: Path) -> AppSettings:
    """Provides custom AppSettings configured for local test directory."""
    return AppSettings(
        stripe=StripeSettings(
            api_key=SecretStr("sk_test_testkey12345"),
            base_url="https://api.stripe.com/v1",
            page_limit=10,
            rate_limit_rps=50.0,
        ),
        salesforce=SalesforceSettings(
            access_token=SecretStr("mock_token_12345"),
            instance_url="https://test.salesforce.com",
            api_version="v59.0",
            page_limit=10,
            rate_limit_rps=50.0,
        ),
        storage=StorageSettings(
            storage_mode="local",
            s3_bucket_name="test-bucket",
            local_storage_path=str(temp_dir / "staging"),
        ),
        pipeline=PipelineSettings(
            environment="development",
            max_retries=3,
        ),
    )


@pytest.fixture
def mock_storage_writer(mock_settings: AppSettings) -> S3DataLakeWriter:
    """Provides an isolated S3DataLakeWriter in local mode."""
    return S3DataLakeWriter(settings=mock_settings.storage)


@pytest.fixture
def mock_state_store(temp_dir: Path) -> IncrementalStateStore:
    """Provides an isolated IncrementalStateStore in temp directory."""
    return IncrementalStateStore(state_file_path=str(temp_dir / "state.json"))
