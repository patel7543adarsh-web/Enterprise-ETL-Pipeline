"""
Tests for Week 1 Day 6-7: Rate-Limit Handling and S3 Raw Data Staging.
"""

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
import requests

from extractors.base import BaseExtractor, RateLimitExceeded, TokenBucketRateLimiter
from storage.s3_client import S3DataLakeWriter


class TestRateLimiter:
    """Validates TokenBucketRateLimiter algorithms and throttling."""

    def test_token_bucket_acquire_within_capacity(self):
        limiter = TokenBucketRateLimiter(rate_limit_rps=100.0, burst_capacity=10.0)
        # Should acquire immediately without sleep
        wait_time = limiter.acquire(1.0)
        assert wait_time == 0.0

    def test_token_bucket_throttles_when_exhausted(self):
        limiter = TokenBucketRateLimiter(rate_limit_rps=20.0, burst_capacity=1.0)
        # Consume available token
        limiter.acquire(1.0)

        # Immediate next acquire should throttle
        start = time.monotonic()
        wait_time = limiter.acquire(1.0)
        elapsed = time.monotonic() - start

        assert wait_time > 0.0
        assert elapsed >= 0.03  # Waited approximately 1/20 = 0.05s


class DummyExtractor(BaseExtractor):
    def extract_entity_pages(self, entity, watermark=None, cursor=None, limit=100):
        yield [{"id": "dummy_1"}], None, False


class TestRetryAndRateLimitHandling:
    """Tests Tenacity retry behavior on 429 and 500 status codes."""

    def test_handles_429_with_retry_after(self, mock_settings, mock_storage_writer, mock_state_store):
        extractor = DummyExtractor(
            source_name="test_source",
            rate_limit_rps=100.0,
            max_retries=3,
            storage_writer=mock_storage_writer,
            state_store=mock_state_store,
        )

        call_count = 0

        def mock_send(request, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = requests.Response()
            if call_count == 1:
                # First call fails with 429 Too Many Requests
                resp.status_code = 429
                resp.headers["Retry-After"] = "0.01"
                resp._content = b'{"error": "rate_limited"}'
            else:
                # Second call succeeds
                resp.status_code = 200
                resp._content = b'{"data": "success"}'
            return resp

        extractor.session.send = mock_send

        # Should retry after 429 and succeed
        response = extractor.execute_request_with_retry("GET", "https://api.example.com/test")
        assert response.status_code == 200
        assert call_count == 2


class TestS3DataLakeWriter:
    """Tests S3 partitioned raw JSON writer, gzip compression, and file reading."""

    def test_partition_key_construction(self, mock_storage_writer):
        ts = datetime(2024, 5, 12, 14, 30, 0, tzinfo=timezone.utc)
        key = mock_storage_writer.build_partition_key(
            source="stripe",
            entity="customers",
            batch_id="batch_001",
            timestamp=ts,
            compressed=True,
        )
        assert key == "raw/stripe/customers/year=2024/month=05/day=12/batch_001.json.gz"

    def test_write_and_read_raw_batch(self, mock_storage_writer):
        sample_records = [
            {"id": "cus_001", "name": "Alice", "amount": 100},
            {"id": "cus_002", "name": "Bob", "amount": 200},
        ]

        uri = mock_storage_writer.write_raw_batch(
            source="stripe",
            entity="customers",
            records=sample_records,
            batch_id="test_batch_123",
            compress=True,
        )

        assert uri is not None

        # Read back batch
        read_payload = mock_storage_writer.read_raw_batch(uri)
        assert read_payload["metadata"]["source"] == "stripe"
        assert read_payload["metadata"]["record_count"] == 2
        assert len(read_payload["records"]) == 2
        assert read_payload["records"][0]["id"] == "cus_001"

    def test_list_raw_batches(self, mock_storage_writer):
        mock_storage_writer.write_raw_batch(
            source="salesforce",
            entity="accounts",
            records=[{"Id": "001"}],
            batch_id="batch_a",
        )
        mock_storage_writer.write_raw_batch(
            source="salesforce",
            entity="accounts",
            records=[{"Id": "002"}],
            batch_id="batch_b",
        )

        batches = mock_storage_writer.list_raw_batches("salesforce", "accounts")
        assert len(batches) >= 2
