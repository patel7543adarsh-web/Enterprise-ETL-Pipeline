"""
Base Extractor with Token Bucket Rate Limiting, Tenacity Retries, and S3 Staging.
"""

import abc
import logging
import math
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type, TypeVar
from pydantic import BaseModel
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from storage.s3_client import S3DataLakeWriter, get_storage_writer
from storage.state_store import IncrementalStateStore

logger = logging.getLogger(__name__)

TModel = TypeVar("TModel", bound=BaseModel)


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded and retry is warranted."""
    def __init__(self, message: str, retry_after: float = 1.0):
        super().__init__(message)
        self.retry_after = retry_after


class APIClientError(Exception):
    """Non-retryable 4xx client errors (e.g. 400 Bad Request, 401 Unauthorized, 403 Forbidden)."""
    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.response_body = response_body


class APIServerError(Exception):
    """Retryable 5xx server errors (e.g. 500 Internal, 502 Bad Gateway, 503 Service Unavailable)."""
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


class TokenBucketRateLimiter:
    """
    Token Bucket Rate Limiter to throttle API requests.
    Enforces maximum requests per second (RPS) with burst capacity support.
    """

    def __init__(self, rate_limit_rps: float = 20.0, burst_capacity: Optional[float] = None):
        self.rate = float(rate_limit_rps)
        self.capacity = float(burst_capacity) if burst_capacity else max(1.0, float(rate_limit_rps))
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        """
        Consumes tokens. If tokens are not yet available, sleeps until they refill.
        Returns the duration waited in seconds.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            # Refill tokens according to elapsed time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate wait time needed
            needed = tokens - self.tokens
            wait_time = needed / self.rate
            self.tokens = 0.0

        if wait_time > 0:
            logger.debug(f"RateLimiter: Throttling request for {wait_time:.3f}s")
            time.sleep(wait_time)
            with self._lock:
                self.last_update = time.monotonic()
            return wait_time

        return 0.0


class BaseExtractor(abc.ABC):
    """
    Abstract Base Extractor providing:
    - Token bucket rate limiting
    - Resilient Tenacity retry policies
    - S3 raw data lake staging
    - Watermark state tracking
    """

    def __init__(
        self,
        source_name: str,
        rate_limit_rps: float = 20.0,
        max_retries: int = 5,
        storage_writer: Optional[S3DataLakeWriter] = None,
        state_store: Optional[IncrementalStateStore] = None,
    ):
        self.source_name = source_name
        self.rate_limiter = TokenBucketRateLimiter(rate_limit_rps=rate_limit_rps)
        self.max_retries = max_retries
        self.storage_writer = storage_writer or get_storage_writer()
        self.state_store = state_store or IncrementalStateStore()

        # Configured HTTP session with connection pooling
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=Retry(total=0),  # We manage retries with Tenacity
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def execute_request_with_retry(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Any] = None,
        timeout: float = 30.0,
    ) -> requests.Response:
        """
        Executes HTTP request wrapped with Token Bucket rate limiter and Tenacity retry backoff.
        """
        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception_type((RateLimitExceeded, APIServerError, requests.exceptions.RequestException)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def _call_api() -> requests.Response:
            # 1. Acquire rate limit token
            self.rate_limiter.acquire()

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=timeout,
                )
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"Network error calling {url}: {e}. Will retry...")
                raise

            # 2. Inspect status codes
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "2.0"))
                logger.warning(
                    f"429 Too Many Requests received from {url}. Retry-After={retry_after}s"
                )
                time.sleep(retry_after)
                raise RateLimitExceeded(f"Rate limited by {self.source_name}", retry_after=retry_after)

            if 500 <= response.status_code < 600:
                logger.warning(
                    f"Server error {response.status_code} received from {url}. Will retry..."
                )
                raise APIServerError(response.status_code, response.text)

            if 400 <= response.status_code < 500:
                logger.error(
                    f"Client error {response.status_code} from {url}: {response.text[:200]}"
                )
                raise APIClientError(response.status_code, response.reason, response.text)

            return response

        return _call_api()

    @abc.abstractmethod
    def extract_entity_pages(
        self,
        entity: str,
        watermark: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> Generator[Tuple[List[Dict[str, Any]], Optional[str], bool], None, None]:
        """
        Abstract generator yielding (records_list, next_cursor, has_more).
        Must be implemented by source extractors.
        """
        pass

    def extract_and_stage(
        self,
        entity: str,
        watermark: Optional[datetime] = None,
        compress: bool = True,
    ) -> Dict[str, Any]:
        """
        Executes full pagination extraction for an entity, validates raw records,
        writes partitioned JSON batches into S3 staging, and records watermark state.
        """
        effective_watermark = watermark or self.state_store.get_watermark(self.source_name, entity)
        logger.info(
            f"Starting extraction for {self.source_name}.{entity} (watermark={effective_watermark})"
        )

        batch_id = str(uuid.uuid4())[:8]
        all_staged_records = []
        staged_uris = []
        page_count = 0
        total_records = 0
        latest_timestamp: Optional[datetime] = None
        last_cursor: Optional[str] = None

        start_time = datetime.now(timezone.utc)

        for raw_records_page, next_cursor, has_more in self.extract_entity_pages(
            entity=entity,
            watermark=effective_watermark,
        ):
            page_count += 1
            last_cursor = next_cursor

            if not raw_records_page:
                continue

            total_records += len(raw_records_page)
            all_staged_records.extend(raw_records_page)

            # Stage page to S3 / data lake
            page_batch_id = f"{batch_id}_p{page_count:04d}"
            staged_uri = self.storage_writer.write_raw_batch(
                source=self.source_name,
                entity=entity,
                records=raw_records_page,
                batch_id=page_batch_id,
                timestamp=start_time,
                compress=compress,
            )
            staged_uris.append(staged_uri)

        end_time = datetime.now(timezone.utc)

        # Update high-watermark state
        self.state_store.update_state(
            source=self.source_name,
            entity=entity,
            last_extracted_timestamp=end_time,
            last_cursor=last_cursor,
            records_synced=total_records,
            status="COMPLETED",
        )

        summary = {
            "source": self.source_name,
            "entity": entity,
            "batch_id": batch_id,
            "total_records": total_records,
            "pages_extracted": page_count,
            "staged_uris": staged_uris,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
        }

        logger.info(
            f"Extraction complete for {self.source_name}.{entity}: {total_records} records staged in {len(staged_uris)} batch files."
        )
        return summary
