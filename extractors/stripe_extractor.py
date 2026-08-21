"""
Stripe API Extractor with Cursor-Based Pagination and Incremental Extraction.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple, Type
from pydantic import BaseModel

from config.settings import StripeSettings, get_settings
from extractors.base import BaseExtractor
from models.raw.stripe_models import (
    StripeCharge,
    StripeCustomer,
    StripeInvoice,
    StripeSubscription,
)
from storage.s3_client import S3DataLakeWriter
from storage.state_store import IncrementalStateStore

logger = logging.getLogger(__name__)

# Map entity name to corresponding Pydantic validation model
STRIPE_ENTITY_MODEL_MAP: Dict[str, Type[BaseModel]] = {
    "customers": StripeCustomer,
    "charges": StripeCharge,
    "invoices": StripeInvoice,
    "subscriptions": StripeSubscription,
}


class StripeExtractor(BaseExtractor):
    """
    Extracts data from Stripe REST API endpoints using cursor-based pagination
    (starting_after parameter) and incremental sync by created timestamp.
    """

    def __init__(
        self,
        settings: Optional[StripeSettings] = None,
        storage_writer: Optional[S3DataLakeWriter] = None,
        state_store: Optional[IncrementalStateStore] = None,
    ):
        self.stripe_settings = settings or get_settings().stripe
        super().__init__(
            source_name="stripe",
            rate_limit_rps=self.stripe_settings.rate_limit_rps,
            storage_writer=storage_writer,
            state_store=state_store,
        )
        self.base_url = self.stripe_settings.base_url.rstrip("/")
        self.api_key = self.stripe_settings.api_key.get_secret_value()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def extract_entity_pages(
        self,
        entity: str,
        watermark: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Generator[Tuple[List[Dict[str, Any]], Optional[str], bool], None, None]:
        """
        Paginates through Stripe list endpoint using cursor pagination ('starting_after').
        Yields (validated_raw_dicts, next_cursor, has_more).
        """
        entity_key = entity.lower()
        if entity_key not in STRIPE_ENTITY_MODEL_MAP:
            raise ValueError(
                f"Unsupported Stripe entity: '{entity}'. Supported: {list(STRIPE_ENTITY_MODEL_MAP.keys())}"
            )

        model_cls = STRIPE_ENTITY_MODEL_MAP[entity_key]
        endpoint_url = f"{self.base_url}/{entity_key}"
        page_limit = limit or self.stripe_settings.page_limit

        current_cursor = cursor
        has_more = True
        page_num = 0

        while has_more:
            page_num += 1
            params: Dict[str, Any] = {
                "limit": page_limit,
            }

            if current_cursor:
                params["starting_after"] = current_cursor

            if watermark:
                # Stripe created[gte] filter expects unix timestamp
                unix_ts = int(watermark.timestamp())
                params["created[gte]"] = unix_ts

            logger.info(
                f"Fetching Stripe {entity_key} page {page_num} (cursor={current_cursor}, limit={page_limit})"
            )

            response = self.execute_request_with_retry(
                method="GET",
                url=endpoint_url,
                headers=self._get_headers(),
                params=params,
            )

            payload = response.json()
            raw_data = payload.get("data", [])
            has_more = payload.get("has_more", False)

            if not raw_data:
                logger.info(f"No more records returned for Stripe {entity_key}.")
                yield [], None, False
                break

            # Validate each record through Pydantic model for schema integrity
            validated_records: List[Dict[str, Any]] = []
            for item in raw_data:
                try:
                    parsed_model = model_cls.model_validate(item)
                    validated_records.append(parsed_model.model_dump(mode="json"))
                except Exception as e:
                    logger.warning(
                        f"Schema validation error in Stripe {entity_key} record ID={item.get('id')}: {e}. Including raw."
                    )
                    validated_records.append(item)

            # Stripe cursor for next page is the ID of the last item in current page
            next_cursor = raw_data[-1]["id"] if raw_data else None
            current_cursor = next_cursor

            yield validated_records, next_cursor, has_more

            if not has_more:
                logger.info(f"Completed all pages for Stripe {entity_key}.")
                break
