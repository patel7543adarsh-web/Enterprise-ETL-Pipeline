"""
Salesforce REST API / SOQL Extractor with QueryLocator Pagination.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple, Type
from urllib.parse import quote_plus
from pydantic import BaseModel

from config.settings import SalesforceSettings, get_settings
from extractors.base import BaseExtractor
from models.raw.salesforce_models import (
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    SalesforceOrder,
)
from storage.s3_client import S3DataLakeWriter
from storage.state_store import IncrementalStateStore

logger = logging.getLogger(__name__)

SALESFORCE_ENTITY_CONFIG: Dict[str, Dict[str, Any]] = {
    "accounts": {
        "model": SalesforceAccount,
        "sobject": "Account",
        "fields": [
            "Id", "Name", "Type", "BillingStreet", "BillingCity",
            "BillingState", "BillingPostalCode", "BillingCountry",
            "Phone", "Website", "AnnualRevenue", "NumberOfEmployees",
            "Industry", "CreatedDate", "LastModifiedDate", "SystemModstamp"
        ],
        "watermark_field": "LastModifiedDate",
    },
    "contacts": {
        "model": SalesforceContact,
        "sobject": "Contact",
        "fields": [
            "Id", "AccountId", "FirstName", "LastName", "Email",
            "Phone", "Title", "Department", "CreatedDate", "LastModifiedDate"
        ],
        "watermark_field": "LastModifiedDate",
    },
    "opportunities": {
        "model": SalesforceOpportunity,
        "sobject": "Opportunity",
        "fields": [
            "Id", "AccountId", "Name", "Amount", "StageName",
            "Probability", "CloseDate", "Type", "IsClosed",
            "IsWon", "CreatedDate", "LastModifiedDate"
        ],
        "watermark_field": "LastModifiedDate",
    },
    "orders": {
        "model": SalesforceOrder,
        "sobject": "Order",
        "fields": [
            "Id", "AccountId", "OrderNumber", "Status", "TotalAmount",
            "EffectiveDate", "CreatedDate", "LastModifiedDate"
        ],
        "watermark_field": "LastModifiedDate",
    },
}


class SalesforceExtractor(BaseExtractor):
    """
    Extracts Salesforce SObjects via REST API SOQL queries using
    QueryLocator pagination ('nextRecordsUrl') and incremental filtering.
    """

    def __init__(
        self,
        settings: Optional[SalesforceSettings] = None,
        storage_writer: Optional[S3DataLakeWriter] = None,
        state_store: Optional[IncrementalStateStore] = None,
    ):
        self.sf_settings = settings or get_settings().salesforce
        super().__init__(
            source_name="salesforce",
            rate_limit_rps=self.sf_settings.rate_limit_rps,
            storage_writer=storage_writer,
            state_store=state_store,
        )
        self.instance_url = self.sf_settings.instance_url.rstrip("/")
        self.api_version = self.sf_settings.api_version
        self.access_token = self.sf_settings.access_token.get_secret_value()

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Sforce-Auto-Assign": "FALSE",
        }

    def build_soql_query(
        self,
        entity: str,
        watermark: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> str:
        """Constructs SOQL query with fields and optional incremental date filter."""
        config = SALESFORCE_ENTITY_CONFIG[entity.lower()]
        fields_str = ", ".join(config["fields"])
        sobject = config["sobject"]
        query = f"SELECT {fields_str} FROM {sobject}"

        if watermark:
            wm_iso = watermark.strftime("%Y-%m-%dT%H:%M:%SZ")
            wm_field = config["watermark_field"]
            query += f" WHERE {wm_field} >= {wm_iso}"

        query += " ORDER BY LastModifiedDate ASC"

        if limit:
            query += f" LIMIT {limit}"

        return query

    def extract_entity_pages(
        self,
        entity: str,
        watermark: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Generator[Tuple[List[Dict[str, Any]], Optional[str], bool], None, None]:
        """
        Executes SOQL query and paginates through results using nextRecordsUrl cursor.
        Yields (validated_raw_dicts, next_records_url, has_more).
        """
        entity_key = entity.lower()
        if entity_key not in SALESFORCE_ENTITY_CONFIG:
            raise ValueError(
                f"Unsupported Salesforce entity: '{entity}'. Supported: {list(SALESFORCE_ENTITY_CONFIG.keys())}"
            )

        config = SALESFORCE_ENTITY_CONFIG[entity_key]
        model_cls = config["model"]

        # If starting fresh, issue initial SOQL query
        if not cursor:
            soql = self.build_soql_query(entity=entity_key, watermark=watermark, limit=limit)
            url = f"{self.instance_url}/services/data/{self.api_version}/query/?q={quote_plus(soql)}"
        else:
            # Continue from nextRecordsUrl cursor
            url = f"{self.instance_url}{cursor}" if cursor.startswith("/") else f"{self.instance_url}/{cursor}"

        page_num = 0
        done = False

        while not done:
            page_num += 1
            logger.info(f"Executing Salesforce {entity_key} query page {page_num} from: {url}")

            response = self.execute_request_with_retry(
                method="GET",
                url=url,
                headers=self._get_headers(),
            )

            # Monitor Salesforce API limit headers if provided
            limit_info = response.headers.get("Sforce-Limit-Info")
            if limit_info:
                logger.debug(f"Salesforce Sforce-Limit-Info: {limit_info}")

            payload = response.json()
            total_size = payload.get("totalSize", 0)
            done = payload.get("done", True)
            next_records_url = payload.get("nextRecordsUrl")
            raw_records = payload.get("records", [])

            logger.info(
                f"Salesforce {entity_key} page {page_num}: {len(raw_records)} records (totalSize={total_size}, done={done})"
            )

            if not raw_records:
                yield [], None, False
                break

            # Validate each record through Pydantic model
            validated_records: List[Dict[str, Any]] = []
            for item in raw_records:
                # Strip Salesforce internal attributes object if present
                clean_item = {k: v for k, v in item.items() if k != "attributes"}
                try:
                    parsed = model_cls.model_validate(clean_item)
                    validated_records.append(parsed.model_dump(by_alias=True, mode="json"))
                except Exception as e:
                    logger.warning(
                        f"Schema validation error in Salesforce {entity_key} record ID={item.get('Id')}: {e}. Including raw."
                    )
                    validated_records.append(clean_item)

            has_more = not done and bool(next_records_url)
            yield validated_records, next_records_url, has_more

            if done or not next_records_url:
                break

            url = f"{self.instance_url}{next_records_url}" if next_records_url.startswith("/") else f"{self.instance_url}/{next_records_url}"
