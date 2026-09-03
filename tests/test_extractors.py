"""
Tests for Week 1 Day 3-5: Extraction Scripts & Cursor-Based Pagination.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest
import requests

from extractors.salesforce_extractor import SalesforceExtractor
from extractors.stripe_extractor import StripeExtractor
from mock_api.salesforce_mock import MockSalesforceAPI
from mock_api.stripe_mock import MockStripeAPI


class TestStripeExtractor:
    """Tests Stripe cursor-based pagination and extraction logic."""

    def test_stripe_cursor_pagination_multi_page(self, mock_settings, mock_storage_writer, mock_state_store):
        # Setup Stripe extractor with mock API
        extractor = StripeExtractor(
            settings=mock_settings.stripe,
            storage_writer=mock_storage_writer,
            state_store=mock_state_store,
        )
        mock_api = MockStripeAPI(simulate_rate_limit=False)

        def mock_call(method, url, headers=None, params=None, **kwargs):
            return mock_api.handle_request(endpoint=url, params=params, headers=headers)

        extractor.execute_request_with_retry = mock_call

        # Extract customers with small page limit (10)
        pages = list(extractor.extract_entity_pages(entity="customers", limit=10))

        # 25 total customers with limit=10 should yield 3 pages (10, 10, 5)
        assert len(pages) == 3

        page1_records, cursor1, has_more1 = pages[0]
        assert len(page1_records) == 10
        assert cursor1 == "cus_0010"
        assert has_more1 is True

        page2_records, cursor2, has_more2 = pages[1]
        assert len(page2_records) == 10
        assert cursor2 == "cus_0020"
        assert has_more2 is True

        page3_records, cursor3, has_more3 = pages[2]
        assert len(page3_records) == 5
        assert cursor3 == "cus_0025"
        assert has_more3 is False

    def test_stripe_extract_and_stage(self, mock_settings, mock_storage_writer, mock_state_store):
        extractor = StripeExtractor(
            settings=mock_settings.stripe,
            storage_writer=mock_storage_writer,
            state_store=mock_state_store,
        )
        mock_api = MockStripeAPI(simulate_rate_limit=False)
        extractor.execute_request_with_retry = lambda method, url, headers=None, params=None, **kw: mock_api.handle_request(
            endpoint=url, params=params, headers=headers
        )

        result = extractor.extract_and_stage(entity="charges")
        assert result["total_records"] == 40
        assert len(result["staged_uris"]) > 0

        # Verify state store was updated with high watermark
        watermark = mock_state_store.get_watermark("stripe", "charges")
        assert watermark is not None


class TestSalesforceExtractor:
    """Tests Salesforce SOQL QueryLocator cursor pagination."""

    def test_soql_query_generation(self, mock_settings, mock_storage_writer, mock_state_store):
        extractor = SalesforceExtractor(
            settings=mock_settings.salesforce,
            storage_writer=mock_storage_writer,
            state_store=mock_state_store,
        )

        # 1. Base query
        query_base = extractor.build_soql_query(entity="accounts")
        assert "SELECT" in query_base
        assert "FROM Account" in query_base
        assert "ORDER BY LastModifiedDate ASC" in query_base

        # 2. Incremental query with watermark
        wm = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        query_wm = extractor.build_soql_query(entity="accounts", watermark=wm)
        assert "WHERE LastModifiedDate >= 2024-01-01T12:00:00Z" in query_wm

    def test_salesforce_querylocator_pagination(self, mock_settings, mock_storage_writer, mock_state_store):
        extractor = SalesforceExtractor(
            settings=mock_settings.salesforce,
            storage_writer=mock_storage_writer,
            state_store=mock_state_store,
        )
        mock_api = MockSalesforceAPI(page_size=10)

        extractor.execute_request_with_retry = lambda method, url, headers=None, params=None, **kw: mock_api.handle_request(
            url=url, params=params, headers=headers
        )

        pages = list(extractor.extract_entity_pages(entity="accounts", limit=10))

        # 25 total accounts with page_size=10 should yield 3 pages (10, 10, 5)
        assert len(pages) == 3
        assert len(pages[0][0]) == 10
        assert pages[0][2] is True  # has_more

        assert len(pages[1][0]) == 10
        assert pages[1][2] is True

        assert len(pages[2][0]) == 5
        assert pages[2][2] is False  # done
