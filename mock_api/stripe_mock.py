"""
Mock Stripe API Simulator.
Simulates Stripe REST API paginated endpoints, rate-limiting, and error responses.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests


class MockStripeAPI:
    """Simulates Stripe REST API with cursor-based pagination and rate limiting."""

    def __init__(self, simulate_rate_limit: bool = False, rate_limit_every_n: int = 4):
        self.simulate_rate_limit = simulate_rate_limit
        self.rate_limit_every_n = rate_limit_every_n
        self.request_count = 0
        self.data: Dict[str, List[Dict[str, Any]]] = {
            "customers": self._generate_customers(25),
            "charges": self._generate_charges(40),
            "invoices": self._generate_invoices(30),
            "subscriptions": self._generate_subscriptions(20),
        }

    def _generate_customers(self, count: int) -> List[Dict[str, Any]]:
        base_time = 1704067200  # 2024-01-01 00:00:00 UTC
        records = []
        for i in range(1, count + 1):
            records.append({
                "id": f"cus_{i:04d}",
                "object": "customer",
                "email": f"customer_{i:04d}@example.com",
                "name": f"Enterprise Client {i}",
                "phone": f"+1-555-01{i:02d}",
                "description": f"Standard Tier Customer {i}",
                "currency": "usd",
                "balance": (i * 100) if i % 3 == 0 else 0,
                "delinquent": False,
                "created": base_time + (i * 3600 * 24),
                "address": {
                    "city": "San Francisco" if i % 2 == 0 else "New York",
                    "country": "US",
                    "line1": f"{100 + i} Market St",
                    "line2": f"Suite {i}",
                    "postal_code": f"9410{i % 10}",
                    "state": "CA" if i % 2 == 0 else "NY",
                },
                "metadata": {"tier": "enterprise" if i > 15 else "standard"},
            })
        return records

    def _generate_charges(self, count: int) -> List[Dict[str, Any]]:
        base_time = 1704067200
        records = []
        for i in range(1, count + 1):
            cus_id = f"cus_{(i % 25) + 1:04d}"
            amount = (5000 + (i * 250))  # in cents
            records.append({
                "id": f"ch_{i:04d}",
                "object": "charge",
                "amount": amount,
                "amount_refunded": 500 if i % 7 == 0 else 0,
                "currency": "usd" if i % 4 != 0 else "eur",
                "customer": cus_id,
                "description": f"Monthly SaaS Subscription Charge #{i}",
                "paid": True,
                "status": "succeeded" if i % 10 != 0 else "failed",
                "refunded": i % 7 == 0,
                "payment_method": f"pm_{i:04d}",
                "created": base_time + (i * 3600 * 12),
                "metadata": {"invoice_id": f"in_{(i % 30) + 1:04d}"},
            })
        return records

    def _generate_invoices(self, count: int) -> List[Dict[str, Any]]:
        base_time = 1704067200
        records = []
        for i in range(1, count + 1):
            cus_id = f"cus_{(i % 25) + 1:04d}"
            sub_id = f"sub_{(i % 20) + 1:04d}"
            amount = (12000 + (i * 500))
            records.append({
                "id": f"in_{i:04d}",
                "object": "invoice",
                "customer": cus_id,
                "subscription": sub_id,
                "amount_due": amount,
                "amount_paid": amount if i % 5 != 0 else 0,
                "amount_remaining": 0 if i % 5 != 0 else amount,
                "subtotal": amount,
                "total": amount,
                "currency": "usd",
                "status": "paid" if i % 5 != 0 else "open",
                "paid": i % 5 != 0,
                "period_start": base_time + ((i - 1) * 3600 * 24 * 30),
                "period_end": base_time + (i * 3600 * 24 * 30),
                "created": base_time + (i * 3600 * 24 * 30),
            })
        return records

    def _generate_subscriptions(self, count: int) -> List[Dict[str, Any]]:
        base_time = 1704067200
        records = []
        for i in range(1, count + 1):
            cus_id = f"cus_{i:04d}"
            records.append({
                "id": f"sub_{i:04d}",
                "object": "subscription",
                "customer": cus_id,
                "status": "active" if i % 6 != 0 else "canceled",
                "current_period_start": base_time + ((i - 1) * 3600 * 24 * 30),
                "current_period_end": base_time + (i * 3600 * 24 * 30),
                "cancel_at_period_end": i % 4 == 0,
                "created": base_time + (i * 3600 * 24),
                "metadata": {"plan_code": f"PRO_{i}"},
            })
        return records

    def handle_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Handles mock Stripe request with pagination logic."""
        self.request_count += 1
        params = params or {}

        # Simulate 429 Too Many Requests if enabled
        if self.simulate_rate_limit and (self.request_count % self.rate_limit_every_n == 0):
            mock_resp = requests.Response()
            mock_resp.status_code = 429
            mock_resp._content = b'{"error": {"type": "rate_limit_error", "message": "Too many requests"}}'
            mock_resp.headers["Retry-After"] = "0.05"
            return mock_resp

        entity = endpoint.strip("/").split("/")[-1].lower()
        all_records = self.data.get(entity, [])

        # Filter by created[gte] if provided
        created_gte = params.get("created[gte]")
        if created_gte is not None:
            created_gte = int(created_gte)
            all_records = [r for r in all_records if r.get("created", 0) >= created_gte]

        limit = int(params.get("limit", 10))
        starting_after = params.get("starting_after")

        start_idx = 0
        if starting_after:
            for idx, item in enumerate(all_records):
                if item.get("id") == starting_after:
                    start_idx = idx + 1
                    break

        page_records = all_records[start_idx : start_idx + limit]
        has_more = (start_idx + limit) < len(all_records)

        payload = {
            "object": "list",
            "data": page_records,
            "has_more": has_more,
            "url": f"/v1/{entity}",
        }

        mock_resp = requests.Response()
        mock_resp.status_code = 200
        mock_resp._content = requests.compat.json.dumps(payload).encode("utf-8")
        mock_resp.headers["Content-Type"] = "application/json"
        return mock_resp
