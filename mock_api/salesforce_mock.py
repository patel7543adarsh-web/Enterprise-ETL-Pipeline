"""
Mock Salesforce API Simulator.
Simulates Salesforce REST / SOQL query endpoint with QueryLocator pagination.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus
import requests


class MockSalesforceAPI:
    """Simulates Salesforce REST API with SOQL QueryLocator cursor pagination."""

    def __init__(self, page_size: int = 10):
        self.page_size = page_size
        self.cursor_sessions: Dict[str, Dict[str, Any]] = {}
        self.data: Dict[str, List[Dict[str, Any]]] = {
            "accounts": self._generate_accounts(25),
            "contacts": self._generate_contacts(30),
            "opportunities": self._generate_opportunities(20),
            "orders": self._generate_orders(20),
        }

    def _generate_accounts(self, count: int) -> List[Dict[str, Any]]:
        records = []
        for i in range(1, count + 1):
            records.append({
                "Id": f"001{i:015d}",
                "Name": f"Global Corp {i}",
                "Type": "Customer - Direct" if i % 2 == 0 else "Customer - Channel",
                "BillingStreet": f"{500 + i} 5th Ave",
                "BillingCity": "New York" if i % 2 == 0 else "San Francisco",
                "BillingState": "NY" if i % 2 == 0 else "CA",
                "BillingPostalCode": f"1000{i % 10}",
                "BillingCountry": "USA",
                "Phone": f"+1-212-555-{i:04d}",
                "Website": f"https://globalcorp{i}.example.com",
                "AnnualRevenue": float(1000000 + (i * 250000)),
                "NumberOfEmployees": 50 + (i * 10),
                "Industry": "Technology" if i % 3 == 0 else "Financial Services",
                "CreatedDate": f"2024-01-{(i % 28) + 1:02d}T08:00:00.000+0000",
                "LastModifiedDate": f"2024-02-{(i % 28) + 1:02d}T12:00:00.000+0000",
                "SystemModstamp": f"2024-02-{(i % 28) + 1:02d}T12:00:00.000+0000",
            })
        return records

    def _generate_contacts(self, count: int) -> List[Dict[str, Any]]:
        records = []
        for i in range(1, count + 1):
            acc_id = f"001{((i % 25) + 1):015d}"
            records.append({
                "Id": f"003{i:015d}",
                "AccountId": acc_id,
                "FirstName": f"Jane" if i % 2 == 0 else "John",
                "LastName": f"Doe {i}",
                "Email": f"contact_{i:04d}@example.com",
                "Phone": f"+1-555-99{i:02d}",
                "Title": "VP Engineering" if i % 2 == 0 else "Director of IT",
                "Department": "Engineering" if i % 2 == 0 else "Operations",
                "CreatedDate": f"2024-01-{(i % 28) + 1:02d}T09:00:00.000+0000",
                "LastModifiedDate": f"2024-02-{(i % 28) + 1:02d}T14:00:00.000+0000",
            })
        return records

    def _generate_opportunities(self, count: int) -> List[Dict[str, Any]]:
        records = []
        for i in range(1, count + 1):
            acc_id = f"001{((i % 25) + 1):015d}"
            records.append({
                "Id": f"006{i:015d}",
                "AccountId": acc_id,
                "Name": f"Enterprise Software Deal #{i}",
                "Amount": float(50000 + (i * 10000)),
                "StageName": "Closed Won" if i % 3 == 0 else "Negotiation/Review",
                "Probability": 100.0 if i % 3 == 0 else 70.0,
                "CloseDate": f"2024-03-{(i % 28) + 1:02d}",
                "Type": "New Business" if i % 2 == 0 else "Existing Customer - Upgrade",
                "IsClosed": i % 3 == 0,
                "IsWon": i % 3 == 0,
                "CreatedDate": f"2024-01-{(i % 28) + 1:02d}T10:00:00.000+0000",
                "LastModifiedDate": f"2024-02-{(i % 28) + 1:02d}T15:00:00.000+0000",
            })
        return records

    def _generate_orders(self, count: int) -> List[Dict[str, Any]]:
        records = []
        for i in range(1, count + 1):
            acc_id = f"001{((i % 25) + 1):015d}"
            records.append({
                "Id": f"801{i:015d}",
                "AccountId": acc_id,
                "OrderNumber": f"ORD-{1000 + i}",
                "Status": "Activated" if i % 4 != 0 else "Draft",
                "TotalAmount": float(25000 + (i * 5000)),
                "EffectiveDate": f"2024-02-{(i % 28) + 1:02d}",
                "CreatedDate": f"2024-02-{(i % 28) + 1:02d}T11:00:00.000+0000",
                "LastModifiedDate": f"2024-02-{(i % 28) + 1:02d}T16:00:00.000+0000",
            })
        return records

    def handle_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Handles mock Salesforce SOQL query and cursor pagination."""
        params = params or {}
        mock_resp = requests.Response()
        mock_resp.status_code = 200
        mock_resp.headers["Content-Type"] = "application/json"
        mock_resp.headers["Sforce-Limit-Info"] = "api-usage=42/15000"

        # Check if this is a queryLocator cursor request
        if "/query/01g" in url:
            cursor_id = url.split("/query/")[-1]
            session = self.cursor_sessions.get(cursor_id)
            if not session:
                mock_resp.status_code = 404
                mock_resp._content = b'[{"errorCode": "INVALID_QUERY_LOCATOR", "message": "Invalid locator"}]'
                return mock_resp

            records = session["records"]
            offset = session["offset"]
            page_size = self.page_size
            page_records = records[offset : offset + page_size]
            new_offset = offset + len(page_records)
            done = new_offset >= len(records)

            next_url = None
            if not done:
                next_cursor_id = f"01g_{cursor_id.split('_')[1]}_{new_offset}"
                self.cursor_sessions[next_cursor_id] = {
                    "records": records,
                    "offset": new_offset,
                }
                next_url = f"/services/data/v59.0/query/{next_cursor_id}"

            payload = {
                "totalSize": len(records),
                "done": done,
                "nextRecordsUrl": next_url,
                "records": page_records,
            }
            mock_resp._content = requests.compat.json.dumps(payload).encode("utf-8")
            return mock_resp

        # Initial SOQL query request
        query = params.get("q", "") or url.split("?q=")[-1]
        query = unquote_plus(query)

        # Detect entity from SOQL FROM clause
        entity = "accounts"
        if "FROM Account" in query:
            entity = "accounts"
        elif "FROM Contact" in query:
            entity = "contacts"
        elif "FROM Opportunity" in query:
            entity = "opportunities"
        elif "FROM Order" in query:
            entity = "orders"

        all_records = list(self.data.get(entity, []))

        # Check watermark filter if present
        if "WHERE LastModifiedDate >=" in query:
            try:
                # e.g. WHERE LastModifiedDate >= 2024-02-01T00:00:00Z
                wm_str = query.split("WHERE LastModifiedDate >=")[1].strip().split()[0]
                wm_dt = datetime.fromisoformat(wm_str.replace("Z", "+00:00"))
                all_records = [
                    r for r in all_records
                    if datetime.fromisoformat(r["LastModifiedDate"].replace("+0000", "+00:00")) >= wm_dt
                ]
            except Exception:
                pass

        page_records = all_records[: self.page_size]
        done = len(all_records) <= self.page_size
        next_url = None

        if not done:
            cursor_id = f"01g_{entity}_{self.page_size}"
            self.cursor_sessions[cursor_id] = {
                "records": all_records,
                "offset": self.page_size,
            }
            next_url = f"/services/data/v59.0/query/{cursor_id}"

        payload = {
            "totalSize": len(all_records),
            "done": done,
            "nextRecordsUrl": next_url,
            "records": page_records,
        }
        mock_resp._content = requests.compat.json.dumps(payload).encode("utf-8")
        return mock_resp
