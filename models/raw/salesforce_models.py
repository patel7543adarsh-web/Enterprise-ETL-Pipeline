"""
Salesforce Raw API Data Models.
Defines Pydantic v2 schemas mirroring the Salesforce REST / SOQL query responses.
"""

from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, ConfigDict, Field, field_validator
from dateutil import parser as date_parser


def _parse_iso_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        return date_parser.parse(v)
    return None


class SalesforceAccount(BaseModel):
    """Salesforce Account SObject schema."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(alias="Id", description="18-character Salesforce Record ID")
    name: str = Field(alias="Name", description="Organization / Account Name")
    type: Optional[str] = Field(default=None, alias="Type", description="Customer, Partner, Prospect, etc.")
    billing_street: Optional[str] = Field(default=None, alias="BillingStreet")
    billing_city: Optional[str] = Field(default=None, alias="BillingCity")
    billing_state: Optional[str] = Field(default=None, alias="BillingState")
    billing_postal_code: Optional[str] = Field(default=None, alias="BillingPostalCode")
    billing_country: Optional[str] = Field(default=None, alias="BillingCountry")
    phone: Optional[str] = Field(default=None, alias="Phone")
    website: Optional[str] = Field(default=None, alias="Website")
    annual_revenue: Optional[float] = Field(default=None, alias="AnnualRevenue")
    number_of_employees: Optional[int] = Field(default=None, alias="NumberOfEmployees")
    industry: Optional[str] = Field(default=None, alias="Industry")
    created_date: datetime = Field(alias="CreatedDate")
    last_modified_date: datetime = Field(alias="LastModifiedDate")
    system_modstamp: Optional[datetime] = Field(default=None, alias="SystemModstamp")

    @field_validator("created_date", "last_modified_date", "system_modstamp", mode="before")
    @classmethod
    def validate_dates(cls, v: Any) -> Optional[datetime]:
        return _parse_iso_datetime(v)


class SalesforceContact(BaseModel):
    """Salesforce Contact SObject schema."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(alias="Id")
    account_id: Optional[str] = Field(default=None, alias="AccountId")
    first_name: Optional[str] = Field(default=None, alias="FirstName")
    last_name: str = Field(alias="LastName")
    email: Optional[str] = Field(default=None, alias="Email")
    phone: Optional[str] = Field(default=None, alias="Phone")
    title: Optional[str] = Field(default=None, alias="Title")
    department: Optional[str] = Field(default=None, alias="Department")
    created_date: datetime = Field(alias="CreatedDate")
    last_modified_date: datetime = Field(alias="LastModifiedDate")

    @field_validator("created_date", "last_modified_date", mode="before")
    @classmethod
    def validate_dates(cls, v: Any) -> Optional[datetime]:
        return _parse_iso_datetime(v)


class SalesforceOpportunity(BaseModel):
    """Salesforce Opportunity SObject schema."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(alias="Id")
    account_id: Optional[str] = Field(default=None, alias="AccountId")
    name: str = Field(alias="Name")
    amount: Optional[float] = Field(default=0.0, alias="Amount")
    stage_name: str = Field(alias="StageName")
    probability: Optional[float] = Field(default=0.0, alias="Probability")
    close_date: Optional[str] = Field(default=None, alias="CloseDate")
    type: Optional[str] = Field(default=None, alias="Type")
    is_closed: bool = Field(default=False, alias="IsClosed")
    is_won: bool = Field(default=False, alias="IsWon")
    created_date: datetime = Field(alias="CreatedDate")
    last_modified_date: datetime = Field(alias="LastModifiedDate")

    @field_validator("created_date", "last_modified_date", mode="before")
    @classmethod
    def validate_dates(cls, v: Any) -> Optional[datetime]:
        return _parse_iso_datetime(v)


class SalesforceOrder(BaseModel):
    """Salesforce Order SObject schema."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    id: str = Field(alias="Id")
    account_id: Optional[str] = Field(default=None, alias="AccountId")
    order_number: str = Field(alias="OrderNumber")
    status: str = Field(alias="Status")
    total_amount: Optional[float] = Field(default=0.0, alias="TotalAmount")
    effective_date: Optional[str] = Field(default=None, alias="EffectiveDate")
    created_date: datetime = Field(alias="CreatedDate")
    last_modified_date: datetime = Field(alias="LastModifiedDate")

    @field_validator("created_date", "last_modified_date", mode="before")
    @classmethod
    def validate_dates(cls, v: Any) -> Optional[datetime]:
        return _parse_iso_datetime(v)


T = TypeVar("T", bound=BaseModel)


class SalesforceQueryResult(BaseModel, Generic[T]):
    """Salesforce SOQL Query response wrapper."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    total_size: int = Field(alias="totalSize")
    done: bool = Field(default=True, alias="done")
    next_records_url: Optional[str] = Field(default=None, alias="nextRecordsUrl")
    records: List[T] = Field(default_factory=list, alias="records")
