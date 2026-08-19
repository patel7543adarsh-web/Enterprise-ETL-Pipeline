"""
Stripe Raw API Data Models.
Defines Pydantic v2 schemas mirroring the Stripe REST API payloads.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, ConfigDict, Field, field_validator


class StripeAddress(BaseModel):
    """Stripe address object schema."""
    model_config = ConfigDict(extra="ignore")

    city: Optional[str] = None
    country: Optional[str] = None
    line1: Optional[str] = None
    line2: Optional[str] = None
    postal_code: Optional[str] = None
    state: Optional[str] = None


class StripeCustomer(BaseModel):
    """Stripe Customer object schema."""
    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Unique identifier for the customer, e.g. cus_123")
    object: str = Field(default="customer")
    email: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    description: Optional[str] = None
    currency: Optional[str] = None
    balance: Optional[int] = Field(default=0, description="Customer balance in integer cents")
    delinquent: Optional[bool] = False
    created: datetime = Field(description="Time at which the object was created (UTC)")
    address: Optional[StripeAddress] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("created", mode="before")
    @classmethod
    def parse_unix_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            try:
                ts = float(v)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except ValueError:
                pass
        return v


class StripeCharge(BaseModel):
    """Stripe Charge object schema."""
    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Unique identifier for the charge, e.g. ch_123")
    object: str = Field(default="charge")
    amount: int = Field(description="Amount charged in cents")
    amount_refunded: Optional[int] = Field(default=0, description="Amount refunded in cents")
    currency: str = Field(description="Three-letter ISO currency code in lowercase")
    customer: Optional[str] = Field(default=None, description="ID of the customer charged")
    description: Optional[str] = None
    paid: bool = Field(default=True)
    status: str = Field(description="Status of the charge: succeeded, pending, or failed")
    refunded: bool = Field(default=False)
    payment_method: Optional[str] = None
    created: datetime = Field(description="Timestamp charge was created (UTC)")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("created", mode="before")
    @classmethod
    def parse_unix_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            try:
                ts = float(v)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except ValueError:
                pass
        return v


class StripeInvoice(BaseModel):
    """Stripe Invoice object schema."""
    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Unique identifier for the invoice, e.g. in_123")
    object: str = Field(default="invoice")
    customer: str = Field(description="ID of the customer this invoice belongs to")
    subscription: Optional[str] = Field(default=None, description="ID of the associated subscription")
    amount_due: int = Field(description="Final amount due at this time in cents")
    amount_paid: int = Field(default=0, description="Amount paid in cents")
    amount_remaining: int = Field(default=0, description="Remaining amount due in cents")
    subtotal: int = Field(default=0, description="Total of all items before discount/tax")
    total: int = Field(default=0, description="Total amount after discounts and taxes")
    currency: str = Field(description="Three-letter ISO currency code")
    status: Optional[str] = Field(default="draft", description="draft, open, paid, uncollectible, or void")
    paid: bool = Field(default=False)
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    created: datetime = Field(description="Timestamp invoice was created (UTC)")

    @field_validator("created", "period_start", "period_end", mode="before")
    @classmethod
    def parse_unix_timestamp(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            try:
                ts = float(v)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except ValueError:
                pass
        return v


class StripeSubscription(BaseModel):
    """Stripe Subscription object schema."""
    model_config = ConfigDict(extra="ignore")

    id: str = Field(description="Unique identifier for the subscription, e.g. sub_123")
    object: str = Field(default="subscription")
    customer: str = Field(description="ID of the customer who owns the subscription")
    status: str = Field(description="active, past_due, canceled, incomplete, trialing, etc.")
    current_period_start: datetime = Field(description="Start date of current period (UTC)")
    current_period_end: datetime = Field(description="End date of current period (UTC)")
    cancel_at_period_end: bool = Field(default=False)
    created: datetime = Field(description="Timestamp subscription was created (UTC)")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("created", "current_period_start", "current_period_end", mode="before")
    @classmethod
    def parse_unix_timestamp(cls, v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v, tz=timezone.utc)
        if isinstance(v, str):
            try:
                ts = float(v)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except ValueError:
                pass
        return v


T = TypeVar("T", bound=BaseModel)


class StripeListResponse(BaseModel, Generic[T]):
    """Generic Stripe paginated list response container."""
    model_config = ConfigDict(extra="ignore")

    object: str = Field(default="list")
    data: List[T] = Field(default_factory=list)
    has_more: bool = Field(default=False)
    url: Optional[str] = None
