"""Models package for Enterprise ETL Pipeline."""

from models.raw.stripe_models import (
    StripeAddress,
    StripeCharge,
    StripeCustomer,
    StripeInvoice,
    StripeListResponse,
    StripeSubscription,
)
from models.raw.salesforce_models import (
    SalesforceAccount,
    SalesforceContact,
    SalesforceOpportunity,
    SalesforceOrder,
    SalesforceQueryResult,
)
from models.unified.canonical_models import (
    UnifiedCustomer,
    UnifiedTransaction,
    UnifiedSubscription,
    UnifiedCompanyAccount,
    DataQualityReport,
    IngestionMetadata,
)

__all__ = [
    "StripeAddress",
    "StripeCustomer",
    "StripeCharge",
    "StripeInvoice",
    "StripeSubscription",
    "StripeListResponse",
    "SalesforceAccount",
    "SalesforceContact",
    "SalesforceOpportunity",
    "SalesforceOrder",
    "SalesforceQueryResult",
    "UnifiedCustomer",
    "UnifiedTransaction",
    "UnifiedSubscription",
    "UnifiedCompanyAccount",
    "DataQualityReport",
    "IngestionMetadata",
]
