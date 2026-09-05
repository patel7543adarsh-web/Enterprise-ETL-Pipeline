"""
Data Warehouse Layer Package.
Provides SQLAlchemy connection management, canonical schemas, and dialect-aware upsert loaders.
"""

from warehouse.connection import (
    WarehouseConnectionManager,
    get_warehouse_engine,
)
from warehouse.loader import (
    MODEL_TABLE_MAP,
    WarehouseUpsertLoader,
)
from warehouse.schema import (
    Base,
    DimCompany,
    DimCustomer,
    ETLLoadAudit,
    FactSubscription,
    FactTransaction,
    drop_warehouse_schema,
    init_warehouse_schema,
)

__all__ = [
    "WarehouseConnectionManager",
    "get_warehouse_engine",
    "WarehouseUpsertLoader",
    "MODEL_TABLE_MAP",
    "Base",
    "DimCustomer",
    "FactTransaction",
    "FactSubscription",
    "DimCompany",
    "ETLLoadAudit",
    "init_warehouse_schema",
    "drop_warehouse_schema",
]
