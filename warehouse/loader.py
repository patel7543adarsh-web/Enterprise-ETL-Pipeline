"""
Enterprise Data Warehouse High-Performance Upsert (INSERT/UPDATE) Loader.
Supports dialect-aware incremental upserts for PostgreSQL, SQLite, and Snowflake.
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Type, Union
import pandas as pd
import polars as pl
from pydantic import BaseModel
from dateutil import parser as date_parser
from sqlalchemy import Date, DateTime, Engine, Table, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import declarative_base

from warehouse.connection import WarehouseConnectionManager, get_warehouse_engine
from warehouse.schema import (
    Base,
    DimCompany,
    DimCustomer,
    ETLLoadAudit,
    FactSubscription,
    FactTransaction,
    init_warehouse_schema,
)

logger = logging.getLogger("warehouse.loader")

# Mapping canonical names to ORM classes
MODEL_TABLE_MAP: Dict[str, Type[Any]] = {
    "dim_customers": DimCustomer,
    "customers": DimCustomer,
    "fact_transactions": FactTransaction,
    "transactions": FactTransaction,
    "fact_subscriptions": FactSubscription,
    "subscriptions": FactSubscription,
    "dim_companies": DimCompany,
    "companies": DimCompany,
}


class WarehouseUpsertLoader:
    """
    Executes idempotent upsert operations into target data warehouse tables.
    Eliminates duplicates and ensures updated source records overwrite existing entries cleanly.
    """

    def __init__(
        self,
        engine: Optional[Engine] = None,
        connection_manager: Optional[WarehouseConnectionManager] = None,
        chunk_size: int = 500,
    ):
        if connection_manager:
            self.conn_mgr = connection_manager
            self.engine = connection_manager.engine
        elif engine:
            self.engine = engine
            self.conn_mgr = WarehouseConnectionManager()
            self.conn_mgr._engine = engine
        else:
            self.conn_mgr = WarehouseConnectionManager()
            self.engine = self.conn_mgr.engine

        self.chunk_size = chunk_size
        self.dialect = self.engine.dialect.name.lower()
        # Ensure target schema exists
        init_warehouse_schema(self.engine)

    def _normalize_records(
        self,
        data: Union[pl.DataFrame, pd.DataFrame, List[BaseModel], List[Dict[str, Any]]],
        orm_class: Type[Any],
    ) -> List[Dict[str, Any]]:
        """Converts diverse input data types (Polars, Pandas, Pydantic, dicts) to clean dict records."""
        if isinstance(data, pl.DataFrame):
            # Convert Polars DataFrame to list of dicts
            raw_dicts = data.to_dicts()
        elif isinstance(data, pd.DataFrame):
            raw_dicts = data.to_dict(orient="records")
        elif isinstance(data, list):
            raw_dicts = []
            for item in data:
                if isinstance(item, BaseModel):
                    raw_dicts.append(item.model_dump())
                elif isinstance(item, dict):
                    raw_dicts.append(item)
                else:
                    raise ValueError(f"Unsupported record item type: {type(item)}")
        else:
            raise ValueError(f"Unsupported input dataset type: {type(data)}")

        # Inspect table column types
        table = orm_class.__table__
        valid_cols = {col.name for col in table.columns}
        datetime_cols = {
            col.name for col in table.columns
            if isinstance(col.type, (DateTime, Date))
        }

        cleaned_records = []
        for r in raw_dicts:
            cleaned_row = {}
            for k, v in r.items():
                if k not in valid_cols:
                    continue
                # Automatically parse strings/timestamps into datetime objects for DateTime columns
                if k in datetime_cols and v is not None:
                    if isinstance(v, str):
                        try:
                            v = date_parser.parse(v)
                        except Exception:
                            pass
                    elif isinstance(v, (int, float)):
                        try:
                            v = datetime.fromtimestamp(v, timezone.utc)
                        except Exception:
                            pass
                cleaned_row[k] = v
            cleaned_records.append(cleaned_row)

        return cleaned_records


    def _get_primary_key_column(self, table: Table) -> str:
        """Extracts the primary key column name for the table."""
        pk_cols = [c.name for c in table.primary_key.columns]
        if not pk_cols:
            raise ValueError(f"Table {table.name} has no primary key defined for upsert.")
        return pk_cols[0]

    def upsert_records(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pd.DataFrame, List[BaseModel], List[Dict[str, Any]]],
        batch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes an idempotent batch upsert against the specified target table.
        Returns metrics with rows inserted, rows updated, and execution duration.
        """
        start_time = time.time()
        batch_id = batch_id or f"batch_{uuid.uuid4().hex[:12]}"

        orm_class = MODEL_TABLE_MAP.get(table_name.lower())
        if not orm_class:
            raise ValueError(
                f"Unknown target table '{table_name}'. Valid options: {list(MODEL_TABLE_MAP.keys())}"
            )

        table: Table = orm_class.__table__
        pk_col_name = self._get_primary_key_column(table)
        records = self._normalize_records(data, orm_class)

        if not records:
            logger.info(f"No records provided for table {table.name}. Skipping load.")
            return {
                "table": table.name,
                "batch_id": batch_id,
                "rows_processed": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "duration_seconds": 0.0,
                "status": "SKIPPED",
            }

        # Deduplicate records within the incoming batch itself (keeping the latest occurrence)
        deduped_dict: Dict[Any, Dict[str, Any]] = {}
        for r in records:
            pk_val = r.get(pk_col_name)
            if pk_val is not None:
                deduped_dict[pk_val] = r
        deduped_records = list(deduped_dict.values())

        total_processed = len(deduped_records)
        total_inserted = 0
        total_updated = 0

        # Process in chunks to prevent huge query payload overhead
        for i in range(0, total_processed, self.chunk_size):
            chunk = deduped_records[i : i + self.chunk_size]
            inserted, updated = self._upsert_chunk(table, pk_col_name, chunk)
            total_inserted += inserted
            total_updated += updated

        duration = time.time() - start_time
        logger.info(
            f"Upsert into [{table.name}] complete. Processed: {total_processed}, "
            f"Inserted: {total_inserted}, Updated: {total_updated} in {duration:.2f}s"
        )

        # Record audit log
        self._record_audit_log(
            batch_id=batch_id,
            target_table=table.name,
            rows_processed=total_processed,
            rows_inserted=total_inserted,
            rows_updated=total_updated,
            duration=duration,
            status="SUCCESS",
        )

        return {
            "table": table.name,
            "batch_id": batch_id,
            "rows_processed": total_processed,
            "rows_inserted": total_inserted,
            "rows_updated": total_updated,
            "duration_seconds": duration,
            "status": "SUCCESS",
        }

    def _upsert_chunk(
        self,
        table: Table,
        pk_col_name: str,
        chunk: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        """Upserts a single chunk using dialect-native ON CONFLICT DO UPDATE or MERGE."""
        if not chunk:
            return 0, 0

        chunk_pks = [row[pk_col_name] for row in chunk if row.get(pk_col_name) is not None]

        with self.engine.begin() as conn:
            # Check which PKs already exist to accurately calculate insert vs update metrics
            pk_column = table.c[pk_col_name]
            existing_query = select(pk_column).where(pk_column.in_(chunk_pks))
            existing_pks = set(conn.execute(existing_query).scalars().all())

            updated_count = len([pk for pk in chunk_pks if pk in existing_pks])
            inserted_count = len(chunk) - updated_count

            # Determine columns to update on conflict (all non-primary key columns)
            update_cols = {
                col.name: col
                for col in table.columns
                if col.name != pk_col_name
            }

            if self.dialect == "postgresql":
                stmt = pg_insert(table).values(chunk)
                set_dict = {
                    col_name: stmt.excluded[col_name]
                    for col_name in update_cols.keys()
                }
                # Automatically refresh updated_at timestamp on conflict
                if "updated_at" in table.c:
                    set_dict["updated_at"] = datetime.now(timezone.utc)

                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_col_name],
                    set_=set_dict,
                )
                conn.execute(upsert_stmt)

            elif self.dialect == "sqlite":
                stmt = sqlite_insert(table).values(chunk)
                set_dict = {
                    col_name: stmt.excluded[col_name]
                    for col_name in update_cols.keys()
                }
                if "updated_at" in table.c:
                    set_dict["updated_at"] = datetime.now(timezone.utc)

                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_col_name],
                    set_=set_dict,
                )
                conn.execute(upsert_stmt)

            else:
                # Generic fallback for Snowflake / other engines:
                # Partition chunk into existing updates and new inserts
                to_insert = [r for r in chunk if r[pk_col_name] not in existing_pks]
                to_update = [r for r in chunk if r[pk_col_name] in existing_pks]

                if to_insert:
                    conn.execute(table.insert(), to_insert)

                for row in to_update:
                    pk_val = row[pk_col_name]
                    update_values = {k: v for k, v in row.items() if k != pk_col_name}
                    if "updated_at" in table.c:
                        update_values["updated_at"] = datetime.now(timezone.utc)
                    conn.execute(
                        table.update().where(pk_column == pk_val).values(**update_values)
                    )

        return inserted_count, updated_count

    def _record_audit_log(
        self,
        batch_id: str,
        target_table: str,
        rows_processed: int,
        rows_inserted: int,
        rows_updated: int,
        duration: float,
        status: str = "SUCCESS",
        error_message: Optional[str] = None,
    ) -> None:
        """Persists sync execution metadata in the etl_load_audit table."""
        try:
            audit_table: Table = ETLLoadAudit.__table__
            with self.engine.begin() as conn:
                conn.execute(
                    audit_table.insert().values(
                        batch_id=batch_id,
                        target_table=target_table,
                        rows_processed=rows_processed,
                        rows_inserted=rows_inserted,
                        rows_updated=rows_updated,
                        duration_seconds=duration,
                        status=status,
                        error_message=error_message,
                        synced_at=datetime.now(timezone.utc),
                    )
                )
        except Exception as exc:
            logger.warning(f"Failed to record load audit log: {exc}")

    def load_all_canonical_datasets(
        self,
        customers_data: Optional[Any] = None,
        transactions_data: Optional[Any] = None,
        subscriptions_data: Optional[Any] = None,
        companies_data: Optional[Any] = None,
        batch_id: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Convenience method to upsert all 4 canonical entities in a unified run."""
        batch_id = batch_id or f"sync_{uuid.uuid4().hex[:10]}"
        results = {}

        if customers_data is not None and len(customers_data) > 0:
            results["dim_customers"] = self.upsert_records(
                "dim_customers", customers_data, batch_id=batch_id
            )

        if transactions_data is not None and len(transactions_data) > 0:
            results["fact_transactions"] = self.upsert_records(
                "fact_transactions", transactions_data, batch_id=batch_id
            )

        if subscriptions_data is not None and len(subscriptions_data) > 0:
            results["fact_subscriptions"] = self.upsert_records(
                "fact_subscriptions", subscriptions_data, batch_id=batch_id
            )

        if companies_data is not None and len(companies_data) > 0:
            results["dim_companies"] = self.upsert_records(
                "dim_companies", companies_data, batch_id=batch_id
            )

        return results
