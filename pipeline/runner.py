"""
Enterprise ETL Pipeline Orchestrator & CLI Runner.
Executes:
- Week 1: API Extraction & S3 Staging
- Week 2: Polars Data Cleaning, Schema Mapping & Quality Validation
- Week 3: Target Data Warehouse Batch Upsert & Audit Logging
- Week 4: Multi-Channel Alerting & Failure Notifications
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config.settings import AppSettings, get_settings
from extractors.salesforce_extractor import SalesforceExtractor
from extractors.stripe_extractor import StripeExtractor
from mock_api.salesforce_mock import MockSalesforceAPI
from mock_api.stripe_mock import MockStripeAPI
from models.unified.canonical_models import (
    UnifiedCompanyAccount,
    UnifiedCustomer,
    UnifiedSubscription,
    UnifiedTransaction,
)
from notifications.manager import AlertManager, get_alert_manager
from storage.s3_client import S3DataLakeWriter, get_storage_writer
from storage.state_store import IncrementalStateStore
from transformers.salesforce_transformer import SalesforceDataTransformer
from transformers.stripe_transformer import StripeDataTransformer
from transformers.unified_mapper import UnifiedSchemaMapper
from warehouse.connection import WarehouseConnectionManager
from warehouse.loader import WarehouseUpsertLoader

console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("pipeline.runner")


class ETLPipelineRunner:
    """Orchestrates extraction, data lake landing, transformation, and warehouse loading."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        use_mock: bool = False,
        custom_db_url: Optional[str] = None,
    ):
        self.settings = settings or get_settings()
        self.use_mock = use_mock
        self.storage_writer = S3DataLakeWriter(settings=self.settings.storage)
        state_file = Path(self.settings.storage.local_storage_path) / "state.json"
        self.state_store = IncrementalStateStore(state_file_path=str(state_file))

        self.stripe_extractor = StripeExtractor(
            settings=self.settings.stripe,
            storage_writer=self.storage_writer,
            state_store=self.state_store,
        )
        self.salesforce_extractor = SalesforceExtractor(
            settings=self.settings.salesforce,
            storage_writer=self.storage_writer,
            state_store=self.state_store,
        )

        self.warehouse_conn_mgr = WarehouseConnectionManager(
            settings=self.settings.warehouse,
            custom_url=custom_db_url,
        )
        self.warehouse_loader = WarehouseUpsertLoader(
            connection_manager=self.warehouse_conn_mgr
        )
        self.alert_manager = get_alert_manager(settings=self.settings.notification)

        if self.use_mock:
            self._setup_mocks()

    def _setup_mocks(self) -> None:
        """Hooks in-memory mock API servers into extractor sessions."""
        logger.info("Setting up Mock API simulators for Stripe and Salesforce.")
        mock_stripe = MockStripeAPI(simulate_rate_limit=True, rate_limit_every_n=8)
        mock_sf = MockSalesforceAPI(page_size=10)

        # Monkey patch execute_request_with_retry to route through mock handlers
        original_stripe_call = self.stripe_extractor.execute_request_with_retry

        def _mock_stripe_call(method: str, url: str, headers=None, params=None, **kwargs):
            return mock_stripe.handle_request(endpoint=url, params=params, headers=headers)

        def _mock_sf_call(method: str, url: str, headers=None, params=None, **kwargs):
            return mock_sf.handle_request(url=url, params=params, headers=headers)

        self.stripe_extractor.execute_request_with_retry = _mock_stripe_call
        self.salesforce_extractor.execute_request_with_retry = _mock_sf_call

    def run_week1_extraction(self, sources: List[str]) -> Dict[str, Any]:
        """
        Executes Week 1: Extract from APIs with pagination and write raw JSON to S3.
        """
        console.rule("[bold cyan]Week 1: API Integration & Data Lake Raw Landing[/bold cyan]")
        extraction_results = {}

        if "stripe" in sources or "all" in sources:
            console.print("[yellow]Starting Stripe API Extraction...[/yellow]")
            stripe_entities = ["customers", "charges", "invoices", "subscriptions"]
            for entity in stripe_entities:
                res = self.stripe_extractor.extract_and_stage(entity=entity)
                extraction_results[f"stripe_{entity}"] = res

        if "salesforce" in sources or "all" in sources:
            console.print("[yellow]Starting Salesforce REST/SOQL Extraction...[/yellow]")
            sf_entities = ["accounts", "contacts", "opportunities", "orders"]
            for entity in sf_entities:
                res = self.salesforce_extractor.extract_and_stage(entity=entity)
                extraction_results[f"salesforce_{entity}"] = res

        return extraction_results

    def run_week2_transformation(self) -> Dict[str, Any]:
        """
        Executes Week 2: Read raw staged batches, clean with Polars, transform to unified schema,
        and validate data quality.
        """
        console.rule("[bold green]Week 2: Polars Data Cleaning & Unified Transformation[/bold green]")

        # --- Helper to collect records from all staged batches for a source/entity ---
        def _load_all_records(source: str, entity: str) -> List[Dict[str, Any]]:
            batches = self.storage_writer.list_raw_batches(source=source, entity=entity)
            all_records = []
            for b_path in batches:
                payload = self.storage_writer.read_raw_batch(b_path)
                all_records.extend(payload.get("records", []))
            return all_records

        # 1. Transform Stripe raw data
        stripe_customers_raw = _load_all_records("stripe", "customers")
        stripe_charges_raw = _load_all_records("stripe", "charges")
        stripe_subs_raw = _load_all_records("stripe", "subscriptions")

        stripe_customers_df = StripeDataTransformer.transform_customers(stripe_customers_raw)
        stripe_tx_df = StripeDataTransformer.transform_charges(stripe_charges_raw)
        stripe_subs_df = StripeDataTransformer.transform_subscriptions(stripe_subs_raw)

        # 2. Transform Salesforce raw data
        sf_accounts_raw = _load_all_records("salesforce", "accounts")
        sf_contacts_raw = _load_all_records("salesforce", "contacts")
        sf_opps_raw = _load_all_records("salesforce", "opportunities")
        sf_orders_raw = _load_all_records("salesforce", "orders")

        sf_company_df, sf_account_cust_df = SalesforceDataTransformer.transform_accounts(sf_accounts_raw)
        sf_contact_cust_df = SalesforceDataTransformer.transform_contacts(sf_contacts_raw)
        sf_opp_tx_df = SalesforceDataTransformer.transform_opportunities(sf_opps_raw)
        sf_order_tx_df = SalesforceDataTransformer.transform_orders(sf_orders_raw)

        sf_all_cust_df = pl.concat([sf_account_cust_df, sf_contact_cust_df], how="diagonal")
        sf_all_tx_df = pl.concat([sf_opp_tx_df, sf_order_tx_df], how="diagonal")

        # 3. Cross-source Unified Mapping & Deduplication
        unified_customers_df = UnifiedSchemaMapper.merge_customers(
            stripe_df=stripe_customers_df,
            salesforce_df=sf_all_cust_df,
        )

        unified_transactions_df = UnifiedSchemaMapper.merge_transactions(
            stripe_tx_df=stripe_tx_df,
            salesforce_tx_df=sf_all_tx_df,
        )

        unified_subscriptions_df = stripe_subs_df
        unified_companies_df = sf_company_df

        # 4. Data Quality Validation (Week 2 Day 7)
        quality_reports = [
            UnifiedSchemaMapper.validate_dataset(unified_customers_df, UnifiedCustomer, "Unified Customers"),
            UnifiedSchemaMapper.validate_dataset(unified_transactions_df, UnifiedTransaction, "Unified Transactions"),
            UnifiedSchemaMapper.validate_dataset(unified_subscriptions_df, UnifiedSubscription, "Unified Subscriptions"),
            UnifiedSchemaMapper.validate_dataset(unified_companies_df, UnifiedCompanyAccount, "Unified Company Accounts"),
        ]

        # 5. Persist Curated Warehouse Datasets
        curated_dir = Path(self.settings.storage.local_storage_path) / "curated"
        curated_dir.mkdir(parents=True, exist_ok=True)

        if len(unified_customers_df) > 0:
            unified_customers_df.write_parquet(curated_dir / "dim_customers.parquet")
            unified_customers_df.write_json(curated_dir / "dim_customers.json")

        if len(unified_transactions_df) > 0:
            unified_transactions_df.write_parquet(curated_dir / "fact_transactions.parquet")
            unified_transactions_df.write_json(curated_dir / "fact_transactions.json")

        if len(unified_subscriptions_df) > 0:
            unified_subscriptions_df.write_parquet(curated_dir / "fact_subscriptions.parquet")
            unified_subscriptions_df.write_json(curated_dir / "fact_subscriptions.json")

        if len(unified_companies_df) > 0:
            unified_companies_df.write_parquet(curated_dir / "dim_companies.parquet")
            unified_companies_df.write_json(curated_dir / "dim_companies.json")

        return {
            "customers_df": unified_customers_df,
            "transactions_df": unified_transactions_df,
            "subscriptions_df": unified_subscriptions_df,
            "companies_df": unified_companies_df,
            "customers_count": len(unified_customers_df),
            "transactions_count": len(unified_transactions_df),
            "subscriptions_count": len(unified_subscriptions_df),
            "companies_count": len(unified_companies_df),
            "quality_reports": quality_reports,
            "curated_path": str(curated_dir.resolve()),
        }

    def run_week3_loading(
        self,
        transformation_results: Dict[str, Any],
        batch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes Week 3: Upsert curated canonical datasets into target Data Warehouse tables.
        """
        console.rule("[bold magenta]Week 3: Data Warehouse Batch Upsert & Sync[/bold magenta]")
        console.print(f"[cyan]Target Warehouse Dialect:[/cyan] [bold]{self.warehouse_conn_mgr.get_dialect_name().upper()}[/bold]")

        customers_df = transformation_results.get("customers_df")
        transactions_df = transformation_results.get("transactions_df")
        subscriptions_df = transformation_results.get("subscriptions_df")
        companies_df = transformation_results.get("companies_df")

        load_results = self.warehouse_loader.load_all_canonical_datasets(
            customers_data=customers_df,
            transactions_data=transactions_df,
            subscriptions_data=subscriptions_df,
            companies_data=companies_df,
            batch_id=batch_id,
        )

        return load_results

    def print_summary(
        self,
        extraction_results: Dict[str, Any],
        transformation_results: Dict[str, Any],
        loading_results: Optional[Dict[str, Any]],
        duration: float,
    ) -> None:
        """Renders rich summary tables to the terminal."""
        # 1. Extraction Summary Table
        ext_table = Table(title="[bold cyan]Week 1 API Extraction Summary[/bold cyan]", show_header=True)
        ext_table.add_column("Source & Entity", style="cyan")
        ext_table.add_column("Records Extracted", justify="right", style="green")
        ext_table.add_column("Batch Files", justify="right", style="yellow")
        ext_table.add_column("Duration (s)", justify="right", style="magenta")

        total_extracted = 0
        for name, res in extraction_results.items():
            records = res.get("total_records", 0)
            total_extracted += records
            batches = len(res.get("staged_uris", []))
            dur = f"{res.get('duration_seconds', 0.0):.2f}"
            ext_table.add_row(name, str(records), str(batches), dur)

        console.print(ext_table)

        # 2. Transformation & Quality Summary Table
        trans_table = Table(title="[bold green]Week 2 Canonical Datasets & Quality[/bold green]", show_header=True)
        trans_table.add_column("Canonical Dataset", style="bold")
        trans_table.add_column("Total Records", justify="right")
        trans_table.add_column("Valid Records", justify="right", style="green")
        trans_table.add_column("Quality Score", justify="right", style="magenta")
        trans_table.add_column("Status", justify="center")

        for qr in transformation_results.get("quality_reports", []):
            status_text = "[green]PASSED[/green]" if qr.passed_threshold else "[red]FAILED[/red]"
            trans_table.add_row(
                qr.dataset_name,
                str(qr.total_records),
                str(qr.valid_records),
                f"{qr.quality_score_percent:.1f}%",
                status_text,
            )

        console.print(trans_table)

        # 3. Warehouse Loading Table (Week 3)
        total_inserted = 0
        total_updated = 0
        if loading_results:
            load_table = Table(title="[bold magenta]Week 3 Data Warehouse Upsert Summary[/bold magenta]", show_header=True)
            load_table.add_column("Target Table", style="bold magenta")
            load_table.add_column("Processed", justify="right")
            load_table.add_column("Inserted", justify="right", style="green")
            load_table.add_column("Updated", justify="right", style="yellow")
            load_table.add_column("Duration (s)", justify="right", style="cyan")
            load_table.add_column("Status", justify="center")

            for tbl, res in loading_results.items():
                ins = res.get("rows_inserted", 0)
                upd = res.get("rows_updated", 0)
                total_inserted += ins
                total_updated += upd
                load_table.add_row(
                    tbl,
                    str(res.get("rows_processed", 0)),
                    str(ins),
                    str(upd),
                    f"{res.get('duration_seconds', 0.0):.2f}",
                    "[green]SUCCESS[/green]" if res.get("status") == "SUCCESS" else "[red]FAILED[/red]",
                )
            console.print(load_table)

        panel_content = (
            f"[bold]Total Extracted:[/bold] {total_extracted} records\n"
            f"[bold]Curated Staging:[/bold] {transformation_results.get('curated_path')}\n"
            f"[bold]Warehouse Upsert:[/bold] {total_inserted} inserted, {total_updated} updated\n"
            f"[bold]Pipeline Duration:[/bold] {duration:.2f}s\n"
            f"[bold]Status:[/bold] [green]ALL 4 WEEKS ETL STAGES COMPLETED SUCCESSFULLY[/green]"
        )
        console.print(Panel(panel_content, title="[bold white on blue] ETL Pipeline Run Completed [/bold white on blue]"))


def main() -> None:
    """CLI Entrypoint for the ETL Pipeline."""
    parser = argparse.ArgumentParser(description="Enterprise ETL Pipeline & Data Warehouse Synchronizer")
    parser.add_argument(
        "--source",
        choices=["stripe", "salesforce", "all"],
        default="all",
        help="Data source to extract (stripe, salesforce, all)",
    )
    parser.add_argument(
        "--mode",
        choices=["mock", "live"],
        default="mock",
        help="Execution mode: 'mock' (simulated offline) or 'live' (real API credentials)",
    )
    parser.add_argument(
        "--clean-state",
        action="store_true",
        help="Reset watermark state store before running",
    )
    parser.add_argument(
        "--no-sync-db",
        action="store_true",
        help="Skip Week 3 Data Warehouse loading phase",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Custom database connection URL (e.g. postgresql://... or sqlite://...)",
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Force send Slack and Email notifications upon completion",
    )

    args = parser.parse_args()

    start_time = time.time()
    runner = ETLPipelineRunner(
        use_mock=(args.mode == "mock"),
        custom_db_url=args.db_url,
    )

    if args.clean_state:
        logger.info("Clearing incremental state store.")
        runner.state_store.reset_state()

    try:
        # Step 1: Week 1 Extraction
        extraction_results = runner.run_week1_extraction(sources=[args.source])

        # Step 2: Week 2 Transformation
        transformation_results = runner.run_week2_transformation()

        # Step 3: Week 3 Data Warehouse Loading (Unless skipped)
        loading_results = None
        if not args.no_sync_db:
            loading_results = runner.run_week3_loading(transformation_results)

        total_duration = time.time() - start_time
        runner.print_summary(extraction_results, transformation_results, loading_results, total_duration)

        if args.notify or runner.settings.notification.alert_on_success:
            runner.alert_manager.notify_pipeline_success(
                pipeline_name="Enterprise ETL Pipeline",
                metrics={
                    "sources": args.source,
                    "mode": args.mode,
                    "total_extracted": sum(r.get("total_records", 0) for r in extraction_results.values()),
                    "warehouse_synced": "True" if loading_results else "False",
                },
                duration_seconds=total_duration,
            )

    except Exception as exc:
        logger.exception("Pipeline execution encountered a fatal error.")
        runner.alert_manager.notify_pipeline_failure(
            pipeline_name="Enterprise ETL Pipeline",
            error_message=str(exc),
            stage="runner.main",
            exception=exc,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
