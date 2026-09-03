"""
Tests for Week 2 Day 7: End-to-End Pipeline Integration and Data Quality Validation.
"""

from pathlib import Path
import pytest

from pipeline.runner import ETLPipelineRunner


class TestEndToEndPipeline:
    """Validates full ETL pipeline lifecycle from API extraction to canonical warehouse loading."""

    def test_full_pipeline_run(self, mock_settings, temp_dir):
        # Initialize pipeline runner in mock mode
        runner = ETLPipelineRunner(settings=mock_settings, use_mock=True)

        # 1. Execute Week 1: API Extraction & Data Lake Raw Staging
        extraction_results = runner.run_week1_extraction(sources=["all"])

        assert len(extraction_results) > 0
        assert "stripe_customers" in extraction_results
        assert "stripe_charges" in extraction_results
        assert "salesforce_accounts" in extraction_results
        assert "salesforce_contacts" in extraction_results

        # Verify staged files exist on disk
        staging_dir = Path(mock_settings.storage.local_storage_path)
        raw_files = list(staging_dir.glob("raw/**/*.json.gz"))
        assert len(raw_files) > 0

        # 2. Execute Week 2: Polars Transformation & Unified Canonical Mapping
        transformation_results = runner.run_week2_transformation()

        assert transformation_results["customers_count"] > 0
        assert transformation_results["transactions_count"] > 0
        assert transformation_results["subscriptions_count"] > 0
        assert transformation_results["companies_count"] > 0

        # Verify Data Quality Reports
        quality_reports = transformation_results["quality_reports"]
        assert len(quality_reports) == 4
        for qr in quality_reports:
            assert qr.passed_threshold is True
            assert qr.quality_score_percent >= 95.0

        # Verify curated Parquet output files exist
        curated_dir = Path(mock_settings.storage.local_storage_path) / "curated"
        assert (curated_dir / "dim_customers.parquet").exists()
        assert (curated_dir / "fact_transactions.parquet").exists()
        assert (curated_dir / "fact_subscriptions.parquet").exists()
        assert (curated_dir / "dim_companies.parquet").exists()

    def test_incremental_extraction_watermark(self, mock_settings):
        runner = ETLPipelineRunner(settings=mock_settings, use_mock=True)

        # First run
        res1 = runner.stripe_extractor.extract_and_stage(entity="customers")
        wm1 = runner.state_store.get_watermark("stripe", "customers")
        assert wm1 is not None

        # State store should retain the high watermark
        assert runner.state_store.get_watermark("stripe", "customers") == wm1
