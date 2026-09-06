"""
Unit Tests for SQLAlchemy 2.0 Warehouse Connection & Session Management.
"""

import pytest
from sqlalchemy import text
from config.settings import WarehouseSettings
from warehouse.connection import WarehouseConnectionManager, get_warehouse_engine
from warehouse.schema import DimCustomer, drop_warehouse_schema, init_warehouse_schema


@pytest.fixture
def memory_db_manager():
    """Provides an in-memory SQLite warehouse connection manager."""
    settings = WarehouseSettings(db_type="sqlite", connection_url=None)
    mgr = WarehouseConnectionManager(settings=settings, custom_url="sqlite:///:memory:")
    init_warehouse_schema(mgr.engine)
    yield mgr
    drop_warehouse_schema(mgr.engine)


def test_connection_url_generation():
    """Validates URL generation across dialects."""
    # SQLite
    sqlite_settings = WarehouseSettings(db_type="sqlite", sqlite_db_path="./test.db")
    assert sqlite_settings.get_connection_url() == "sqlite:///./test.db"

    # PostgreSQL
    pg_settings = WarehouseSettings(
        db_type="postgres",
        host="localhost",
        port=5432,
        user="testuser",
        password="secretpassword",
        database="testdb",
    )
    assert "postgresql+psycopg2://testuser:secretpassword@localhost:5432/testdb" == pg_settings.get_connection_url()

    # Snowflake
    sf_settings = WarehouseSettings(
        db_type="snowflake",
        host="xy12345.snowflakecomputing.com",
        user="sf_user",
        password="sf_password",
        database="analytics_dw",
        schema_name="raw",
    )
    assert "snowflake://sf_user:sf_password@xy12345.snowflakecomputing.com/analytics_dw/raw" == sf_settings.get_connection_url()


def test_warehouse_ping_and_dialect(memory_db_manager):
    """Tests database ping health check and dialect resolution."""
    assert memory_db_manager.ping() is True
    assert memory_db_manager.get_dialect_name() == "sqlite"


def test_session_scope_commit(memory_db_manager):
    """Tests transactional commit within session_scope."""
    with memory_db_manager.session_scope() as session:
        cust = DimCustomer(
            unified_customer_id="cust_test_001",
            source_system="stripe",
            source_id="cus_123",
            email="test@example.com",
            preferred_currency="USD",
        )
        session.add(cust)

    # Verify committed in fresh query
    with memory_db_manager.session_scope() as session:
        queried = session.query(DimCustomer).filter_by(unified_customer_id="cust_test_001").first()
        assert queried is not None
        assert queried.email == "test@example.com"


def test_session_scope_rollback_on_error(memory_db_manager):
    """Tests transactional rollback on unexpected exception."""
    with pytest.raises(RuntimeError):
        with memory_db_manager.session_scope() as session:
            cust = DimCustomer(
                unified_customer_id="cust_rollback_001",
                source_system="stripe",
                source_id="cus_999",
                email="rollback@example.com",
            )
            session.add(cust)
            raise RuntimeError("Forced transaction failure")

    # Verify not committed
    with memory_db_manager.session_scope() as session:
        queried = session.query(DimCustomer).filter_by(unified_customer_id="cust_rollback_001").first()
        assert queried is None
